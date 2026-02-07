"""
Restaurant Recommendation API & Frontend
Flask application serving recommendations and analytics.
"""

import os
from datetime import datetime, timezone

from flask import Flask, render_template, jsonify, request
from flask_cors import CORS
from pymongo import MongoClient
import psycopg2
from psycopg2.extras import RealDictCursor

app = Flask(__name__)
CORS(app)

# ─── Database Connections ────────────────────────────────────
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "restaurant_reviews")

POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname": os.getenv("POSTGRES_DB", "review_warehouse"),
    "user": os.getenv("POSTGRES_USER", "pipeline_user"),
    "password": os.getenv("POSTGRES_PASSWORD", "secure_password"),
}


def get_mongo():
    """Get MongoDB database client."""
    client = MongoClient(MONGO_URI)
    return client[MONGO_DB]


def get_postgres():
    """Get PostgreSQL connection."""
    return psycopg2.connect(**POSTGRES_CONFIG, cursor_factory=RealDictCursor)


# ─── Frontend Routes ────────────────────────────────────────
@app.route("/")
def index():
    """Render the recommendation dashboard."""
    return render_template("index.html")


# ─── API Routes ─────────────────────────────────────────────
@app.route("/api/v1/health")
def health_check():
    """Pipeline health check."""
    db = get_mongo()
    stats = {
        col: db[col].count_documents({})
        for col in ["raw_reviews", "restaurants", "processed_reviews", "recommendations"]
    }
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "collections": stats,
    })


@app.route("/api/v1/restaurants")
def list_restaurants():
    """List all restaurants with their metadata."""
    db = get_mongo()
    restaurants = list(
        db["restaurants"].find({}, {"_id": 0}).sort("rating", -1).limit(100)
    )
    return jsonify({"count": len(restaurants), "restaurants": restaurants})


@app.route("/api/v1/restaurants/<place_id>")
def get_restaurant(place_id):
    """Get restaurant details by place_id."""
    db = get_mongo()
    restaurant = db["restaurants"].find_one({"place_id": place_id}, {"_id": 0})
    if not restaurant:
        return jsonify({"error": "Restaurant not found"}), 404
    return jsonify(restaurant)


@app.route("/api/v1/restaurants/<place_id>/reviews")
def get_restaurant_reviews(place_id):
    """Get reviews for a specific restaurant."""
    db = get_mongo()
    reviews = list(
        db["processed_reviews"]
        .find({"place_id": place_id}, {"_id": 0})
        .sort("predicted_score", -1)
        .limit(50)
    )
    return jsonify({"count": len(reviews), "reviews": reviews})


@app.route("/api/v1/recommendations")
def get_recommendations():
    """Get restaurant recommendations based on optional user preferences."""
    db = get_mongo()

    # Parse optional query parameters
    min_rating = request.args.get("min_rating", type=float)
    cuisine = request.args.get("cuisine")
    limit = request.args.get("limit", default=20, type=int)

    # Fetch global recommendations
    rec_doc = db["recommendations"].find_one({"user_id": "global"}, {"_id": 0})

    if not rec_doc:
        return jsonify({"count": 0, "recommendations": []})

    recommendations = rec_doc.get("recommendations", [])

    # Apply filters
    if min_rating:
        recommendations = [
            r for r in recommendations
            if r.get("avg_predicted_score", 0) >= min_rating
        ]

    return jsonify({
        "count": len(recommendations[:limit]),
        "recommendations": recommendations[:limit],
        "generated_at": rec_doc.get("generated_at"),
    })


@app.route("/api/v1/preferences", methods=["POST"])
def submit_preferences():
    """Submit user preferences for personalized recommendations."""
    data = request.get_json()
    if not data:
        return jsonify({"error": "No preferences provided"}), 400

    user_id = data.get("user_id", "anonymous")
    preferences = {
        "cuisine_types": data.get("cuisine_types", []),
        "min_rating": data.get("min_rating", 3.0),
        "price_range": data.get("price_range"),
    }

    db = get_mongo()
    db["user_preferences"].update_one(
        {"user_id": user_id},
        {"$set": {"preferences": preferences, "updated_at": datetime.now(timezone.utc).isoformat()}},
        upsert=True,
    )

    return jsonify({"status": "saved", "user_id": user_id, "preferences": preferences})


@app.route("/api/v1/analytics/sentiment")
def sentiment_analytics():
    """Get sentiment distribution analytics from the warehouse."""
    try:
        conn = get_postgres()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    r.name,
                    r.place_id,
                    COUNT(p.review_id) AS prediction_count,
                    ROUND(AVG(p.predicted_score)::numeric, 2) AS avg_nlp_score,
                    ROUND(AVG(f.rating)::numeric, 2) AS avg_actual_rating,
                    SUM(CASE WHEN p.sentiment_label IN ('positive', 'very_positive')
                             THEN 1 ELSE 0 END) AS positive_count,
                    SUM(CASE WHEN p.sentiment_label IN ('negative', 'very_negative')
                             THEN 1 ELSE 0 END) AS negative_count
                FROM fact_predictions p
                JOIN fact_reviews f ON p.review_id = f.review_id
                JOIN dim_restaurants r ON p.place_id = r.place_id
                GROUP BY r.name, r.place_id
                HAVING COUNT(p.review_id) >= 3
                ORDER BY avg_nlp_score DESC
                LIMIT 50
            """)
            results = [dict(row) for row in cur.fetchall()]
        conn.close()
        return jsonify({"count": len(results), "analytics": results})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
