"""
Recommendation DAG
Generates restaurant recommendations based on NLP analysis:
1. Load processed reviews and restaurant data
2. Run sentiment aggregation per restaurant
3. Generate and store ranked recommendations
"""

from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


default_args = {
    "owner": "data-science",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}


def load_restaurant_data(**context):
    """Load all restaurants and their processed reviews."""
    from scripts.etl.mongo_loader import MongoLoader

    mongo = MongoLoader()
    try:
        restaurants = mongo.get_all_restaurants()
        restaurant_reviews = {}

        for restaurant in restaurants:
            place_id = restaurant["place_id"]
            reviews = mongo.get_restaurant_reviews(place_id)
            if reviews:
                restaurant_reviews[place_id] = {
                    "restaurant": restaurant,
                    "reviews": reviews,
                }

        context["ti"].xcom_push(key="restaurant_reviews", value=restaurant_reviews)
        return f"Loaded {len(restaurant_reviews)} restaurants with reviews"
    finally:
        mongo.close()


def analyze_and_rank(**context):
    """Run sentiment analysis aggregation and rank restaurants."""
    from scripts.nlp.sentiment_analyzer import SentimentAnalyzer

    restaurant_reviews = context["ti"].xcom_pull(
        key="restaurant_reviews", task_ids="load_restaurant_data"
    )

    if not restaurant_reviews:
        return "No data to analyze"

    analyzer = SentimentAnalyzer()
    analyses = []

    for place_id, data in restaurant_reviews.items():
        analysis = analyzer.analyze_restaurant(data["reviews"])
        analysis["place_id"] = place_id
        analysis["restaurant_name"] = data["restaurant"].get("name", "Unknown")
        analysis["types"] = data["restaurant"].get("types", [])
        analyses.append(analysis)

    # Generate global recommendations (no user preferences)
    recommendations = analyzer.generate_recommendations(analyses)

    context["ti"].xcom_push(key="analyses", value=analyses)
    context["ti"].xcom_push(key="recommendations", value=recommendations)

    return f"Analyzed {len(analyses)} restaurants, generated {len(recommendations)} recommendations"


def store_recommendations(**context):
    """Save recommendations to MongoDB for API access."""
    from scripts.etl.mongo_loader import MongoLoader

    recommendations = context["ti"].xcom_pull(
        key="recommendations", task_ids="analyze_and_rank"
    )

    if not recommendations:
        return "No recommendations to save"

    mongo = MongoLoader()
    try:
        # Save as global recommendations (user_id = "global")
        mongo.save_recommendations("global", recommendations)
        return f"Saved {len(recommendations)} global recommendations"
    finally:
        mongo.close()


with DAG(
    dag_id="recommendation_pipeline",
    default_args=default_args,
    description="Recommendation: aggregate sentiments → rank → store",
    schedule_interval="0 6 * * *",  # Daily at 6 AM (after NLP processing)
    start_date=days_ago(1),
    catchup=False,
    tags=["recommendation", "analytics"],
    max_active_runs=1,
) as dag:

    t_load = PythonOperator(
        task_id="load_restaurant_data",
        python_callable=load_restaurant_data,
    )

    t_analyze = PythonOperator(
        task_id="analyze_and_rank",
        python_callable=analyze_and_rank,
    )

    t_store = PythonOperator(
        task_id="store_recommendations",
        python_callable=store_recommendations,
    )

    t_load >> t_analyze >> t_store
