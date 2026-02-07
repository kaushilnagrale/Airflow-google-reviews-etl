"""
Unit tests for Flask API endpoints.
"""

import pytest
from unittest.mock import patch, MagicMock


@pytest.fixture
def client():
    """Create test Flask client."""
    # Patch database connections before importing app
    with patch("frontend.app.MongoClient") as mock_mongo, \
         patch("frontend.app.psycopg2.connect") as mock_pg:
        
        mock_db = MagicMock()
        mock_mongo.return_value.__getitem__.return_value = mock_db

        import sys
        sys.path.insert(0, ".")
        from frontend.app import app
        app.config["TESTING"] = True
        with app.test_client() as client:
            yield client, mock_db


class TestHealthEndpoint:
    def test_health_returns_200(self, client):
        client, mock_db = client
        # Mock collection counts
        for col in ["raw_reviews", "restaurants", "processed_reviews", "recommendations"]:
            mock_db[col].count_documents.return_value = 10

        response = client.get("/api/v1/health")
        assert response.status_code == 200
        data = response.get_json()
        assert data["status"] == "healthy"
        assert "collections" in data


class TestRestaurantEndpoints:
    def test_list_restaurants(self, client):
        client, mock_db = client
        mock_db["restaurants"].find.return_value.sort.return_value.limit.return_value = [
            {"place_id": "test1", "name": "Restaurant A", "rating": 4.5},
        ]

        response = client.get("/api/v1/restaurants")
        assert response.status_code == 200
        data = response.get_json()
        assert "restaurants" in data

    def test_get_restaurant_not_found(self, client):
        client, mock_db = client
        mock_db["restaurants"].find_one.return_value = None

        response = client.get("/api/v1/restaurants/nonexistent")
        assert response.status_code == 404


class TestRecommendationEndpoints:
    def test_get_recommendations(self, client):
        client, mock_db = client
        mock_db["recommendations"].find_one.return_value = {
            "user_id": "global",
            "recommendations": [
                {"rank": 1, "restaurant_name": "Best Place", "avg_predicted_score": 4.8}
            ],
            "generated_at": "2024-01-01T00:00:00Z",
        }

        response = client.get("/api/v1/recommendations")
        assert response.status_code == 200
        data = response.get_json()
        assert data["count"] >= 1

    def test_submit_preferences(self, client):
        client, mock_db = client
        mock_db["user_preferences"].update_one.return_value = MagicMock()

        response = client.post(
            "/api/v1/preferences",
            json={"user_id": "test_user", "cuisine_types": ["italian"], "min_rating": 4.0},
        )
        assert response.status_code == 200
        data = response.get_json()
        assert data["status"] == "saved"
