"""
Unit tests for ETL modules.
"""

import pytest
from unittest.mock import patch, MagicMock


class TestGoogleAPIExtractor:
    """Tests for the Google Places API extractor."""

    @patch("scripts.etl.google_api_extractor.config")
    def test_extract_reviews_from_place_details(self, mock_config):
        """Test review extraction and normalization from place details."""
        mock_config.google_api_key = "test_key"
        mock_config.google_api = {
            "rate_limit": {"requests_per_second": 10, "retry_max": 3, "retry_backoff": 2},
            "search_radius": 5000,
            "max_results_per_query": 60,
            "review_fields": ["place_id", "name", "rating", "reviews"],
            "search_locations": [],
        }

        from scripts.etl.google_api_extractor import GoogleAPIExtractor
        extractor = GoogleAPIExtractor()

        place_details = {
            "place_id": "ChIJ_test123",
            "name": "Test Restaurant",
            "reviews": [
                {
                    "author_name": "John Doe",
                    "rating": 5,
                    "text": "Amazing food and great service!",
                    "language": "en",
                    "time": 1700000000,
                    "relative_time_description": "2 weeks ago",
                    "profile_photo_url": "https://example.com/photo.jpg",
                },
                {
                    "author_name": "Jane Smith",
                    "rating": 3,
                    "text": "Decent food but slow service.",
                    "language": "en",
                    "time": 1699000000,
                },
            ],
        }

        reviews = extractor.extract_reviews(place_details)

        assert len(reviews) == 2
        assert reviews[0]["place_id"] == "ChIJ_test123"
        assert reviews[0]["restaurant_name"] == "Test Restaurant"
        assert reviews[0]["rating"] == 5
        assert reviews[0]["text"] == "Amazing food and great service!"
        assert reviews[0]["review_id"]  # Should have a hash ID
        assert reviews[0]["ingested_at"]  # Should have timestamp

    @patch("scripts.etl.google_api_extractor.config")
    def test_extract_restaurant_metadata(self, mock_config):
        """Test restaurant metadata extraction."""
        mock_config.google_api_key = "test_key"
        mock_config.google_api = {
            "rate_limit": {"requests_per_second": 10, "retry_max": 3, "retry_backoff": 2},
            "search_radius": 5000,
            "max_results_per_query": 60,
            "review_fields": [],
            "search_locations": [],
        }

        from scripts.etl.google_api_extractor import GoogleAPIExtractor
        extractor = GoogleAPIExtractor()

        place_details = {
            "place_id": "ChIJ_test456",
            "name": "Pasta Palace",
            "formatted_address": "123 Main St, Phoenix, AZ",
            "geometry": {"location": {"lat": 33.4484, "lng": -112.074}},
            "rating": 4.5,
            "user_ratings_total": 250,
            "price_level": 2,
            "types": ["restaurant", "italian_restaurant", "food"],
        }

        metadata = extractor.extract_restaurant_metadata(place_details)

        assert metadata["place_id"] == "ChIJ_test456"
        assert metadata["name"] == "Pasta Palace"
        assert metadata["latitude"] == 33.4484
        assert metadata["rating"] == 4.5
        assert metadata["total_ratings"] == 250
        assert "italian_restaurant" in metadata["types"]


class TestTextPreprocessor:
    """Tests for the text preprocessor."""

    def test_clean_basic_text(self):
        from scripts.nlp.text_preprocessor import TextPreprocessor
        preprocessor = TextPreprocessor()

        text = "  Great food!  Really  enjoyed it.  "
        result = preprocessor.clean(text)
        assert result == "Great food! Really enjoyed it."

    def test_clean_html_tags(self):
        from scripts.nlp.text_preprocessor import TextPreprocessor
        preprocessor = TextPreprocessor()

        text = "<p>Great <b>food</b>!</p>"
        result = preprocessor.clean(text)
        assert "<" not in result
        assert "Great" in result

    def test_clean_urls(self):
        from scripts.nlp.text_preprocessor import TextPreprocessor
        preprocessor = TextPreprocessor()

        text = "Check out https://example.com for more info"
        result = preprocessor.clean(text)
        assert "https" not in result

    def test_clean_empty_text(self):
        from scripts.nlp.text_preprocessor import TextPreprocessor
        preprocessor = TextPreprocessor()

        assert preprocessor.clean("") == ""
        assert preprocessor.clean(None) == ""

    def test_extract_key_phrases(self):
        from scripts.nlp.text_preprocessor import TextPreprocessor
        preprocessor = TextPreprocessor()

        text = "The pizza was amazing and the staff was very friendly"
        phrases = preprocessor.extract_key_phrases(text)
        assert "pizza" in phrases
        assert "staff" in phrases


class TestMongoLoader:
    """Tests for MongoDB loader with mocked connections."""

    @patch("scripts.etl.mongo_loader.MongoClient")
    @patch("scripts.etl.mongo_loader.config")
    def test_upsert_reviews(self, mock_config, mock_client):
        mock_config.mongo_uri = "mongodb://localhost:27017"
        mock_config.mongo_db = "test_db"
        mock_config.mongo_collections = {
            "raw_reviews": "raw_reviews",
            "restaurants": "restaurants",
            "processed_reviews": "processed_reviews",
            "recommendations": "recommendations",
        }

        from scripts.etl.mongo_loader import MongoLoader
        loader = MongoLoader()

        reviews = [
            {"review_id": "abc123", "text": "Great food", "rating": 5},
            {"review_id": "def456", "text": "Okay food", "rating": 3},
        ]

        mock_result = MagicMock()
        mock_result.upserted_count = 2
        mock_result.modified_count = 0
        loader.db["raw_reviews"].bulk_write.return_value = mock_result

        result = loader.upsert_reviews(reviews)
        assert result["inserted"] == 2
        assert result["modified"] == 0


class TestWarehouseLoader:
    """Tests for warehouse loader with mocked connections."""

    @patch("scripts.etl.warehouse_loader.psycopg2.connect")
    @patch("scripts.etl.warehouse_loader.config")
    def test_load_dim_restaurants(self, mock_config, mock_connect):
        mock_config.postgres_host = "localhost"
        mock_config.postgres_port = 5432
        mock_config.postgres_db = "test_warehouse"
        mock_config.postgres_user = "test_user"
        mock_config.postgres_password = "test_pass"

        from scripts.etl.warehouse_loader import WarehouseLoader
        loader = WarehouseLoader()

        restaurants = [
            {
                "place_id": "test_1",
                "name": "Test Restaurant",
                "address": "123 Test St",
                "latitude": 33.45,
                "longitude": -112.07,
                "rating": 4.5,
                "total_ratings": 100,
                "price_level": 2,
                "types": ["restaurant"],
            }
        ]

        result = loader.load_dim_restaurants(restaurants)
        assert result == 1
