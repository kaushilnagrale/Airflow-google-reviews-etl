"""
MongoDB Loader
Handles CRUD operations for raw and processed review data in MongoDB.
"""

from datetime import datetime, timezone
from typing import Optional

from pymongo import MongoClient, UpdateOne, ASCENDING, DESCENDING
from pymongo.errors import BulkWriteError

from scripts.utils.logger import get_logger
from scripts.utils.config_loader import config

logger = get_logger("mongo_loader")


class MongoLoader:
    """MongoDB data loader with upsert and batch operations."""

    def __init__(self):
        self.client = MongoClient(config.mongo_uri)
        self.db = self.client[config.mongo_db]
        self.collections = config.mongo_collections
        self._ensure_indexes()

    def _ensure_indexes(self):
        """Create necessary indexes for query performance."""
        # Raw reviews collection
        raw_col = self.db[self.collections["raw_reviews"]]
        raw_col.create_index("review_id", unique=True)
        raw_col.create_index("place_id")
        raw_col.create_index("ingested_at")

        # Restaurants collection
        rest_col = self.db[self.collections["restaurants"]]
        rest_col.create_index("place_id", unique=True)
        rest_col.create_index([("rating", DESCENDING)])
        rest_col.create_index([("latitude", ASCENDING), ("longitude", ASCENDING)])

        # Processed reviews collection
        proc_col = self.db[self.collections["processed_reviews"]]
        proc_col.create_index("review_id", unique=True)
        proc_col.create_index("place_id")
        proc_col.create_index([("predicted_score", DESCENDING)])

        # Recommendations collection
        rec_col = self.db[self.collections["recommendations"]]
        rec_col.create_index("user_id")
        rec_col.create_index("generated_at")

        logger.info("MongoDB indexes ensured")

    def upsert_reviews(self, reviews: list[dict]) -> dict:
        """
        Upsert reviews into raw_reviews collection.
        Uses review_id as the unique key.
        Returns counts of inserted and modified documents.
        """
        if not reviews:
            return {"inserted": 0, "modified": 0}

        collection = self.db[self.collections["raw_reviews"]]
        operations = [
            UpdateOne(
                {"review_id": review["review_id"]},
                {"$set": review},
                upsert=True,
            )
            for review in reviews
        ]

        try:
            result = collection.bulk_write(operations, ordered=False)
            stats = {
                "inserted": result.upserted_count,
                "modified": result.modified_count,
            }
            logger.info(f"Upserted reviews: {stats}")
            return stats
        except BulkWriteError as e:
            logger.error(f"Bulk write error: {e.details}")
            raise

    def upsert_restaurant(self, restaurant: dict) -> bool:
        """Upsert a restaurant document."""
        collection = self.db[self.collections["restaurants"]]
        result = collection.update_one(
            {"place_id": restaurant["place_id"]},
            {"$set": restaurant},
            upsert=True,
        )
        return result.upserted_id is not None or result.modified_count > 0

    def save_processed_reviews(self, reviews: list[dict]) -> int:
        """Save NLP-processed reviews with predictions."""
        if not reviews:
            return 0

        collection = self.db[self.collections["processed_reviews"]]
        operations = [
            UpdateOne(
                {"review_id": review["review_id"]},
                {"$set": review},
                upsert=True,
            )
            for review in reviews
        ]

        result = collection.bulk_write(operations, ordered=False)
        count = result.upserted_count + result.modified_count
        logger.info(f"Saved {count} processed reviews")
        return count

    def save_recommendations(self, user_id: str, recommendations: list[dict]):
        """Save generated recommendations for a user."""
        collection = self.db[self.collections["recommendations"]]
        doc = {
            "user_id": user_id,
            "recommendations": recommendations,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }
        collection.update_one(
            {"user_id": user_id},
            {"$set": doc},
            upsert=True,
        )
        logger.info(f"Saved {len(recommendations)} recommendations for user {user_id}")

    def get_unprocessed_reviews(self, batch_size: int = 1000) -> list[dict]:
        """Get reviews that haven't been processed by the NLP model yet."""
        raw_col = self.db[self.collections["raw_reviews"]]
        processed_col = self.db[self.collections["processed_reviews"]]

        # Get IDs of already-processed reviews
        processed_ids = set(
            doc["review_id"]
            for doc in processed_col.find({}, {"review_id": 1})
        )

        # Fetch unprocessed reviews
        cursor = raw_col.find(
            {"review_id": {"$nin": list(processed_ids)}},
            {"_id": 0},
        ).limit(batch_size)

        reviews = list(cursor)
        logger.info(f"Found {len(reviews)} unprocessed reviews")
        return reviews

    def get_restaurant_reviews(self, place_id: str) -> list[dict]:
        """Get all processed reviews for a restaurant."""
        collection = self.db[self.collections["processed_reviews"]]
        return list(collection.find({"place_id": place_id}, {"_id": 0}))

    def get_all_restaurants(self) -> list[dict]:
        """Get all restaurant metadata."""
        collection = self.db[self.collections["restaurants"]]
        return list(collection.find({}, {"_id": 0}))

    def get_recommendations(self, user_id: str) -> Optional[dict]:
        """Get stored recommendations for a user."""
        collection = self.db[self.collections["recommendations"]]
        return collection.find_one({"user_id": user_id}, {"_id": 0})

    def get_pipeline_stats(self) -> dict:
        """Get collection counts for monitoring."""
        return {
            col_name: self.db[col_key].count_documents({})
            for col_name, col_key in self.collections.items()
        }

    def close(self):
        """Close MongoDB connection."""
        self.client.close()
        logger.info("MongoDB connection closed")
