"""
Warehouse Loader
Loads transformed data into PostgreSQL star-schema data warehouse.
"""

from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import execute_batch
from scripts.utils.logger import get_logger
from scripts.utils.config_loader import config

logger = get_logger("warehouse_loader")


class WarehouseLoader:
    """Loads data into the PostgreSQL data warehouse."""

    def __init__(self):
        self.conn_params = {
            "host": config.postgres_host,
            "port": config.postgres_port,
            "dbname": config.postgres_db,
            "user": config.postgres_user,
            "password": config.postgres_password,
        }
        self._conn = None

    @property
    def conn(self):
        """Lazy connection with auto-reconnect."""
        if self._conn is None or self._conn.closed:
            self._conn = psycopg2.connect(**self.conn_params)
            self._conn.autocommit = False
            logger.info("Connected to PostgreSQL warehouse")
        return self._conn

    def load_dim_restaurants(self, restaurants: list[dict]) -> int:
        """Load restaurant dimension table with upsert."""
        if not restaurants:
            return 0

        query = """
            INSERT INTO dim_restaurants (place_id, name, address, latitude, longitude,
                                         rating, total_ratings, price_level, types, updated_at)
            VALUES (%(place_id)s, %(name)s, %(address)s, %(latitude)s, %(longitude)s,
                    %(rating)s, %(total_ratings)s, %(price_level)s, %(types)s, %(updated_at)s)
            ON CONFLICT (place_id)
            DO UPDATE SET
                name = EXCLUDED.name,
                address = EXCLUDED.address,
                rating = EXCLUDED.rating,
                total_ratings = EXCLUDED.total_ratings,
                price_level = EXCLUDED.price_level,
                types = EXCLUDED.types,
                updated_at = EXCLUDED.updated_at;
        """

        now = datetime.now(timezone.utc).isoformat()
        for r in restaurants:
            r["updated_at"] = now
            if isinstance(r.get("types"), list):
                r["types"] = ",".join(r["types"])

        try:
            with self.conn.cursor() as cur:
                execute_batch(cur, query, restaurants, page_size=100)
            self.conn.commit()
            logger.info(f"Loaded {len(restaurants)} restaurants into dim_restaurants")
            return len(restaurants)
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to load dim_restaurants: {e}")
            raise

    def load_fact_reviews(self, reviews: list[dict]) -> int:
        """Load review fact table with upsert."""
        if not reviews:
            return 0

        query = """
            INSERT INTO fact_reviews (review_id, place_id, author_name, rating,
                                      review_text, review_length, word_count,
                                      review_date, review_age_days, loaded_at)
            VALUES (%(review_id)s, %(place_id)s, %(author_name)s, %(rating)s,
                    %(review_text)s, %(review_length)s, %(word_count)s,
                    %(review_date)s, %(review_age_days)s, %(loaded_at)s)
            ON CONFLICT (review_id)
            DO UPDATE SET
                rating = EXCLUDED.rating,
                review_text = EXCLUDED.review_text,
                review_age_days = EXCLUDED.review_age_days,
                loaded_at = EXCLUDED.loaded_at;
        """

        now = datetime.now(timezone.utc).isoformat()
        for r in reviews:
            r["loaded_at"] = now
            r.setdefault("review_text", r.get("text_cleaned", ""))

        try:
            with self.conn.cursor() as cur:
                execute_batch(cur, query, reviews, page_size=100)
            self.conn.commit()
            logger.info(f"Loaded {len(reviews)} reviews into fact_reviews")
            return len(reviews)
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to load fact_reviews: {e}")
            raise

    def load_fact_predictions(self, predictions: list[dict]) -> int:
        """Load NLP prediction results into fact table."""
        if not predictions:
            return 0

        query = """
            INSERT INTO fact_predictions (review_id, place_id, predicted_score,
                                          sentiment_label, confidence, model_version,
                                          predicted_at)
            VALUES (%(review_id)s, %(place_id)s, %(predicted_score)s,
                    %(sentiment_label)s, %(confidence)s, %(model_version)s,
                    %(predicted_at)s)
            ON CONFLICT (review_id)
            DO UPDATE SET
                predicted_score = EXCLUDED.predicted_score,
                sentiment_label = EXCLUDED.sentiment_label,
                confidence = EXCLUDED.confidence,
                model_version = EXCLUDED.model_version,
                predicted_at = EXCLUDED.predicted_at;
        """

        try:
            with self.conn.cursor() as cur:
                execute_batch(cur, query, predictions, page_size=100)
            self.conn.commit()
            logger.info(f"Loaded {len(predictions)} predictions into fact_predictions")
            return len(predictions)
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to load fact_predictions: {e}")
            raise

    def load_dim_time(self, time_records: list[dict]) -> int:
        """Load time dimension table."""
        if not time_records:
            return 0

        query = """
            INSERT INTO dim_time (full_date, year, month, day, quarter,
                                  day_of_week, week_of_year)
            VALUES (%(full_date)s, %(year)s, %(month)s, %(day)s, %(quarter)s,
                    %(day_of_week)s, %(week_of_year)s)
            ON CONFLICT (full_date) DO NOTHING;
        """

        try:
            with self.conn.cursor() as cur:
                execute_batch(cur, query, time_records, page_size=100)
            self.conn.commit()
            logger.info(f"Loaded {len(time_records)} records into dim_time")
            return len(time_records)
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to load dim_time: {e}")
            raise

    def get_sentiment_analytics(self) -> list[dict]:
        """Query sentiment distribution across restaurants."""
        query = """
            SELECT
                r.name AS restaurant_name,
                r.place_id,
                COUNT(p.review_id) AS prediction_count,
                ROUND(AVG(p.predicted_score)::numeric, 2) AS avg_predicted_score,
                ROUND(AVG(f.rating)::numeric, 2) AS avg_actual_rating,
                SUM(CASE WHEN p.sentiment_label = 'positive' THEN 1 ELSE 0 END) AS positive_count,
                SUM(CASE WHEN p.sentiment_label = 'negative' THEN 1 ELSE 0 END) AS negative_count,
                SUM(CASE WHEN p.sentiment_label = 'neutral' THEN 1 ELSE 0 END) AS neutral_count
            FROM fact_predictions p
            JOIN fact_reviews f ON p.review_id = f.review_id
            JOIN dim_restaurants r ON p.place_id = r.place_id
            GROUP BY r.name, r.place_id
            ORDER BY avg_predicted_score DESC;
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute(query)
                columns = [desc[0] for desc in cur.description]
                return [dict(zip(columns, row)) for row in cur.fetchall()]
        except Exception as e:
            logger.error(f"Failed to get sentiment analytics: {e}")
            return []

    def close(self):
        """Close database connection."""
        if self._conn and not self._conn.closed:
            self._conn.close()
            logger.info("PostgreSQL connection closed")
