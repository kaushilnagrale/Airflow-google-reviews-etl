"""
Spark Transformer
PySpark-based data transformation, cleaning, deduplication,
and feature engineering for restaurant reviews.
"""

import re
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType,
    IntegerType, TimestampType, ArrayType,
)
from pyspark.sql.window import Window

from scripts.utils.logger import get_logger
from scripts.utils.config_loader import config

logger = get_logger("spark_transformer")


# Schema for raw review data
REVIEW_SCHEMA = StructType([
    StructField("review_id", StringType(), False),
    StructField("place_id", StringType(), False),
    StructField("restaurant_name", StringType(), True),
    StructField("author_name", StringType(), True),
    StructField("rating", FloatType(), True),
    StructField("text", StringType(), True),
    StructField("language", StringType(), True),
    StructField("time_epoch", IntegerType(), True),
    StructField("review_time", StringType(), True),
    StructField("relative_time", StringType(), True),
    StructField("profile_photo_url", StringType(), True),
    StructField("ingested_at", StringType(), True),
])

RESTAURANT_SCHEMA = StructType([
    StructField("place_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("address", StringType(), True),
    StructField("latitude", FloatType(), True),
    StructField("longitude", FloatType(), True),
    StructField("rating", FloatType(), True),
    StructField("total_ratings", IntegerType(), True),
    StructField("price_level", IntegerType(), True),
    StructField("types", ArrayType(StringType()), True),
    StructField("extracted_at", StringType(), True),
])


class SparkTransformer:
    """PySpark-based data transformation engine."""

    def __init__(self, spark: SparkSession = None):
        if spark:
            self.spark = spark
        else:
            spark_cfg = config.spark_config
            builder = (
                SparkSession.builder
                .appName(spark_cfg["app_name"])
                .master(spark_cfg.get("master", "local[*]"))
            )
            for key, value in spark_cfg.get("config", {}).items():
                builder = builder.config(key, value)
            self.spark = builder.getOrCreate()

        logger.info("SparkTransformer initialized")

    def load_reviews_from_json(self, data: list[dict]) -> DataFrame:
        """Load raw review data into a Spark DataFrame."""
        rdd = self.spark.sparkContext.parallelize(data)
        df = self.spark.read.json(rdd, schema=REVIEW_SCHEMA)
        logger.info(f"Loaded {df.count()} reviews into Spark DataFrame")
        return df

    def load_restaurants_from_json(self, data: list[dict]) -> DataFrame:
        """Load restaurant metadata into a Spark DataFrame."""
        rdd = self.spark.sparkContext.parallelize(data)
        return self.spark.read.json(rdd, schema=RESTAURANT_SCHEMA)

    def clean_reviews(self, df: DataFrame) -> DataFrame:
        """
        Clean and normalize review data:
        - Remove duplicates
        - Clean text (HTML tags, extra whitespace)
        - Filter empty reviews
        - Normalize ratings
        """
        initial_count = df.count()

        # Deduplicate by review_id
        df = df.dropDuplicates(["review_id"])

        # Remove HTML tags and normalize whitespace in review text
        df = df.withColumn(
            "text_cleaned",
            F.trim(F.regexp_replace(F.col("text"), r"<[^>]+>", ""))
        )
        df = df.withColumn(
            "text_cleaned",
            F.regexp_replace(F.col("text_cleaned"), r"\s+", " ")
        )

        # Filter out empty or very short reviews
        df = df.filter(
            (F.col("text_cleaned").isNotNull()) &
            (F.length(F.col("text_cleaned")) > 5)
        )

        # Ensure rating is within valid range [1, 5]
        df = df.withColumn(
            "rating",
            F.when(F.col("rating") < 1, 1)
            .when(F.col("rating") > 5, 5)
            .otherwise(F.col("rating"))
        )

        # Parse review timestamp
        df = df.withColumn(
            "review_date",
            F.from_unixtime(F.col("time_epoch")).cast(TimestampType())
        )

        final_count = df.count()
        logger.info(
            f"Cleaned reviews: {initial_count} → {final_count} "
            f"(removed {initial_count - final_count})"
        )
        return df

    def engineer_features(self, df: DataFrame) -> DataFrame:
        """
        Add derived features for downstream analytics and ML:
        - Review length
        - Word count
        - Has text flag
        - Day of week
        - Review age in days
        """
        now_ts = F.lit(datetime.now(timezone.utc).timestamp()).cast("timestamp")

        df = (
            df
            .withColumn("review_length", F.length(F.col("text_cleaned")))
            .withColumn(
                "word_count",
                F.size(F.split(F.col("text_cleaned"), r"\s+"))
            )
            .withColumn(
                "has_text",
                F.when(F.col("review_length") > 0, True).otherwise(False)
            )
            .withColumn(
                "day_of_week",
                F.dayofweek(F.col("review_date"))
            )
            .withColumn(
                "review_age_days",
                F.datediff(F.current_date(), F.col("review_date"))
            )
        )

        return df

    def aggregate_restaurant_stats(self, reviews_df: DataFrame) -> DataFrame:
        """
        Compute aggregate statistics per restaurant:
        - Average rating, review count, avg review length
        - Rating distribution
        - Most recent review date
        """
        stats = (
            reviews_df
            .groupBy("place_id", "restaurant_name")
            .agg(
                F.count("*").alias("review_count"),
                F.avg("rating").alias("avg_rating"),
                F.stddev("rating").alias("rating_stddev"),
                F.avg("review_length").alias("avg_review_length"),
                F.avg("word_count").alias("avg_word_count"),
                F.max("review_date").alias("latest_review_date"),
                F.min("review_date").alias("earliest_review_date"),
                F.sum(F.when(F.col("rating") == 5, 1).otherwise(0)).alias("five_star_count"),
                F.sum(F.when(F.col("rating") == 1, 1).otherwise(0)).alias("one_star_count"),
            )
            .withColumn(
                "rating_stddev",
                F.coalesce(F.col("rating_stddev"), F.lit(0.0))
            )
            .withColumn(
                "positive_ratio",
                (F.col("five_star_count") / F.col("review_count"))
            )
        )

        logger.info(f"Aggregated stats for {stats.count()} restaurants")
        return stats

    def transform_for_warehouse(self, reviews_df: DataFrame) -> dict[str, DataFrame]:
        """
        Transform data into star-schema warehouse format.
        Returns dict of DataFrames for each fact/dimension table.
        """
        # Dimension: Time
        dim_time = (
            reviews_df
            .select(
                F.col("review_date").alias("full_date"),
                F.year("review_date").alias("year"),
                F.month("review_date").alias("month"),
                F.dayofmonth("review_date").alias("day"),
                F.quarter("review_date").alias("quarter"),
                F.dayofweek("review_date").alias("day_of_week"),
                F.weekofyear("review_date").alias("week_of_year"),
            )
            .distinct()
        )

        # Dimension: Restaurants
        dim_restaurants = (
            reviews_df
            .select("place_id", "restaurant_name")
            .distinct()
            .withColumnRenamed("restaurant_name", "name")
        )

        # Fact: Reviews
        fact_reviews = reviews_df.select(
            "review_id",
            "place_id",
            "author_name",
            "rating",
            "text_cleaned",
            "review_length",
            "word_count",
            "review_date",
            "review_age_days",
        )

        return {
            "dim_time": dim_time,
            "dim_restaurants": dim_restaurants,
            "fact_reviews": fact_reviews,
        }

    def stop(self):
        """Stop the Spark session."""
        self.spark.stop()
        logger.info("Spark session stopped")
