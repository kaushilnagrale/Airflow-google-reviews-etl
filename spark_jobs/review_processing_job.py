"""
Standalone Spark Job for Review Processing
Can be submitted directly to Spark cluster:
    spark-submit --master spark://spark-master:7077 review_processing_job.py
"""

import sys
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType,
    IntegerType, TimestampType,
)


def create_spark_session():
    """Create and configure Spark session."""
    return (
        SparkSession.builder
        .appName("RestaurantReviewProcessing")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )


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
    StructField("ingested_at", StringType(), True),
])


def process_reviews(spark, input_path: str, output_path: str):
    """
    Main Spark processing pipeline:
    1. Read raw JSON reviews
    2. Clean and deduplicate
    3. Engineer features
    4. Compute aggregations
    5. Write output as Parquet
    """
    print(f"Reading reviews from: {input_path}")

    # Read raw data
    df = spark.read.json(input_path, schema=REVIEW_SCHEMA)
    raw_count = df.count()
    print(f"Loaded {raw_count} raw reviews")

    # ── Step 1: Deduplicate ──────────────────────────────────
    df = df.dropDuplicates(["review_id"])

    # ── Step 2: Clean text ───────────────────────────────────
    df = df.withColumn(
        "text_cleaned",
        F.trim(F.regexp_replace(
            F.regexp_replace(F.col("text"), r"<[^>]+>", " "),
            r"\s+", " "
        ))
    )

    # Filter empty reviews
    df = df.filter(
        F.col("text_cleaned").isNotNull() &
        (F.length(F.col("text_cleaned")) > 5)
    )

    # ── Step 3: Feature engineering ──────────────────────────
    df = (
        df
        .withColumn("review_length", F.length("text_cleaned"))
        .withColumn("word_count", F.size(F.split("text_cleaned", r"\s+")))
        .withColumn(
            "review_date",
            F.from_unixtime("time_epoch").cast(TimestampType())
        )
        .withColumn("review_year", F.year("review_date"))
        .withColumn("review_month", F.month("review_date"))
        .withColumn("day_of_week", F.dayofweek("review_date"))
        .withColumn(
            "rating_category",
            F.when(F.col("rating") >= 4, "positive")
            .when(F.col("rating") <= 2, "negative")
            .otherwise("neutral")
        )
    )

    clean_count = df.count()
    print(f"After cleaning: {clean_count} reviews (removed {raw_count - clean_count})")

    # ── Step 4: Restaurant-level aggregations ─────────────────
    restaurant_stats = (
        df
        .groupBy("place_id", "restaurant_name")
        .agg(
            F.count("*").alias("review_count"),
            F.round(F.avg("rating"), 2).alias("avg_rating"),
            F.round(F.stddev("rating"), 2).alias("rating_stddev"),
            F.round(F.avg("review_length"), 0).alias("avg_review_length"),
            F.max("review_date").alias("latest_review"),
            F.sum(F.when(F.col("rating") >= 4, 1).otherwise(0)).alias("positive_reviews"),
            F.sum(F.when(F.col("rating") <= 2, 1).otherwise(0)).alias("negative_reviews"),
        )
        .withColumn(
            "positive_ratio",
            F.round(F.col("positive_reviews") / F.col("review_count"), 3)
        )
    )

    # ── Step 5: Write output ─────────────────────────────────
    reviews_output = f"{output_path}/reviews"
    stats_output = f"{output_path}/restaurant_stats"

    df.write.mode("overwrite").parquet(reviews_output)
    restaurant_stats.write.mode("overwrite").parquet(stats_output)

    print(f"Written {clean_count} reviews to {reviews_output}")
    print(f"Written {restaurant_stats.count()} restaurant stats to {stats_output}")

    # Print summary
    restaurant_stats.orderBy(F.desc("avg_rating")).show(20, truncate=False)


if __name__ == "__main__":
    input_path = sys.argv[1] if len(sys.argv) > 1 else "/opt/spark-data/raw"
    output_path = sys.argv[2] if len(sys.argv) > 2 else "/opt/spark-data/processed"

    spark = create_spark_session()
    try:
        process_reviews(spark, input_path, output_path)
    finally:
        spark.stop()
