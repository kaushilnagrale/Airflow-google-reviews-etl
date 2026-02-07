"""
Review Ingestion DAG
Orchestrates the full ETL pipeline:
1. Extract reviews from Google Places API
2. Store raw data in S3 and MongoDB
3. Transform with Spark
4. Load into PostgreSQL warehouse
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# ─── Default DAG arguments ──────────────────────────────────
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}


# ─── Task callables ─────────────────────────────────────────
def extract_reviews(**context):
    """Extract restaurant reviews from Google Places API."""
    from scripts.etl.google_api_extractor import GoogleAPIExtractor

    extractor = GoogleAPIExtractor()
    all_data = []

    for batch in extractor.run_full_extraction():
        all_data.append(batch)

    # Push data to XCom for downstream tasks
    context["ti"].xcom_push(key="extraction_data", value=all_data)
    context["ti"].xcom_push(key="batch_count", value=len(all_data))
    return f"Extracted data for {len(all_data)} restaurants"


def upload_to_s3(**context):
    """Upload raw data to S3 data lake."""
    from scripts.etl.s3_manager import S3Manager

    data = context["ti"].xcom_pull(key="extraction_data", task_ids="extract_reviews")
    s3 = S3Manager()
    uploaded_keys = []

    for batch in data:
        location = batch["location"]
        reviews = batch["reviews"]
        if reviews:
            key = s3.upload_raw_reviews(reviews, location)
            uploaded_keys.append(key)

    context["ti"].xcom_push(key="s3_keys", value=uploaded_keys)
    return f"Uploaded {len(uploaded_keys)} files to S3"


def load_to_mongodb(**context):
    """Load raw reviews and restaurant metadata to MongoDB."""
    from scripts.etl.mongo_loader import MongoLoader

    data = context["ti"].xcom_pull(key="extraction_data", task_ids="extract_reviews")
    mongo = MongoLoader()

    total_reviews = 0
    total_restaurants = 0

    try:
        for batch in data:
            mongo.upsert_restaurant(batch["restaurant"])
            total_restaurants += 1
            stats = mongo.upsert_reviews(batch["reviews"])
            total_reviews += stats["inserted"] + stats["modified"]
    finally:
        mongo.close()

    return f"Loaded {total_restaurants} restaurants, {total_reviews} reviews to MongoDB"


def spark_transform(**context):
    """Run Spark transformations on extracted data."""
    from scripts.etl.spark_transformer import SparkTransformer

    data = context["ti"].xcom_pull(key="extraction_data", task_ids="extract_reviews")

    # Flatten all reviews
    all_reviews = []
    for batch in data:
        all_reviews.extend(batch["reviews"])

    if not all_reviews:
        return "No reviews to transform"

    transformer = SparkTransformer()

    try:
        # Load and clean
        reviews_df = transformer.load_reviews_from_json(all_reviews)
        cleaned_df = transformer.clean_reviews(reviews_df)
        featured_df = transformer.engineer_features(cleaned_df)

        # Aggregate stats
        stats_df = transformer.aggregate_restaurant_stats(featured_df)

        # Prepare warehouse tables
        warehouse_data = transformer.transform_for_warehouse(featured_df)

        # Collect results for downstream tasks
        reviews_list = [row.asDict() for row in featured_df.collect()]
        stats_list = [row.asDict() for row in stats_df.collect()]

        context["ti"].xcom_push(key="transformed_reviews", value=reviews_list)
        context["ti"].xcom_push(key="restaurant_stats", value=stats_list)

        return f"Transformed {len(reviews_list)} reviews, {len(stats_list)} restaurant stats"
    finally:
        transformer.stop()


def load_to_warehouse(**context):
    """Load transformed data into PostgreSQL data warehouse."""
    from scripts.etl.warehouse_loader import WarehouseLoader

    reviews = context["ti"].xcom_pull(
        key="transformed_reviews", task_ids="spark_transform"
    )
    data = context["ti"].xcom_pull(
        key="extraction_data", task_ids="extract_reviews"
    )

    warehouse = WarehouseLoader()

    try:
        # Load dimension: restaurants
        restaurants = [batch["restaurant"] for batch in data]
        warehouse.load_dim_restaurants(restaurants)

        # Load fact: reviews
        if reviews:
            warehouse.load_fact_reviews(reviews)

        return f"Loaded {len(restaurants)} restaurants, {len(reviews or [])} reviews to warehouse"
    finally:
        warehouse.close()


# ─── DAG Definition ──────────────────────────────────────────
with DAG(
    dag_id="review_ingestion_pipeline",
    default_args=default_args,
    description="ETL pipeline: Google API → S3 → Spark → MongoDB → PostgreSQL",
    schedule_interval="0 2 * * *",  # Daily at 2 AM
    start_date=days_ago(1),
    catchup=False,
    tags=["etl", "ingestion", "reviews"],
    max_active_runs=1,
) as dag:

    t_extract = PythonOperator(
        task_id="extract_reviews",
        python_callable=extract_reviews,
    )

    t_s3 = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
    )

    t_mongo = PythonOperator(
        task_id="load_to_mongodb",
        python_callable=load_to_mongodb,
    )

    t_spark = PythonOperator(
        task_id="spark_transform",
        python_callable=spark_transform,
    )

    t_warehouse = PythonOperator(
        task_id="load_to_warehouse",
        python_callable=load_to_warehouse,
    )

    # Task dependencies
    #              ┌─→ upload_to_s3
    # extract ────┤
    #              ├─→ load_to_mongodb
    #              │
    #              └─→ spark_transform ──→ load_to_warehouse
    t_extract >> [t_s3, t_mongo, t_spark]
    t_spark >> t_warehouse
