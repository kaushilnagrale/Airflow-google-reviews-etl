"""
NLP Processing DAG
Runs NLP model inference on unprocessed reviews:
1. Fetch unprocessed reviews from MongoDB
2. Run BERT sentiment analysis and score prediction
3. Save predictions to MongoDB and PostgreSQL warehouse
"""

from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


default_args = {
    "owner": "data-science",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=3),
}


def fetch_unprocessed_reviews(**context):
    """Fetch reviews that haven't been scored by the NLP model."""
    from scripts.etl.mongo_loader import MongoLoader

    mongo = MongoLoader()
    try:
        reviews = mongo.get_unprocessed_reviews(batch_size=5000)
        context["ti"].xcom_push(key="unprocessed_reviews", value=reviews)
        return f"Fetched {len(reviews)} unprocessed reviews"
    finally:
        mongo.close()


def run_nlp_inference(**context):
    """Run BERT model for sentiment analysis and score prediction."""
    from scripts.nlp.score_predictor import ReviewScorePredictor

    reviews = context["ti"].xcom_pull(
        key="unprocessed_reviews", task_ids="fetch_unprocessed_reviews"
    )

    if not reviews:
        return "No reviews to process"

    predictor = ReviewScorePredictor()
    processed = predictor.process_reviews(reviews)

    context["ti"].xcom_push(key="processed_reviews", value=processed)

    # Compute summary statistics
    avg_score = sum(r["predicted_score"] for r in processed) / len(processed)
    avg_conf = sum(r["confidence"] for r in processed) / len(processed)

    return (
        f"Processed {len(processed)} reviews | "
        f"Avg predicted score: {avg_score:.2f} | "
        f"Avg confidence: {avg_conf:.3f}"
    )


def save_predictions_mongo(**context):
    """Save NLP predictions back to MongoDB."""
    from scripts.etl.mongo_loader import MongoLoader

    processed = context["ti"].xcom_pull(
        key="processed_reviews", task_ids="run_nlp_inference"
    )

    if not processed:
        return "No predictions to save"

    mongo = MongoLoader()
    try:
        count = mongo.save_processed_reviews(processed)
        return f"Saved {count} predictions to MongoDB"
    finally:
        mongo.close()


def save_predictions_warehouse(**context):
    """Save NLP predictions to PostgreSQL warehouse."""
    from scripts.etl.warehouse_loader import WarehouseLoader

    processed = context["ti"].xcom_pull(
        key="processed_reviews", task_ids="run_nlp_inference"
    )

    if not processed:
        return "No predictions to save"

    warehouse = WarehouseLoader()
    try:
        predictions = [
            {
                "review_id": r["review_id"],
                "place_id": r["place_id"],
                "predicted_score": r["predicted_score"],
                "sentiment_label": r["sentiment_label"],
                "confidence": r["confidence"],
                "model_version": r["model_version"],
                "predicted_at": r["predicted_at"],
            }
            for r in processed
        ]
        count = warehouse.load_fact_predictions(predictions)
        return f"Saved {count} predictions to warehouse"
    finally:
        warehouse.close()


with DAG(
    dag_id="nlp_processing_pipeline",
    default_args=default_args,
    description="NLP inference: fetch reviews → BERT prediction → save results",
    schedule_interval="0 4 * * *",  # Daily at 4 AM (after ingestion)
    start_date=days_ago(1),
    catchup=False,
    tags=["nlp", "ml", "sentiment"],
    max_active_runs=1,
) as dag:

    t_fetch = PythonOperator(
        task_id="fetch_unprocessed_reviews",
        python_callable=fetch_unprocessed_reviews,
    )

    t_inference = PythonOperator(
        task_id="run_nlp_inference",
        python_callable=run_nlp_inference,
    )

    t_save_mongo = PythonOperator(
        task_id="save_predictions_mongo",
        python_callable=save_predictions_mongo,
    )

    t_save_warehouse = PythonOperator(
        task_id="save_predictions_warehouse",
        python_callable=save_predictions_warehouse,
    )

    # fetch → inference → [mongo, warehouse]
    t_fetch >> t_inference >> [t_save_mongo, t_save_warehouse]
