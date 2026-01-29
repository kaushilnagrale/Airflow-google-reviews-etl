from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from scripts.extract import extract_data
from scripts.transform import transform_data
from scripts.load import load_data

default_args = {
    "owner": "kaushil",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

with DAG(
    dag_id="google_reviews_etl",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    extract = PythonOperator(
        task_id="extract_reviews",
        python_callable=extract_data,
    )

    transform = PythonOperator(
        task_id="transform_reviews",
        python_callable=transform_data,
    )

    load = PythonOperator(
        task_id="load_reviews",
        python_callable=load_data,
    )

    extract >> transform >> load
