from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import sys

sys.path.insert(0, "/opt/expdir/scripts")

from extract import extract_data
from transform import load_staging_data, transform_data
from load import load_data
from validate_extraction import validate_extracted_data
from validate_transformation import validate_transformed_data


# DAG arguments
default_args = {
    "owner": "marko",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "etl_pipeline",
    default_args=default_args,
    description="etl pipeline",
    start_date=days_ago(1),
    schedule_interval=None,
    tags=["test"],
    catchup=False,
    max_active_runs=1,
    ) as dag:

    # Defining tasks:
    # Extraction task
    def extract_task():
        extract_data()

    # Validation extraction task
    def validate_extraction_task():
        validate_extracted_data()

    # Transformation task
    def transform_task():
        staging_df = load_staging_data()
        if staging_df is not None:
            transform_data(staging_df)
        else:
            raise ValueError("Failed to load staging data")

    # Validation transformation task
    def validate_transformation_task():
        validate_transformed_data()

    # Loading task
    def load_task():
        load_data()

    # PythonOperators
    extract = PythonOperator(
        task_id="extract",
        python_callable=extract_task,
    )

    validate_extraction = PythonOperator(
        task_id="validate_extraction",
        python_callable=validate_extraction_task,
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_task,
    )

    validate_transformation = PythonOperator(
        task_id="validate_transformation",
        python_callable=validate_transformation_task,
    )

    load = PythonOperator(
        task_id="load",
        python_callable=load_task,
    )

    # Task dependencies
    extract >> validate_extraction >> transform >> validate_transformation >> load
