from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

# Add scripts path
sys.path.append(os.path.join(os.path.dirname(__file__), '../scripts'))

from fetch_weather import main

with DAG(
    dag_id="weather_data_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    run_pipeline = PythonOperator(
        task_id="fetch_and_store_weather",
        python_callable=main
    )

    run_pipeline
