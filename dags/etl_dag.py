# dags/etl_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from etl.extract import extract_and_stage

# Define the DAG
with DAG(
    dag_id="etl_pipeline",
    start_date=datetime(2026, 7, 30),
    schedule_interval="@daily",  # run once per day
    catchup=False,
    tags=["etl", "duckdb", "pipeline"]
) as dag:

    # Task: Extract + Stage
    extract_stage_task = PythonOperator(
        task_id="extract_and_stage",
        python_callable=extract_and_stage
    )

    # Later we’ll add transform and load tasks here
    extract_stage_task
