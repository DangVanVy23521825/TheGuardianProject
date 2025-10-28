from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from tasks.fetch_articles_task import fetch_articles
from tasks.upload_to_s3_task import upload_to_s3
from tasks.load_data_to_postgres_task import load_data_to_postgres, log_ingestion_metadata
from tasks.trigger_dbt_task import trigger_dbt_run
from tasks.notify_task import notify_success

default_args = {
    "owner": "guardian_team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email": ["dangvanvy112@gmail.com"],
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="guardian_daily_ingestion_dag",
    default_args=default_args,
    description="Daily Guardian batch ingestion + dbt transform",
    schedule_interval="0 2 * * *",  # 2:00 AM UTC
    start_date=datetime(2025, 10, 21),
    catchup=False,
    tags=["guardian", "batch", "ingestion"],
) as dag:

    start = EmptyOperator(task_id="start")

    fetch = PythonOperator(
        task_id="fetch_articles",
        python_callable=fetch_articles,
    )

    upload = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
    )

    load_pg = PythonOperator(
        task_id="load_data_to_postgres",
        python_callable=load_data_to_postgres,
    )

    log_meta = PythonOperator(
        task_id="log_ingestion_metadata",
        python_callable=log_ingestion_metadata,
    )

    dbt_run = PythonOperator(
        task_id="trigger_dbt_run",
        python_callable=trigger_dbt_run,
    )

    notify = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
    )

    end = EmptyOperator(task_id="end")

    start >> fetch >> upload >> load_pg >> log_meta >> dbt_run >> notify >> end