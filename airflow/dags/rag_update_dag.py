# dags/rag_update_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import os
import sys

# Thêm src vào PYTHONPATH (để import các module)
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from tasks.rag.extract_task import extract_and_clean
from tasks.rag.chunk_task import chunk_new_articles
from tasks.rag.update_index_task import update_vector_index
from tasks.notify_task import notify_success

# --- Config ---
DEFAULT_ARGS = {
    "owner": "vy",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

DAG_ID = "rag_update_dag"

# --- Define DAG ---
with DAG(
        dag_id=DAG_ID,
        default_args=DEFAULT_ARGS,
        description="Daily RAG update — refresh FAISS index with new Guardian articles",
        schedule_interval="0 6 * * *",  # chạy 6AM mỗi ngày
        start_date=datetime(2025, 10, 27),
        catchup=False,
        tags=["rag", "guardian", "chatbot"],
) as dag:

    task_extract = PythonOperator(
        task_id="extract_and_clean",
        python_callable=extract_and_clean,
    )

    task_chunk = PythonOperator(
        task_id="chunk_new_articles",
        python_callable=chunk_new_articles,
    )

    task_update_index = PythonOperator(
        task_id="update_vector_index",
        python_callable=update_vector_index,
    )

    task_notify = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
    )

    end = EmptyOperator(task_id="end")

    task_extract >> task_chunk >> task_update_index >> task_notify >> end