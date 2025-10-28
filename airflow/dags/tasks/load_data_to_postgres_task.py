import os
import logging
from datetime import datetime
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from src.analytics.load_parquet_to_postgres import load_parquet_to_postgres

logger = logging.getLogger(__name__)

def load_data_to_postgres(**context):
    """
    Load processed Guardian Parquet file from S3 into the data warehouse (guardian_dw).
    """
    ti = context["ti"]
    parquet_key = ti.xcom_pull(task_ids="upload_to_s3", key="parquet_key")
    if not parquet_key:
        raise ValueError("Missing parquet_key from previous task.")

    bucket_name = os.getenv("S3_BUCKET", "the-guardian-data")

    print(f"Loading parquet from S3 → guardian_dw: {parquet_key}")
    rows_loaded = load_parquet_to_postgres(parquet_key, bucket_name)
    print(f"✅ Data successfully loaded into guardian_dw ({rows_loaded} rows).")

    # Push số dòng đã load lên XCom
    ti.xcom_push(key="rows_loaded", value=rows_loaded)


def log_ingestion_metadata(**context):
    """
    Log metadata of ingestion into guardian_ingestion_log (in Airflow's Postgres DB).
    """
    ti = context["ti"]
    parquet_key = ti.xcom_pull(task_ids="upload_to_s3", key="parquet_key")
    rows_loaded = ti.xcom_pull(task_ids="load_data_to_postgres", key="rows_loaded") or 0

    PG_USER = os.getenv("POSTGRES_USER", "airflow")
    PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "airflow")
    PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
    PG_PORT = os.getenv("POSTGRES_PORT", 5432)
    PG_DB = os.getenv("POSTGRES_LOG_DB", "guardian_dw")

    engine = create_engine(f"postgresql://{PG_USER}:{quote_plus(PG_PASSWORD)}@{PG_HOST}:{PG_PORT}/{PG_DB}")

    try:
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO guardian_ingestion_log (parquet_key, load_time, rows_loaded, status)
                VALUES (:key, :time, :rows, :status)
            """), {
                "key": parquet_key,
                "time": datetime.utcnow(),
                "rows": rows_loaded,
                "status": "SUCCESS"
            })
        logger.info("✅ Logged ingestion metadata to guardian_ingestion_log.")
    except Exception as e:
        logger.error(f"❌ Failed to log ingestion metadata: {e}")