from datetime import datetime, timedelta
from src.ingestion.ingest_job import run_guardian_ingestion

def fetch_articles(**context):
    execution_date = context["ds"]  # 'YYYY-MM-DD'
    prev_date = context["prev_ds"]  # ngày hôm trước (Airflow cung cấp sẵn)

    print(f"📅 Fetching Guardian articles from {prev_date} → {execution_date}")

    raw_s3_key = run_guardian_ingestion(
        from_date=prev_date,
        to_date=execution_date,
        page_size=50,
        max_pages=100
    )

    if not raw_s3_key:
        raise ValueError("❌ Ingestion failed — no S3 key returned.")

    print(f"✅ Raw data uploaded to S3: {raw_s3_key}")

    # Push key để các task sau (upload_to_s3, load_to_pg, dbt_run) dùng
    context["ti"].xcom_push(key="raw_s3_key", value=raw_s3_key)