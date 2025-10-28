import os

from src.processing.transform_guardian import transform_guardian_json_to_parquet

def upload_to_s3(**context):
    ti = context["ti"]
    raw_key = ti.xcom_pull(task_ids="fetch_articles", key="raw_s3_key")
    bucket_name = os.getenv("S3_BUCKET", "the-guardian-data")

    print(f"🚀 Transforming raw data file: {raw_key}")
    parquet_key = transform_guardian_json_to_parquet(raw_key, bucket_name)

    # Push parquet path to XCom for downstream tasks
    ti.xcom_push(key="parquet_key", value=parquet_key)