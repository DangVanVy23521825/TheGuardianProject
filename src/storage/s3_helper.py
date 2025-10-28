import boto3
import json
import os
import io
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

def get_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION", "ap-southeast-2")
    )

def upload_json_to_s3(data, bucket_name, prefix="raw/guardian"):
    """Upload raw JSON data to S3 (for raw zone)."""
    s3 = get_s3_client()
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    key = f"{prefix}/articles_{timestamp}.json"

    s3.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=json.dumps(data, indent=2),
        ContentType="application/json"
    )

    print(f"✅ Uploaded raw data to s3://{bucket_name}/{key}")
    return key

def read_json_from_s3(bucket_name, key):
    """Read JSON file from S3 and return parsed data."""
    s3 = get_s3_client()
    obj = s3.get_object(Bucket=bucket_name, Key=key)
    data = json.loads(obj["Body"].read().decode("utf-8"))
    return data

def upload_dataframe_to_s3(df, bucket_name, key):
    """Upload pandas DataFrame as Parquet directly to S3."""
    import pandas as pd
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3 = get_s3_client()
    s3.put_object(Bucket=bucket_name, Key=key, Body=buffer.getvalue())
    print(f"✅ Uploaded parquet to s3://{bucket_name}/{key}")