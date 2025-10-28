import os
from datetime import datetime, timedelta
from src.ingestion.guardian_api import fetch_guardian_articles
from src.storage.s3_helper import upload_json_to_s3

def run_guardian_ingestion(from_date=None, to_date=None, page_size=50, max_pages=200):
    """
    Fetch Guardian articles and upload as JSON to S3 (bronze/raw zone).
    """
    bucket_name = os.getenv("S3_BUCKET", "the-guardian-data")
    today_str = datetime.utcnow().strftime("%Y-%m-%d")
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    prefix = f"raw/{today_str}/{timestamp}"

    # 🗓️ Nếu không truyền, mặc định lấy 1 ngày gần nhất
    if from_date is None or to_date is None:
        to_date = datetime.utcnow().strftime("%Y-%m-%d")
        from_date = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"🚀 Fetching Guardian articles from {from_date} → {to_date}")

    # 🔹 Fetch paginated data
    data = fetch_guardian_articles(
        from_date=from_date,
        to_date=to_date,
        page_size=page_size,
        max_pages=max_pages
    )

    if not data:
        print("⚠️ No articles fetched — skipping upload.")
        return None

    # 🔹 Upload JSON to S3
    print(f"💾 Uploading raw data to s3://{bucket_name}/{prefix}/")
    s3_key = upload_json_to_s3(data, bucket_name=bucket_name, prefix=prefix)

    print(f"✅ Ingestion complete! File stored at: s3://{bucket_name}/{s3_key}")
    return s3_key