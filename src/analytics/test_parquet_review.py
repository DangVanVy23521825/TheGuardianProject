import pandas as pd
from io import BytesIO
from src.storage.s3_helper import get_s3_client

bucket_name = "the-guardian-data"
parquet_key = "processed/2025-10-08/guardian_articles_20251008_030002.parquet"

s3 = get_s3_client()

buffer = BytesIO()
s3.download_fileobj(bucket_name, parquet_key, buffer)
buffer.seek(0)

df = pd.read_parquet(buffer)

print("📊 Số dòng:", len(df))
print("📋 Các cột:", df.columns.tolist())
print("\n🔎 5 dòng đầu tiên:")
print(df.head(5))

print("\n🧱 Thông tin schema:")
print(df.info())