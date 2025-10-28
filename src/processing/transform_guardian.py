import os
import re
import pandas as pd
from datetime import datetime, timezone
from io import BytesIO
from src.storage.s3_helper import get_s3_client, read_json_from_s3


def clean_html(text: str) -> str:
    if not isinstance(text, str):
        return ""
    return re.sub(r"<[^>]*>", "", text).strip()


def extract_from_id(article_id):
    if not isinstance(article_id, str):
        return pd.Series({
            "article_type": None,
            "article_year": None,
            "article_month": None,
            "article_day": None,
            "slug_keywords": [],
            "slug_length": None,
            "is_live_blog": None,
            "topic_country": None
        })

    parts = article_id.split('/')
    year_idx = next((i for i, p in enumerate(parts) if re.fullmatch(r"\d{4}", p)), None)
    if year_idx is None:
        # ✅ FIX: Không trả về "..." nữa, mà trả về dict rỗng an toàn
        return pd.Series({
            "article_type": None,
            "article_year": None,
            "article_month": None,
            "article_day": None,
            "slug_keywords": [],
            "slug_length": None,
            "is_live_blog": None,
            "topic_country": None
        })

    article_type = parts[1] if year_idx > 1 else None
    year = int(parts[year_idx])
    month = parts[year_idx + 1] if len(parts) > year_idx + 1 else None
    day = int(parts[year_idx + 2]) if len(parts) > year_idx + 2 and parts[year_idx + 2].isdigit() else None

    slug_parts = parts[year_idx + 3:]
    slug = "-".join(slug_parts)

    slug_keywords = re.findall(r"[a-zA-Z]+", slug)
    slug_length = len(slug_keywords)
    is_live_blog = article_type and "live" in article_type.lower()
    known_countries = {"uk", "us", "china", "india", "australia", "eu"}
    topic_country = next((w for w in slug_keywords if w.lower() in known_countries), None)

    return pd.Series({
        "article_type": article_type,
        "article_year": year,
        "article_month": month,
        "article_day": day,
        "slug_keywords": slug_keywords,
        "slug_length": slug_length,
        "is_live_blog": bool(is_live_blog),
        "topic_country": topic_country
    })


def transform_guardian_json_to_parquet(raw_key: str, bucket_name: str):
    s3 = get_s3_client()

    print(f"📥 Reading raw data from s3://{bucket_name}/{raw_key}")
    data = read_json_from_s3(bucket_name, raw_key)
    df = pd.json_normalize(data, sep="_")

    cols_to_keep = [
        "id", "type", "sectionName",
        "webPublicationDate", "webTitle",
        "fields_headline", "fields_trailText",
        "fields_byline", "fields_wordcount",
        "fields_publication", "fields_thumbnail",
        "fields_body",  # nội dung chính
        "pillarName", "webUrl"
    ]
    df = df[[c for c in cols_to_keep if c in df.columns]]

    df["webPublicationDate"] = pd.to_datetime(df["webPublicationDate"], errors="coerce")
    df["fields_wordcount"] = pd.to_numeric(df["fields_wordcount"], errors="coerce")

    for col in ["fields_trailText", "fields_headline", "fields_body"]:
        if col in df.columns:
            df[col] = df[col].apply(clean_html)
    df["fields_body"] = df.get("fields_body", "").fillna("").astype(str).str.strip()
    df["fields_byline"] = df["fields_byline"].fillna("Unknown").str.title()
    df["sectionName"] = df["sectionName"].fillna("Unknown").str.title()
    df["webTitle"] = df["webTitle"].fillna("").str.strip()
    df["fields_publication"] = df["fields_publication"].fillna("The Guardian")

    # ---- Feature Engineering ----
    df["title_length"] = df["webTitle"].apply(lambda x: len(x.split()) if isinstance(x, str) else 0)
    df["headline_length"] = df["fields_headline"].apply(lambda x: len(x.split()) if isinstance(x, str) else 0)
    df["days_since_publication"] = (
        pd.Timestamp.now(tz=timezone.utc) - df["webPublicationDate"]
    ).dt.days
    df["has_thumbnail"] = df["fields_thumbnail"].notnull().astype(int)

    df["ingested_at"] = datetime.now(timezone.utc)
    df["source_system"] = "guardian_api"
    df["raw_s3_key"] = raw_key
    df["processed_by"] = "transform_guardian_v2"

    df = df.join(df["id"].apply(extract_from_id))
    df = df.rename(columns={"id": "article_id"})

    # ✅ FIX: loại bỏ mọi giá trị Ellipsis hoặc invalid Python objects
    df = df.applymap(lambda x: None if x is Ellipsis else x)

    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    parquet_key = f"processed/{today}/guardian_articles_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.parquet"
    s3.put_object(Bucket=bucket_name, Key=parquet_key, Body=buffer.getvalue())

    print(f"✅ Transformed & uploaded parquet to s3://{bucket_name}/{parquet_key}")
    print(f"✅ Uploaded parquet ({len(df)} records, {buffer.getbuffer().nbytes / 1024:.2f} KB) → {parquet_key}")
    return parquet_key