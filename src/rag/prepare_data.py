# src/rag/prepare_data.py
import os
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv() 

def get_postgres_engine():
    user = os.getenv("POSTGRES_USER", "airflow")
    password = os.getenv("POSTGRES_PASSWORD", "airflow")
    db = os.getenv("POSTGRES_DB", "guardian_dw")

    # Kết nối trong Docker network → luôn dùng 'postgres' làm host
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = os.getenv("POSTGRES_PORT", "5432")

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    print(f"✅ Connecting to Postgres at {url}")
    return create_engine(url)

def load_new_articles(since_date: str, limit: int | None = None):
    """
    Lấy các bài báo mới từ staging kể từ since_date.
    since_date: ISO format string (e.g., '2025-10-26')
    """
    engine = get_postgres_engine()

    q = f"""
    SELECT
      a.article_id,
      a.title,
      a.headline,
      a.trail_text AS summary,
      a.body AS content,
      a.publication_date AS published_at,
      a.web_url AS url,
      p.publication_name AS publication,
      s.section_name AS section,
      s.pillar_name AS pillar,
      COALESCE(STRING_AGG(DISTINCT au.author_name, ', '), 'Unknown') AS authors,
      COALESCE(STRING_AGG(DISTINCT k.keyword, ', '), '') AS keywords,
      a.topic_country
    FROM analytics_staging.stg_articles a
    LEFT JOIN analytics_staging.stg_publications p 
      ON a.publication_id = p.publication_id
    LEFT JOIN analytics_staging.stg_sections s 
      ON a.section_id = s.section_id
    LEFT JOIN analytics_staging.stg_article_authors aa 
      ON a.article_id = aa.article_id
    LEFT JOIN analytics_staging.stg_authors au 
      ON aa.author_id = au.author_id
    LEFT JOIN analytics_staging.stg_article_keywords k 
      ON a.article_id = k.article_id
    WHERE a.body IS NOT NULL
      AND a.publication_date >= '{since_date}'
    GROUP BY
      a.article_id, a.title, a.headline, a.trail_text, a.body, a.web_url,
      a.publication_date, p.publication_name, s.section_name, s.pillar_name, a.topic_country
    ORDER BY a.publication_date DESC
    LIMIT {int(limit) if limit else 5000};
    """

    print(f"📥 Loading new articles since {since_date} ...")
    with engine.connect() as conn:
        df = pd.read_sql(text(q), conn)

    print(f"✅ Loaded {len(df)} new articles.")
    return df

def clean_text_fields(df: pd.DataFrame) -> pd.DataFrame:
    """
    Basic cleaning for text fields before embedding.
    - Normalize whitespace
    - Trim
    - Remove rows with very short content (configurable threshold)
    """
    # Ensure expected cols exist
    for col in ["title", "headline", "summary", "content", "authors", "section"]:
        if col not in df.columns:
            df[col] = ""

    # Normalize whitespace and types
    for col in ["title", "headline", "summary", "content", "authors", "section"]:
        df[col] = (
            df[col]
            .fillna("")
            .astype(str)
            .str.replace(r"\s+", " ", regex=True)
            .str.strip()
        )

    # Drop rows where content is too short to be useful
    min_content_chars = int(os.getenv("MIN_CONTENT_CHARS", 200))
    before = len(df)
    df = df[df["content"].str.len() >= min_content_chars].reset_index(drop=True)
    after = len(df)
    print(f"🧹 Cleaned dataframe → {after} articles remain (dropped {before - after} with < {min_content_chars} chars).")

    # Optionally keep only necessary columns and rename for downstream steps
    keep_cols = ["article_id", "title", "headline", "summary", "content", "published_at", "section", "authors", "url"]
    df = df[[c for c in keep_cols if c in df.columns]]

    # Convert published_at to pandas datetime if present
    if "published_at" in df.columns:
        df["published_at"] = pd.to_datetime(df["published_at"], errors="coerce")

    return df


def save_clean_data(df: pd.DataFrame, output_path: str = "data/clean_guardian_articles.parquet"):
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out, index=False)
    print(f"💾 Saved cleaned data → {out.resolve()}")


if __name__ == "__main__":
    print("🚀 Preparing Guardian articles for RAG chatbot...")
    limit = os.getenv("LOAD_LIMIT")
    limit_val = int(limit) if limit and limit.isdigit() else 10000

    df = load_new_articles(limit=limit_val, since_date="2025-10-28")
    df_clean = clean_text_fields(df)
    save_clean_data(df_clean)
    print(df_clean)
    print("✅ Done! Data ready for embedding.")