#!/usr/bin/env python3

import logging
import os
import json
import re
import pandas as pd
from io import BytesIO
from typing import Optional
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from src.storage.s3_helper import get_s3_client

# -------------------------
# Config
# -------------------------
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_BUCKET = os.getenv("S3_BUCKET", "the-guardian-data")
S3_KEY = os.getenv("S3_KEY", "processed/2025-10-15/guardian_articles_20251015_070945.parquet")

PG_USER = os.getenv("POSTGRES_USER", "airflow")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "airflow")
PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = int(os.getenv("POSTGRES_PORT", 5432))
PG_DB = os.getenv("POSTGRES_DB", "guardian_dw")
SCHEMA = "dw"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# -------------------------
# Helpers
# -------------------------
def create_db_engine():
    pg_url = f"postgresql://{PG_USER}:{quote_plus(PG_PASSWORD)}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    return create_engine(pg_url, client_encoding="utf8")

def upsert_dimension(conn, table, unique_col, values):
    """Upsert unique values and return mapping {value: id}"""
    id_col = f"{table[:-1]}_id"
    insert_sql = text(f"""
        INSERT INTO {SCHEMA}.{table} ({unique_col})
        VALUES (:val)
        ON CONFLICT ({unique_col}) DO NOTHING;
    """)
    select_sql = text(f"SELECT {unique_col}, {id_col} FROM {SCHEMA}.{table} WHERE {unique_col} = :val;")
    mapping = {}
    for val in values:
        if not val or (isinstance(val, float) and pd.isna(val)):
            continue
        val = str(val).strip()
        if val == "":
            continue
        conn.execute(insert_sql, {"val": val})
        res = conn.execute(select_sql, {"val": val}).fetchone()
        if res:
            mapping[val] = res[1]
    return mapping

def fetch_existing_article_ids(conn, article_ids):
    q = text(f"SELECT article_id FROM {SCHEMA}.articles WHERE article_id = ANY(:ids)")
    rows = conn.execute(q, {"ids": list(article_ids)}).fetchall()
    return {r[0] for r in rows}

def normalize_section_key(name: str) -> Optional[str]:
    if not isinstance(name, str):
        return None
    s = name.lower().strip()
    s = s.replace("&", "and")
    # keep alphanum, replace others by hyphen
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-+", "-", s).strip("-")
    return s

def load_parquet_to_postgres(s3_key: str, bucket_name: str, schema: str = "dw"):
    """
    Load transformed Guardian parquet file from S3 into Postgres DW schema.
    """

    s3 = get_s3_client()
    logger.info(f"📦 Downloading parquet from s3://{bucket_name}/{s3_key}")
    obj = s3.get_object(Bucket=bucket_name, Key=s3_key)
    df = pd.read_parquet(BytesIO(obj["Body"].read()))
    logger.info("✅ Loaded parquet: %s rows", len(df))

    required = ["article_id", "webPublicationDate"]
    for r in required:
        if r not in df.columns:
            raise RuntimeError(f"❌ Missing column: {r}")

    engine = create_db_engine()
    with engine.connect() as conn:
        trans = conn.begin()

        try:
            author_raw = df.get("fields_byline", pd.Series(dtype=str)).dropna().astype(str)
            all_authors = []
            for s in author_raw.unique():
                parts = [p.strip() for p in s.split(",") if p.strip()]
                all_authors.extend(parts)
            author_map = upsert_dimension(conn, "authors", "author_name", list(dict.fromkeys(all_authors)))
            logger.info("🔹 Upserted authors: %s", len(author_map))

            # Sections: try to use sectionId if exists, otherwise build from sectionName/pillarName
            if "sectionId" in df.columns:
                sections_df = df[["sectionId", "sectionName"]].dropna().drop_duplicates()
                sections_df.columns = ["section_key", "section_name"]
                # normalize keys
                sections_df["section_key"] = sections_df["section_key"].astype(str).apply(normalize_section_key)
                # Some sectionId values may already be normalized keys (keep as-is)
                sections_df = sections_df.drop_duplicates(subset=["section_key"])
                sections_df = sections_df[["section_key", "section_name"]]
            else:
                # Fallback khi file parquet không có sectionId
                sections_df = df[["sectionName", "pillarName"]].dropna(subset=["sectionName"]).drop_duplicates()
                sections_df["section_key"] = (
                    sections_df["sectionName"].astype(str).apply(normalize_section_key)
                )
                sections_df = sections_df[["section_key", "sectionName", "pillarName"]]
                sections_df.columns = ["section_key", "section_name", "pillar_name"]

            # Insert into dw.sections
            insert_section_sql = text(f"""
                        INSERT INTO {schema}.sections (section_key, section_name, pillar_name)
                        VALUES (:key, :name, :pillar)
                        ON CONFLICT (section_key)
                        DO UPDATE SET
                            section_name = EXCLUDED.section_name,
                            pillar_name = EXCLUDED.pillar_name;
                    """)

            for _, row in sections_df.iterrows():
                conn.execute(insert_section_sql, {
                    "key": row["section_key"],
                    "name": row["section_name"],
                    "pillar": row.get("pillar_name")
                })

            logger.info("🔹 Upserted sections: %s", len(sections_df))

            # Publications
            pubs_values = (
                df["fields_publication"].dropna().astype(str).str.strip().unique().tolist()
                if "fields_publication" in df.columns else []
            )
            pub_map = upsert_dimension(conn, "publications", "publication_name", pubs_values)
            logger.info("🔹 Upserted publications: %s", len(pub_map))

            # ✅ FIX: Re-fetch mappings using SAME connection (conn) instead of engine
            sections_db = pd.read_sql(
                text(f"SELECT section_id, section_key, section_name FROM {schema}.sections"),
                conn  # ← Changed from engine to conn
            )
            pubs_db = pd.read_sql(
                text(f"SELECT publication_id, publication_name FROM {schema}.publications"),
                conn  # ← Changed from engine to conn
            )

            # --------------------
            # Filter new articles
            # --------------------
            incoming_ids = list(df["article_id"].dropna().unique())
            existing_ids = fetch_existing_article_ids(conn, incoming_ids)
            new_df = df[~df["article_id"].isin(existing_ids)].copy()
            logger.info("ℹ️ New articles to insert: %s", len(new_df))

            if new_df.empty:
                logger.info("✅ No new articles. Done.")
                trans.commit()
                return

            # --------------------
            # Prepare Articles — robust section mapping
            # --------------------
            # Build normalized map from sections_db
            sections_db["section_key_norm"] = sections_db["section_key"].astype(str).apply(normalize_section_key)
            map_norm_to_id = dict(zip(sections_db["section_key_norm"], sections_db["section_id"]))

            # Custom expansions / edge-case mappings (extend as needed)
            custom_expansions = {
                "tv": "television-and-radio",
                "tv-and-radio": "television-and-radio",
                "television": "television-and-radio",
                "world": "world-news",
                "us": "us-news",
                "uk": "uk-news",
            }
            # If DB contains the target, map the source to the target id
            for src, tgt in list(custom_expansions.items()):
                tgt_norm = normalize_section_key(tgt)
                if tgt_norm in map_norm_to_id:
                    map_norm_to_id[normalize_section_key(src)] = map_norm_to_id[tgt_norm]

            def resolve_section_id_from_prefix(prefix: Optional[str], map_dict: dict) -> Optional[int]:
                """Resolve article prefix → section_id using normalized lookup"""
                if not isinstance(prefix, str) or not prefix:
                    return None
                p_norm = normalize_section_key(prefix)
                if not p_norm:
                    return None
                # direct
                if p_norm in map_dict:
                    return map_dict[p_norm]
                # try variants
                if (p_norm + "-news") in map_dict:
                    return map_dict[p_norm + "-news"]
                if p_norm.endswith("-news") and p_norm[:-5] in map_dict:
                    return map_dict[p_norm[:-5]]
                return None

            # Apply with explicit argument
            new_df["article_prefix"] = new_df["article_id"].apply(
                lambda x: str(x).split("/")[0] if pd.notna(x) else None)
            new_df["section_id"] = new_df["article_prefix"].apply(
                lambda x: resolve_section_id_from_prefix(x, map_norm_to_id))

            # Log unmapped prefixes for debugging
            unmapped = new_df.loc[new_df["section_id"].isnull(), "article_prefix"].dropna().unique().tolist()
            if unmapped:
                logger.warning("⚠️ Unmapped article prefixes: %s", unmapped)

            # Merge with publications to get publication_id
            new_df = new_df.merge(
                pubs_db,
                how="left",
                left_on="fields_publication",
                right_on="publication_name"
            )

            # Rename columns to target schema
            rename_map = {
                "webPublicationDate": "publication_date",
                "webTitle": "title",
                "fields_headline": "headline",
                "fields_trailText": "trail_text",
                "fields_body": "body",
                "fields_wordcount": "wordcount",
                "fields_thumbnail": "thumbnail_url",
                "webUrl": "web_url",
            }
            new_df = new_df.rename(columns=rename_map)

            # Keep only columns for the articles table
            keep_cols = [
                "article_id", "type", "section_id", "publication_date", "title",
                "headline", "trail_text", "body", "wordcount", "publication_id",
                "thumbnail_url", "web_url", "has_thumbnail", "is_live_blog",
                "topic_country", "ingested_at", "source_system", "raw_s3_key", "processed_by"
            ]
            articles_to_write = new_df[[c for c in keep_cols if c in new_df.columns]].copy()

            # Type conversions
            if "publication_date" in articles_to_write:
                articles_to_write["publication_date"] = pd.to_datetime(
                    articles_to_write["publication_date"], errors="coerce"
                )

            for col in ["has_thumbnail", "is_live_blog"]:
                if col in articles_to_write.columns:
                    # safe coercion to bool
                    articles_to_write[col] = articles_to_write[col].astype(bool)

            # --------------------
            # Insert ARTICLES
            # --------------------
            articles_to_write.to_sql("articles", conn, schema=schema, if_exists="append", index=False)
            logger.info("✅ Inserted %s new articles.", len(articles_to_write))

            # --------------------
            # ARTICLE ↔ AUTHORS
            # --------------------
            insert_author_sql = text(f"""
                        INSERT INTO {schema}.article_authors (article_id, author_id, ord)
                        VALUES (:article_id, :author_id, :ord)
                        ON CONFLICT DO NOTHING;
                    """)
            for _, row in new_df.iterrows():
                a_id = row["article_id"]
                byline = row.get("fields_byline")
                if not byline:
                    continue
                parts = [p.strip() for p in str(byline).split(",") if p.strip()]
                for i, name in enumerate(parts):
                    aid = author_map.get(name)
                    if aid:
                        conn.execute(insert_author_sql, {"article_id": a_id, "author_id": int(aid), "ord": i})

            # --------------------
            # ARTICLE ↔ KEYWORDS
            # --------------------
            insert_kw_sql = text(f"""
                        INSERT INTO {schema}.article_keywords (article_id, keyword)
                        VALUES (:article_id, :keyword)
                        ON CONFLICT DO NOTHING;
                    """)

            for _, row in new_df.iterrows():
                a_id = row["article_id"]
                kw = row.get("slug_keywords")

                # Skip if None/NaN
                if kw is None or (isinstance(kw, float) and pd.isna(kw)):
                    continue

                # Normalize to list
                keywords = []
                if isinstance(kw, str):
                    try:
                        parsed = json.loads(kw)
                        if isinstance(parsed, list):
                            keywords = parsed
                        elif isinstance(parsed, dict):
                            keywords = list(parsed.values())
                        else:
                            keywords = re.findall(r"[A-Za-z]+", kw)
                    except Exception:
                        keywords = re.findall(r"[A-Za-z]+", kw)
                elif isinstance(kw, (list, tuple, set)):
                    keywords = list(kw)
                elif hasattr(kw, "__iter__"):
                    keywords = list(kw)
                else:
                    continue

                for k in keywords:
                    k = str(k).strip().lower()
                    if not k:
                        continue
                    conn.execute(insert_kw_sql, {"article_id": a_id, "keyword": k})

            trans.commit()
            logger.info("🎉 ETL complete and committed successfully.")
        except Exception:
            trans.rollback()
            logger.exception("❌ Error during ETL")
            raise

    return len(articles_to_write)
