from datetime import datetime, timedelta, UTC
from src.rag.prepare_data import load_new_articles, clean_text_fields, save_clean_data

def extract_and_clean(**context):
    """Load new Guardian articles since yesterday and clean."""
    since_date = (datetime.now(UTC) - timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"📅 Loading new Guardian articles since {since_date}")

    df = load_new_articles(since_date=since_date)
    if df.empty:
        print("⚠️ No new articles found.")
        context["ti"].xcom_push(key="new_data_path", value=None)
        return

    df_clean = clean_text_fields(df)
    output_path = "data/clean_new_articles.parquet"
    save_clean_data(df_clean, output_path)
    context["ti"].xcom_push(key="new_data_path", value=output_path)
    print(f"✅ Cleaned and saved {len(df_clean)} new articles → {output_path}")