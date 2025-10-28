from src.rag.chunking_data import chunk_dataframe, save_chunked_data

def chunk_new_articles(**context):
    """Chunk cleaned articles into semantic pieces."""
    input_path = context["ti"].xcom_pull(key="new_data_path")
    if not input_path:
        print("⚠️ No new cleaned data — skipping chunking.")
        return

    import pandas as pd
    from pathlib import Path

    if not Path(input_path).exists():
        print(f"❌ Input parquet not found: {input_path}")
        return

    df = pd.read_parquet(input_path)
    if df.empty:
        print("⚠️ No data in parquet.")
        return

    chunked_df = chunk_dataframe(df)
    output_path = "data/chunked_new_articles.parquet"
    save_chunked_data(chunked_df, output_path)
    context["ti"].xcom_push(key="chunked_data_path", value=output_path)
    print(f"✅ Chunked {len(chunked_df)} new chunks saved → {output_path}")