from src.rag.update_index import append_to_index
from src.rag.update_index import main as update_index_main

def update_vector_index(**context):
    """Append new chunks into FAISS vector index."""
    input_path = context["ti"].xcom_pull(key="chunked_data_path")
    if not input_path:
        print("⚠️ No new chunked data — skipping index update.")
        return

    print(f"🚀 Appending new chunks to FAISS index from {input_path}")
    update_index_main(input_path)
    print("✅ FAISS index updated successfully.")