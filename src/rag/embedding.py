import os
from pathlib import Path
import pandas as pd
import numpy as np
from tqdm import tqdm
from sentence_transformers import SentenceTransformer
import faiss
import pickle
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# --- Config ---
DEFAULT_INPUT_PATH = "data/chunked_guardian_articles.parquet"
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2")
FAISS_INDEX_PATH = os.getenv("FAISS_INDEX_PATH", "data/faiss_guardian_index.faiss")
METADATA_PATH = os.getenv("METADATA_PATH", "data/faiss_guardian_metadata.pkl")
BATCH_SIZE = int(os.getenv("EMBED_BATCH_SIZE", 64))
USE_NORMALIZE = True  # chuẩn hóa embedding
DIMENSION = None  # sẽ lấy từ model output

# --- 1. Load chunked data ---
def load_chunked_data(input_path=DEFAULT_INPUT_PATH):
    logging.info(f"Loading chunked data from {input_path}")
    df = pd.read_parquet(input_path)
    logging.info(f"Loaded {len(df)} chunks.")
    return df

# --- 2. Load embedding model ---
def load_embedding_model(model_name=EMBEDDING_MODEL):
    logging.info(f"Loading embedding model: {model_name}")
    model = SentenceTransformer(model_name)
    return model

# --- 3. Compute embeddings in batches ---
def compute_embeddings(model, texts, batch_size=BATCH_SIZE):
    embeddings = []
    for i in tqdm(range(0, len(texts), batch_size), desc="Embedding batches"):
        batch = texts[i : i + batch_size]
        emb = model.encode(batch, show_progress_bar=False)
        if USE_NORMALIZE:
            # chuẩn hóa mỗi vector
            emb = emb / np.linalg.norm(emb, axis=1, keepdims=True)
        embeddings.append(emb)
    embeddings = np.vstack(embeddings)
    return embeddings

# --- 4. Build FAISS index ---
def build_faiss_index(embeddings, dimension, index_path=FAISS_INDEX_PATH):
    logging.info(f"Building FAISS index (dimension={dimension})")
    # sử dụng inner-product hoặc cosine (nếu đã chuẩn hóa)
    index = faiss.IndexFlatIP(dimension)
    index.add(embeddings)
    logging.info(f"FAISS index: total vectors = {index.ntotal}")
    # lưu index ngay
    faiss.write_index(index, index_path)
    logging.info(f"Saved FAISS index to {index_path}")
    return index

# --- 5. Save metadata mapping ---
def save_metadata(metadata_df, metadata_path=METADATA_PATH):
    metadata_path = Path(metadata_path)
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    with open(metadata_path, "wb") as f:
        pickle.dump(metadata_df, f)
    logging.info(f"Saved metadata mapping to {metadata_path}")

# --- 6. Main pipeline ---
def run_embedding_pipeline():
    df = load_chunked_data()
    model = load_embedding_model()

    texts = df["chunk_text"].tolist()
    embeddings = compute_embeddings(model, texts, batch_size=BATCH_SIZE)

    dimension = embeddings.shape[1]
    # Build index
    index = build_faiss_index(embeddings, dimension)

    # Save metadata
    metadata_cols = ["chunk_id", "article_id", "title", "authors", "section",
                     "keywords", "publication", "pillar", "topic_country", "published_at"]
    metadata_df = df[metadata_cols].copy()
    save_metadata(metadata_df)

    logging.info("Embedding pipeline completed.")

if __name__ == "__main__":
    logging.info("🚀 Starting embedding pipeline …")
    run_embedding_pipeline()
    logging.info("✅ Done.")