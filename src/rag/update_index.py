import os
from pathlib import Path
import pandas as pd
import numpy as np
import faiss
import pickle
import logging
from sentence_transformers import SentenceTransformer
from tqdm import tqdm
import hashlib
import tempfile
import shutil
import argparse

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2")
FAISS_INDEX_PATH = os.getenv("FAISS_INDEX_PATH", "data/faiss_guardian_index.faiss")
METADATA_PATH = os.getenv("METADATA_PATH", "data/faiss_guardian_metadata.pkl")
BATCH_SIZE = int(os.getenv("EMBED_BATCH_SIZE", 64))
USE_NORMALIZE = True

def fingerprint(text: str):
    return hashlib.sha1(text.encode("utf-8")).hexdigest()

def load_existing():
    if Path(FAISS_INDEX_PATH).exists() and Path(METADATA_PATH).exists():
        logging.info("Loading existing FAISS index and metadata.")
        index = faiss.read_index(FAISS_INDEX_PATH)
        with open(METADATA_PATH, "rb") as f:
            meta = pickle.load(f)
        return index, meta
    logging.info("No existing index/metadata found; will create new.")
    return None, None

def create_index(dimension):
    # IndexFlatIP expects float32 vectors; we assume we use normalized vectors and inner product
    logging.info(f"Creating new FAISS IndexFlatIP (dim={dimension})")
    return faiss.IndexFlatIP(dimension)

def ensure_dtype(arr: np.ndarray):
    arr = np.asarray(arr)
    if arr.dtype != np.float32:
        arr = arr.astype("float32")
    return arr

def compute_embeddings(model, texts, batch_size=BATCH_SIZE):
    outs = []
    for i in tqdm(range(0, len(texts), batch_size), desc="Embedding"):
        batch = texts[i:i+batch_size]
        emb = model.encode(batch, show_progress_bar=False)
        if USE_NORMALIZE:
            emb = emb / np.linalg.norm(emb, axis=1, keepdims=True)
        outs.append(ensure_dtype(emb))
    if outs:
        return np.vstack(outs)
    return np.zeros((0,0), dtype=np.float32)

def atomic_write_index_and_meta(index, meta_df, index_path=FAISS_INDEX_PATH, meta_path=METADATA_PATH):
    # write to tmp then move
    tmp_dir = Path(meta_path).parent
    tmp_index = tmp_dir / (Path(index_path).name + ".tmp")
    tmp_meta = tmp_dir / (Path(meta_path).name + ".tmp")
    logging.info(f"Writing tmp files: {tmp_index}, {tmp_meta}")
    faiss.write_index(index, str(tmp_index))
    with open(tmp_meta, "wb") as f:
        pickle.dump(meta_df, f)
    # atomically replace
    shutil.move(str(tmp_index), index_path)
    shutil.move(str(tmp_meta), meta_path)
    logging.info("Atomic replace done.")

def append_to_index(new_chunks_df: pd.DataFrame):
    if new_chunks_df.empty:
        logging.info("No new chunks to add.")
        return

    model = SentenceTransformer(EMBEDDING_MODEL)
    # compute fingerprints field if not exist
    if "chunk_fingerprint" not in new_chunks_df.columns:
        new_chunks_df["chunk_fingerprint"] = new_chunks_df["chunk_text"].fillna("").apply(fingerprint)

    # load existing
    index, meta = load_existing()
    if meta is None:
        meta = pd.DataFrame(columns=[
            "chunk_id", "article_id", "title", "section", "authors", "keywords",
            "publication", "pillar", "topic_country", "published_at", "url", "chunk_fingerprint"
        ])
    # dedupe by chunk_id or fingerprint
    existing_chunk_ids = set(meta["chunk_id"].astype(str).tolist())
    existing_fps = set(meta["chunk_fingerprint"].astype(str).tolist())

    to_add = new_chunks_df[~new_chunks_df["chunk_id"].astype(str).isin(existing_chunk_ids)]
    # additionally drop if fingerprint present (protect against re-ingest with different chunk_id)
    to_add = to_add[~to_add["chunk_fingerprint"].isin(existing_fps)]

    if to_add.empty:
        logging.info("No novel chunks after dedupe. Exiting.")
        return

    texts = to_add["chunk_text"].tolist()
    embeddings = compute_embeddings(model, texts, batch_size=BATCH_SIZE)
    dim = embeddings.shape[1]

    # build index if needed
    if index is None:
        index = create_index(dim)
    else:
        # check dimension matches
        if index.d != dim:
            raise RuntimeError(f"Existing index dim {index.d} != new embeddings dim {dim}. Rebuild required.")

    # append vectors
    logging.info(f"Adding {len(embeddings)} vectors to FAISS index.")
    index.add(embeddings)

    # append metadata rows (preserving order matching embeddings)
    add_meta = to_add[[
        "chunk_id", "article_id", "title", "section", "authors", "keywords",
        "publication", "pillar", "topic_country", "published_at", "url", "chunk_fingerprint"
    ]].copy()
    # ensure columns exist
    for c in ["chunk_id","article_id","title","section","authors","keywords",
              "publication","pillar","topic_country","published_at","chunk_fingerprint"]:
        if c not in add_meta.columns:
            add_meta[c] = ""

    new_meta = pd.concat([meta, add_meta], ignore_index=True)
    # backup old files
    if Path(FAISS_INDEX_PATH).exists():
        shutil.copy(FAISS_INDEX_PATH, FAISS_INDEX_PATH + ".bak")
    if Path(METADATA_PATH).exists():
        shutil.copy(METADATA_PATH, METADATA_PATH + ".bak")

    # atomic write
    atomic_write_index_and_meta(index, new_meta)

    logging.info(f"Finished appending {len(add_meta)} chunks. New total metadata rows: {len(new_meta)}")

def main(input_parquet):
    df = pd.read_parquet(input_parquet)
    # required columns: chunk_id, chunk_text, article_id, title, section, authors, keywords, publication, pillar, topic_country, published_at
    missing = [c for c in ["chunk_id","chunk_text","article_id"] if c not in df.columns]
    if missing:
        raise ValueError(f"Input missing columns: {missing}")
    append_to_index(df)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", "-i", required=True, help="Path to chunked parquet to index")
    args = parser.parse_args()
    main(args.input)