# src/rag/retriever.py
import faiss
import pickle
import numpy as np
import pandas as pd
from sentence_transformers import SentenceTransformer
from typing import List, Dict, Optional
import logging
from pathlib import Path
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


BASE_DIR = Path(__file__).resolve().parents[2]

FAISS_INDEX_PATH = os.getenv(
    "FAISS_INDEX_PATH",
    str(BASE_DIR / "airflow" / "data" / "faiss_guardian_index.faiss")
)

METADATA_PATH = os.getenv(
    "METADATA_PATH",
    str(BASE_DIR / "airflow" / "data" / "faiss_guardian_metadata.pkl")
)

EMBEDDING_MODEL = "sentence-transformers/all-MiniLM-L6-v2"

class GuardianRetriever:
    def __init__(
        self,
        index_path: str = FAISS_INDEX_PATH,
        metadata_path: str = METADATA_PATH,
        model_name: str = EMBEDDING_MODEL,
        normalize: bool = True,
    ):
        logging.info("🔍 Loading FAISS index and metadata...")
        self.index = faiss.read_index(index_path)
        with open(metadata_path, "rb") as f:
            self.metadata = pickle.load(f)
        self.model = SentenceTransformer(model_name)
        self.normalize = normalize
        logging.info(f"✅ Retriever initialized with {len(self.metadata)} chunks.")

    def encode_query(self, query: str) -> np.ndarray:
        emb = self.model.encode([query])
        if self.normalize:
            emb = emb / np.linalg.norm(emb, axis=1, keepdims=True)
        return emb.astype("float32")

    def _filter_metadata(self, filters: Optional[Dict] = None) -> pd.DataFrame:
        if not filters:
            return self.metadata
        df = self.metadata.copy()
        for k, v in filters.items():
            if k in df.columns:
                df = df[df[k].str.lower() == str(v).lower()]
        return df

    # --- Maximal Marginal Relevance reranking ---
    def _mmr(self, query_emb, doc_embs, lambda_param=0.5, top_k=5):
        """MMR: đa dạng hóa kết quả, tránh trả về các chunks gần nhau."""
        sim_to_query = np.dot(doc_embs, query_emb.T).flatten()
        selected, remaining = [], list(range(len(doc_embs)))
        selected.append(np.argmax(sim_to_query))
        while len(selected) < top_k and remaining:
            remaining = [i for i in range(len(doc_embs)) if i not in selected]
            if not remaining:
                break
            mmr_scores = [
                lambda_param * sim_to_query[i]
                - (1 - lambda_param) * max(np.dot(doc_embs[i], doc_embs[j]) for j in selected)
                for i in remaining
            ]
            selected.append(remaining[np.argmax(mmr_scores)])
        return selected

    # --- Search method ---
    def search(
        self,
        query: str,
        top_k: int = 5,
        score_threshold: float = 0.3,
        filters: Optional[Dict] = None,
        use_mmr: bool = True,
    ) -> List[Dict]:
        query_emb = self.encode_query(query)
        filtered_df = self._filter_metadata(filters)
        idxs = filtered_df.index.to_list()

        if not idxs:
            logging.warning("⚠️ No matching metadata found for filters.")
            return []

        # Truy xuất vector subset
        faiss_embs = self.index.reconstruct_n(0, self.index.ntotal)
        filtered_embs = faiss_embs[idxs]

        # Tính cosine similarity
        scores = np.dot(filtered_embs, query_emb.T).flatten()
        top_idx = np.argsort(scores)[::-1][:top_k * 3]  # lấy nhiều hơn để rerank

        # Rerank bằng MMR
        if use_mmr and len(top_idx) > 1:
            rerank_idx = self._mmr(query_emb, filtered_embs[top_idx], top_k=top_k)
            top_idx = [top_idx[i] for i in rerank_idx]

        results = []
        for i in top_idx:
            score = float(scores[i])
            if score < score_threshold:
                continue
            meta = filtered_df.iloc[i].to_dict()
            meta["score"] = round(score, 4)
            results.append(meta)

        logging.info(f"🎯 Retrieved {len(results)} chunks (after filtering + reranking).")
        return results