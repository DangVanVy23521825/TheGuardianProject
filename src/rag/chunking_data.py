import os
from pathlib import Path
import pandas as pd
from tqdm import tqdm
import re
import nltk
from nltk.tokenize import sent_tokenize

try:
    nltk.data.find("tokenizers/punkt")
except LookupError:
    nltk.download("punkt", quiet=True)

# --- Config ---
DEFAULT_INPUT_PATH = "data/clean_guardian_articles.parquet"
DEFAULT_OUTPUT_PATH = "data/chunked_guardian_articles.parquet"

# --- Text chunking utility ---
def clean_text(text: str) -> str:
    text = re.sub(r"\s+", " ", str(text))
    text = re.sub(r"[^\x00-\x7F]+", " ", text)
    return text.strip()

def chunk_text_semantic(text, chunk_size=400, overlap=50):
    if not isinstance(text, str) or not text.strip():
        return []

    sentences = sent_tokenize(text)
    chunks, current_chunk = [], []
    current_len = 0

    for sent in sentences:
        sent = clean_text(sent)
        sent_len = len(sent.split())

        # Nếu thêm câu này vượt quá chunk_size, lưu chunk hiện tại
        if current_len + sent_len > chunk_size:
            chunks.append(" ".join(current_chunk))

            # Tính overlap theo số từ
            overlap_words = " ".join(" ".join(current_chunk).split()[-overlap:])
            current_chunk = [overlap_words] if overlap_words else []
            current_len = len(overlap_words.split())

        current_chunk.append(sent)
        current_len += sent_len

    if current_chunk:
        chunks.append(" ".join(current_chunk))

    return [clean_text(c) for c in chunks if len(c.split()) > 5]  # bỏ các đoạn quá ngắn


def get_dynamic_chunk_size(content_len):
    if content_len < 300:
        return 300
    elif content_len < 1500:
        return 400
    else:
        return 800


def chunk_dataframe(df: pd.DataFrame, chunk_size: int = 400, overlap: int = 50):
    rows = []

    for _, r in tqdm(df.iterrows(), total=len(df), desc="🔪 Chunking articles"):
        article_id = r["article_id"]
        dynamic_size = get_dynamic_chunk_size(len(r["content"].split()))
        chunks = chunk_text_semantic(r["content"], chunk_size=dynamic_size, overlap=overlap)

        for i, chunk in enumerate(chunks):
            rows.append({
                "article_id": article_id,
                "chunk_id": f"{article_id}__{i}",
                "chunk_ord": i,
                "chunk_text": chunk,
                "title": r.get("title", ""),
                "section": r.get("section", ""),
                "authors": r.get("authors", ""),
                "keywords": r.get("keywords", ""),
                "publication": r.get("publication", ""),
                "pillar": r.get("pillar", ""),
                "topic_country": r.get("topic_country", ""),
                "source": "guardian",
                "published_at": r.get("published_at", None),
                "url": r.get("url", "")
            })

    chunk_df = pd.DataFrame(rows)
    print(f"✅ Created {len(chunk_df)} chunks from {len(df)} articles.")
    return chunk_df


def save_chunked_data(df: pd.DataFrame, output_path: str = DEFAULT_OUTPUT_PATH):

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out, index=False)
    print(f"💾 Saved chunked data → {out.resolve()}")


if __name__ == "__main__":
    print("🚀 Chunking Guardian articles for RAG embeddings...")

    input_path = DEFAULT_INPUT_PATH
    output_path = DEFAULT_OUTPUT_PATH
    chunk_size = 400
    overlap = 50

    if not Path(input_path).exists():
        raise FileNotFoundError(f"❌ Input file not found: {input_path}")

    df = pd.read_parquet(input_path)
    chunked_df = chunk_dataframe(df, chunk_size=chunk_size, overlap=overlap)
    save_chunked_data(chunked_df, output_path=output_path)

    # Stats
    avg_chunks = len(chunked_df) / len(df)
    avg_len = chunked_df["chunk_text"].str.len().mean()
    print(f"📏 Avg chunks/article: {avg_chunks:.2f}, avg length: {avg_len:.0f} chars")

    print("✅ Done! Chunked data ready for embedding.")