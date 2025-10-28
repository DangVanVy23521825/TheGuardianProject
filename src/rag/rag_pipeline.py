from src.rag.retriever import GuardianRetriever
from langchain_core.documents import Document
from langchain_groq import ChatGroq
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough
import logging
import os

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def make_docs(results):
    docs = []
    for r in results:
        content = r.get("chunk_text") or ""
        metadata = {k: v for k, v in r.items() if k != "chunk_text"}
        docs.append(Document(page_content=content, metadata=metadata))
    return docs


def format_docs(docs):
    return "\n\n---\n\n".join(
        f"[Source {i+1} | {doc.metadata.get('title', 'Unknown')} | {doc.metadata.get('section', '')}]\n"
        f"{doc.page_content}"
        for i, doc in enumerate(docs)
    )


def run_rag(query: str, top_k: int = 10, filters: dict | None = None):
    """
    Thực thi RAG pipeline với LCEL (LangChain Expression Language).
    """
    logging.info(f"🔍 Query: {query}")

    # 1️⃣ Lấy documents từ retriever
    retriever = GuardianRetriever()
    results = retriever.search(query, top_k=top_k, score_threshold=0.2)
    if not results:
        logging.warning("⚠️ No documents retrieved.")
        return {"answer": "No relevant documents found.", "sources": []}

    docs = make_docs(results)

    # 2️⃣ LLM
    llm = ChatGroq(
        model="llama-3.1-8b-instant",
        groq_api_key=os.getenv("GROQ_API_KEY"),
        temperature=0
    )
    # 3️⃣ Prompt template
    template = """
You are a knowledgeable assistant summarizing sports news from The Guardian database.

Use the context below to answer concisely what happened in the user's query.
If the context doesn't contain all details, summarize the most relevant news and infer a likely answer.

Examples:
Q: What did The Guardian report about Brexit talks?
A: The Guardian reported that negotiations were tense, focusing on trade and migration.

Context:
{context}

Question: {question}

Answer:
"""

    prompt = ChatPromptTemplate.from_template(template)

    # 4️⃣ Tạo RAG chain với LCEL
    rag_chain = (
        {
            "context": lambda x: format_docs(docs),
            "question": RunnablePassthrough()
        }
        | prompt
        | llm
        | StrOutputParser()
    )

    # 5️⃣ Chạy truy vấn
    answer = rag_chain.invoke(query)

    logging.info(f"🧠 Answer: {answer}")
    return {"answer": answer, "sources": [d.metadata for d in docs]}