import streamlit as st
from src.rag.rag_pipeline import run_rag
import pandas as pd
import time

st.set_page_config(page_title="📰 The Guardian RAG Chatbot", page_icon="🧠", layout="wide")

# --- Sidebar ---
st.sidebar.header("⚙️ Settings")
top_k = st.sidebar.slider("Number of relevant chunks (top_k)", 3, 20, 8)
show_sources = st.sidebar.checkbox("Show article sources", True)
st.sidebar.markdown("---")
st.sidebar.write("💡 Ask about topics covered by The Guardian.")

# --- Header ---
st.title("🧠 The Guardian RAG Chatbot")
st.write("Ask me about recent articles and I’ll summarize what The Guardian reported.")

# --- Chat input ---
query = st.chat_input("Type your question here...")

if "history" not in st.session_state:
    st.session_state.history = []

# --- Handle user query ---
if query:
    with st.spinner("🔍 Searching and generating answer..."):
        start = time.time()
        response = run_rag(query, top_k=top_k)
        elapsed = time.time() - start

    answer = response["answer"]
    sources = response.get("sources", [])

    st.session_state.history.append({
        "query": query,
        "answer": answer,
        "sources": sources
    })

    st.success(f"🕒 Response time: {elapsed:.2f}s")

# --- Display chat history ---
for i, chat in enumerate(reversed(st.session_state.history)):
    with st.container(border=True):
        st.markdown(f"**🧍‍♂️ You:** {chat['query']}")
        st.markdown(f"**🤖 Bot:** {chat['answer']}")

        if show_sources and chat["sources"]:
            st.markdown("#### 📚 Sources:")
            df = pd.DataFrame(chat["sources"])
            if "url" in df.columns:
                df["url"] = df["url"].apply(lambda x: f"[link]({x})" if x else "")
            st.dataframe(df[["title", "section", "score", "url"]].fillna(""), use_container_width=True)