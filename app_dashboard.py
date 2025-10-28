import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv()

# --- Postgres connection ---
user = os.getenv("POSTGRES_USER", "airflow")
password = os.getenv("POSTGRES_PASSWORD", "airflow")
db = os.getenv("POSTGRES_DB", "guardian_dw")

# ✅ Nếu chạy Streamlit ngoài Docker
host = "localhost"
port = "5433"

engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")

st.set_page_config(page_title="📊 The Guardian Data Dashboard", layout="wide")
st.title("📊 The Guardian Analytics Dashboard")

# --- Sidebar filters ---
st.sidebar.header("🔍 Filters")
section_filter = st.sidebar.text_input("Filter by section (optional)")
author_filter = st.sidebar.text_input("Filter by author (optional)")
date_range = st.sidebar.date_input("Select date range", [])

# --- Load data from marts/core ---
@st.cache_data
def load_data():
    query = """
    SELECT 
        article_id,
        title,
        authors,
        section,
        published_at::date AS date,
        word_count,
        topic_country
    FROM marts.core_articles
    """
    return pd.read_sql(query, engine)

df = load_data()

# --- Apply filters ---
if section_filter:
    df = df[df["section"].str.contains(section_filter, case=False, na=False)]
if author_filter:
    df = df[df["authors"].str.contains(author_filter, case=False, na=False)]
if len(date_range) == 2:
    df = df[(df["date"] >= pd.Timestamp(date_range[0])) & (df["date"] <= pd.Timestamp(date_range[1]))]

st.write(f"### Showing {len(df)} articles")

# --- Visualization 1: Article count by section ---
st.subheader("🗂️ Articles by Section")
by_section = df.groupby("section")["article_id"].count().reset_index(name="article_count")
st.bar_chart(by_section.set_index("section"))

# --- Visualization 2: Articles over time ---
st.subheader("📆 Articles Published Over Time")
by_date = df.groupby("date")["article_id"].count().reset_index(name="articles")
st.line_chart(by_date.set_index("date"))

# --- Visualization 3: Top authors ---
st.subheader("✍️ Top Authors")
by_author = df["authors"].value_counts().head(10)
st.bar_chart(by_author)

# --- Raw data preview ---
with st.expander("🧾 Show raw data"):
    st.dataframe(df)