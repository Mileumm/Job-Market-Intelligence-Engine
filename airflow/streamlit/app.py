import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import os

# 1. Setup the Page
st.set_page_config(page_title="Job Market Tracker", page_icon="💼", layout="wide")
st.title("💼 Job Market Tracker")

# 2. Connect to the Database
@st.cache_resource
def get_db_engine():
    user = os.getenv("USER_DB", "airflow")
    password = os.getenv("PW_DB", "airflow")
    db_name = os.getenv("TABLE_DB", "airflow")
    host = "db" # Matches your docker-compose database service name
    port = "5432"
    return create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db_name}")

engine = get_db_engine()

# 3. Build the Sidebar (Our future control panel)
st.sidebar.header("🔍 Scraper Controls")
search_keyword = st.sidebar.text_input("Job Keyword", "Data Engineer")
search_location = st.sidebar.text_input("Location", "Quebec, Canada")

if st.sidebar.button("🚀 Run Airflow Scraper"):
    st.sidebar.warning("We will wire this button to the Airflow API in the next step!")

# 4. Display the Data
st.subheader("Current Active Jobs")
try:
    # Notice we only pull jobs where is_active = TRUE!
    query = "SELECT title, company, location, workplace_type, date, link FROM raw_jobs WHERE is_active = TRUE ORDER BY date DESC"
    df = pd.read_sql(query, engine)
    
    # Streamlit natively creates a beautiful, filterable table
    st.dataframe(
        df,
        use_container_width=True,
        column_config={
            "link": st.column_config.LinkColumn("Apply Link") # Turns URLs into clickable links
        },
        hide_index=True
    )
    st.metric("Total Active Jobs", len(df))
    
except Exception as e:
    st.error(f"Waiting for pipeline to run... Database table might not exist yet. Error: {e}")