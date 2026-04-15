import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import os
import urllib.parse

# 1. Setup the Page
st.set_page_config(page_title="Job Market Tracker", page_icon="💼", layout="wide")
st.title("💼 Job Market Tracker")

# 2. Connect to the Database
@st.cache_resource
def get_db_engine():
    # Fetch credentials mimicking the secure database_utils pattern
    raw_user = os.getenv("USER_DB", "airflow")
    raw_password = os.getenv("PW_DB", "airflow")
    db_name = os.getenv("TABLE_DB", "airflow")
    
    # Safely encode credentials
    user = urllib.parse.quote_plus(raw_user)
    password = urllib.parse.quote_plus(raw_password)
    
    # CRITICAL FIX: Base the host on Docker networks correctly, defaulting to the Airflow Postgres DB.
    host = os.getenv("DB_HOST", "postgres")
    port = os.getenv("DB_PORT", "5432")
    
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

from sqlalchemy import inspect
inspector = inspect(engine)

if not inspector.has_table("raw_jobs"):
    st.info("👋 Welcome! The database is currently empty. Please wait for the Airflow pipeline to finish its first run to populate the 'raw_jobs' table.")
else:
    try:
        # Notice we only pull jobs where is_active = TRUE!
        query = "SELECT title, company, location, workplace_type, date, link FROM raw_jobs WHERE is_active = TRUE ORDER BY date DESC"
        df = pd.read_sql(query, engine)
        
        if df.empty:
            st.info("Pipeline ran, but no active jobs were found in 'raw_jobs'.")
        else:
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
        st.error(f"Database error during extraction: {e}")