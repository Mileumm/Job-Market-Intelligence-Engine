import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import os
import urllib.parse
import json
import time
import requests
from requests.auth import HTTPBasicAuth

# 1. Setup the Page
st.set_page_config(page_title="Job Market Tracker", page_icon="💼", layout="wide")

# 2. Connect to the Database
@st.cache_resource
def get_db_engine():
    raw_user = os.getenv("USER_DB", "airflow")
    raw_password = os.getenv("PW_DB", "airflow")
    db_name = os.getenv("TABLE_DB", "airflow")
    
    user = urllib.parse.quote_plus(raw_user)
    password = urllib.parse.quote_plus(raw_password)
    
    host = os.getenv("DB_HOST", "postgres")
    port = os.getenv("DB_PORT", "5432")
    
    return create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db_name}")

engine = get_db_engine()

from sqlalchemy import inspect
inspector = inspect(engine)

# If the pipeline hasn't run at all yet
if not inspector.has_table("silver_companies") or not inspector.has_table("raw_jobs"):
    st.title("💼 Job Market Tracker")
    st.info(" Welcome! The database is currently empty or preparing. Please wait for the Airflow pipeline to finish its first full run.")
    st.stop()

# 3. Fetch Data
@st.cache_data(ttl=600) # Cache for 10 minutes to make it blazing fast
def load_data():
    query = """
        SELECT 
            c.company_name, c.industry, c.company_rating, c.review_count, c.detailed_description,
            c.website, c.company_size, c.tech_team_size, c.public_sentiment,
            j.title as job_title, j.location as job_location, j.link as job_link, 
            j.description as job_desc, j.workplace_type, j.tech_stack, j.salary 
        FROM silver_companies c
        LEFT JOIN raw_jobs j ON c.company_name = j.company AND j.is_active = TRUE
    """
    return pd.read_sql(query, engine)

df = load_data()

if df.empty:
    st.title("💼 Job Market Tracker")
    st.warning("No data found in the Silver layer. Ensure the enrichment Airflow DAG has completed.")
    st.stop()

# 4. Sidebar Controls
st.sidebar.title("🔍 Filters & Navigation")

st.sidebar.markdown("### Job Filters")
skill_filter = st.sidebar.text_input("Required Skill (e.g. Python, SQL)")

# Dynamically gather unique locations from the database
unique_locations = ["All"] + sorted([str(loc) for loc in df['job_location'].unique() if pd.notna(loc)])
place_filter = st.sidebar.selectbox("Location", unique_locations)
job_category_filter = st.sidebar.selectbox("Job Category", ["All", "Data Engineering", "Data Science / ML", "Data Analysis", "Software Engineering"])
experience_filter = st.sidebar.selectbox("Experience Level", ["All", "Junior / Entry-Level", "Mid-Level", "Senior / Lead"])
workplace_filter = st.sidebar.selectbox("Workplace Type", ["All", "Remote", "Hybrid", "On-site"])
salary_filter = st.sidebar.checkbox("Must have Disclosed Salary", value=False)

# ---- FILTER GLOBAL DATAFRAME FIRST ----
global_jobs_df = df.dropna(subset=['job_title']).copy()

if skill_filter:
    skill_lower = skill_filter.lower()
    global_jobs_df = global_jobs_df[
        global_jobs_df['tech_stack'].str.lower().str.contains(skill_lower, na=False) | 
        global_jobs_df['job_desc'].str.lower().str.contains(skill_lower, na=False)
    ]
    
if place_filter != "All":
    global_jobs_df = global_jobs_df[global_jobs_df['job_location'] == place_filter]
    
if job_category_filter != "All":
    if job_category_filter == "Data Engineering":
        keywords = "data engineer|ingénieur de données|data architect|architecte de données"
    elif job_category_filter == "Data Science / ML":
        keywords = "data scientist|machine learning|ml|ai engineer|intelligence artificielle"
    elif job_category_filter == "Data Analysis":
        keywords = "data analyst|analyste de données|business analyst"
    elif job_category_filter == "Software Engineering":
        keywords = "software|developer|développeur|backend|frontend|fullstack"
    else:
        keywords = ""
        
    if keywords:
        global_jobs_df = global_jobs_df[global_jobs_df['job_title'].str.lower().str.contains(keywords, na=False)]
        
if experience_filter != "All":
    if experience_filter == "Junior / Entry-Level":
        exp_keywords = "junior|intern|stage|entry level|student|étudiant|apprenti"
    elif experience_filter == "Mid-Level":
        exp_keywords = "mid level|intermédiaire|intermediate|confirmé"
    elif experience_filter == "Senior / Lead":
        exp_keywords = "senior|manager|lead|architect|directeur|principal"
    else:
        exp_keywords = ""
        
    if exp_keywords:
        global_jobs_df = global_jobs_df[
            global_jobs_df['job_title'].str.lower().str.contains(exp_keywords, na=False) |
            global_jobs_df['job_desc'].str.lower().str.contains(exp_keywords, na=False)
        ]
        
if workplace_filter != "All":
    global_jobs_df = global_jobs_df[global_jobs_df['workplace_type'] == workplace_filter]
    
if salary_filter:
    global_jobs_df = global_jobs_df[~global_jobs_df['salary'].isin(["Not disclosed", None]) & global_jobs_df['salary'].notna()]

# NOW get unique companies from the filtered jobs
companies = sorted([c for c in global_jobs_df['company_name'].unique() if pd.notna(c)])

st.sidebar.markdown("### 🏢 Navigation")
if not companies:
    st.sidebar.warning("No companies match these filters.")
    selected_company = None
else:
    selected_company = st.sidebar.selectbox("Select a Company Profile", companies)

st.sidebar.markdown("---")
st.sidebar.markdown("### ⚙️ Job Scraping Configuration")
search_profession = st.sidebar.text_input("Profession / Keyword", "Data Engineer")
search_location = st.sidebar.text_input("Location", "Paris, France")

job_level = st.sidebar.selectbox(
    "Experience Level Target", 
    ["All Levels", "Junior / Entry-Level", "Mid-Level", "Senior / Lead"]
)

if st.sidebar.button("🚀 Trigger Airflow Pipeline"):
    # 1. Translate levels into broad API query keywords
    levels_map = {
        "All Levels": [""],
        "Junior / Entry-Level": ["junior", "intern", "internship", "stage", "entry level", "student", "étudiant"],
        "Mid-Level": ["mid level", "intermédiaire", "intermediate", "confirmé"],
        "Senior / Lead": ["senior", "manager", "lead", "architect", "directeur", "principal"]
    }
    
    level_keywords = levels_map[job_level]
    if level_keywords == [""]:
        level_queries = [search_profession.strip()]
    else:
        level_queries = [f"{search_profession.strip()} {kw}" for kw in level_keywords]
        
    conf_payload = {
        "profession": search_profession.strip(),
        "location": search_location.strip(),
        "level_queries": level_queries
    }
    
    # 2. Define internal API helper functions
    def get_airflow_token():
        try:
            url = "http://airflow-apiserver:8080/auth/token"
            payload = {"username": "airflow", "password": "airflow"}
            resp = requests.post(url, json=payload, timeout=5)
            if resp.status_code in [200, 201]:
                return resp.json().get("access_token")
            return f"Error: Status HTTP {resp.status_code} ({resp.text})"
        except Exception as e:
            return f"Exception: {str(e)}"

    def trigger_dag(dag_id, conf=None):
        token = get_airflow_token()
        if token and "Error" in token: return token
        if token and "Exception" in token: return token
        if not token: return "Error: Token generation returned None"
        
        url = f"http://airflow-apiserver:8080/api/v2/dags/{dag_id}/dagRuns"
        headers = {"Authorization": f"Bearer {token}"}
        
        from datetime import datetime, timezone
        payload = {
            "logical_date": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "conf": conf if conf else {}
        }
        try:
            resp = requests.post(url, json=payload, headers=headers, timeout=5)
            if resp.status_code in [200, 201]:
                return resp.json().get("dag_run_id")
            return f"Error HTTP {resp.status_code} on Trigger: {resp.text}"
        except Exception as e:
            return f"Trigger Exception: {str(e)}"
            
    def check_dag_status(dag_id, run_id):
        token = get_airflow_token()
        if not token or "Error" in token or "Exception" in token:
            return f"Token Error in polling: {token}"
            
        import urllib.parse
        safe_run_id = urllib.parse.quote(run_id)
        url = f"http://airflow-apiserver:8080/api/v2/dags/{dag_id}/dagRuns/{safe_run_id}"
        headers = {"Authorization": f"Bearer {token}"}
        try:
            resp = requests.get(url, headers=headers, timeout=10)
            if resp.status_code == 200:
                return resp.json().get("state")
            return f"HTTP {resp.status_code}: {resp.text}"
        except Exception as e:
            return f"Exception: {str(e)}"
            
    # 3. Trigger Bronze Layer DAG
    run_result_1 = trigger_dag("linkedin_bronze_to_silver", conf_payload)
    
    # If run_result_1 is a valid ID (not starting with 'Error' or 'Exception')
    if run_result_1 and not str(run_result_1).startswith(("Error", "Exception", "Trigger")):
        st.sidebar.info("🚀 Pipeline Started!")
        progress_text = st.sidebar.empty()
        
        state1 = "running"
        with st.spinner('Scraping jobs... This might take ~5-10 minutes due to LinkedIn limits.'):
            while state1 in ["queued", "running"]:
                time.sleep(5)
                state1 = check_dag_status("linkedin_bronze_to_silver", run_result_1)
            
        if state1 == "success":
            progress_text.success("✅ Scraping Complete! Triggering AI...")
            run_result_2 = trigger_dag("enrich_companies")
            if run_result_2 and not str(run_result_2).startswith(("Error", "Exception", "Trigger")):
                state2 = "running"
                with st.spinner('🧠 AI Enrichment running (Gemini API)...'):
                    while state2 in ["queued", "running"]:
                        time.sleep(5)
                        state2 = check_dag_status("enrich_companies", run_result_2)
                    
                if state2 == "success":
                    progress_text.success("🎉 Pipeline Complete! Refreshing UI...")
                    time.sleep(2)
                    st.rerun()
                else:
                    st.sidebar.error(f"Enrichment DAG failed: {state2}")
            else:
                st.sidebar.error(f"Failed to trigger Enrichment DAG. {run_result_2}")
        else:
            st.sidebar.error(f"Scraping stopped with state: {state1}")
    else:
        st.sidebar.error(f"Failed to trigger Airflow pipeline: {run_result_1}")

# 5. Display the Company Profile & Jobs
if selected_company:
    company_df = df[df['company_name'] == selected_company]
    
    # We grab the first row for company-level metadata
    comp_meta = company_df.iloc[0]
    
    st.title(f"🏢 {comp_meta['company_name']}")
    
    # Badges
    col1, col2, col3, col4 = st.columns(4)
    if pd.notna(comp_meta['website']) and comp_meta['website'] != "Unknown":
        col1.markdown(f"[🌐 Visit Website]({comp_meta['website']})")
    col2.info(f"🏷️ {comp_meta['industry']}")
    col3.info(f"👥 {comp_meta['company_size']} Employees")
    
    rating = comp_meta['company_rating']
    if pd.notna(rating) and str(rating).lower() not in ["n/a", "unknown", "nan", "null"]:
        col4.warning(f"⭐ {rating}/5.0 ({comp_meta['review_count']} reviews)")
    else:
        col4.secondary("No rating data")
        
    st.markdown("### 📖 Company Overview")
    st.write(comp_meta['detailed_description'])
    
    st.success(f"**💡 AI Sentiment Analysis:** {comp_meta['public_sentiment']}")
    
    st.markdown("---")
    st.markdown(f"### 🎯 Open Positions")
    
    # Render jobs from the globally filtered dataset for this company
    jobs_df = global_jobs_df[global_jobs_df['company_name'] == selected_company]
    
    if jobs_df.empty:
        st.error("No open positions match your current filters for this company.")
    else:
        for _, job in jobs_df.iterrows():
            with st.container():
                st.markdown(f"#### [{job['job_title']}]({job['job_link']})")
                
                meta_cols = st.columns([1, 1, 2])
                meta_cols[0].markdown(f"**📍 Location:** {job['job_location']}")
                meta_cols[1].markdown(f"**🏢 Type:** {job['workplace_type']}")
                if pd.notna(job['salary']) and job['salary'] != "Not disclosed":
                    meta_cols[2].markdown(f"**💰 Salary:** <span style='color:green;font-weight:bold;'>{job['salary']}</span>", unsafe_allow_html=True)
                
                # Tech tags
                if pd.notna(job['tech_stack']):
                    try:
                        tools = json.loads(job['tech_stack'])
                        if tools:
                            st.markdown("**🛠️ Tech Stack:** " + " ".join([f"`{t}`" for t in tools]))
                    except:
                        pass
                
                with st.expander("View Job Description"):
                    st.write(job['job_desc'])
                st.markdown("<br>", unsafe_allow_html=True)
else:
    st.title("💼 Job Market Tracker")
    st.warning("Adjust your Job Filters on the left to discover active companies matching your criteria.")