from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import time
import random

# Import your custom scripts (Ensure these exist in your scripts/ folder!)
from scripts.bronze_raw import LinkedInScraper, DatabaseManager
from scripts.company_enrichment import CompanyEnricher

# --- TASK FUNCTIONS ---

def setup_db_task():
    """Initializes the database connections and logs startup."""
    print("Connecting to database to verify setup...")
    db = DatabaseManager()
    print("Database connection verified.")

def fetch_and_load_bronze_task():
    """Scrapes basic job data and saves it immediately to PostgreSQL."""
    search_queries = ["Data Engineer", "Ingénieur de données", "Data Architect"]
    scraper = LinkedInScraper()
    db = DatabaseManager()
    
    all_jobs = []
    for query in search_queries:
        print(f"--- Fetching {query} ---")
        for i in range(0, 50, 25):  
            batch = scraper.fetch_quebec_jobs(keywords=query, start=i)
            all_jobs.extend(batch)
            # Crucial anti-ban sleep between batches
            time.sleep(random.uniform(3, 7))
    
    if all_jobs:
        df = pd.DataFrame(all_jobs)
        db.fill_db(df)
        print(f"Saved {len(all_jobs)} jobs to the Bronze layer.")
    else:
        print("No jobs found in this run.")

def enrich_bronze_descriptions_task():
    """Finds jobs missing descriptions and scrapes them."""
    scraper = LinkedInScraper()
    db = DatabaseManager()
    
    print("Starting Job Description enrichment process...")
    scraper.enrich_job_descriptions(db.engine)
    print("Job Description enrichment complete.")
    
def enrich_companies_silver_task():
    """Extracts unique companies, skips already enriched ones, queries AI/APIs, and saves."""
    print("Starting Company Enrichment (Silver Layer)...")
    enricher = CompanyEnricher()
    db_manager = DatabaseManager()
    
    # 1. Fetch ALL dynamic company list from the Bronze database
    all_companies = db_manager.get_unique_companies()
    if not all_companies:
        print("No companies found in Bronze layer.")
        return

    # 2. Fetch existing Silver data to see what we already did
    existing_records = []
    already_enriched_names = []
    
    try:
        # We try to read the existing silver table
        existing_df = pd.read_sql("SELECT * FROM silver_companies", db_manager.engine)
        
        # Filter for companies that actually have a valid description (not 'Unknown' or None)
        valid_existing = existing_df[
            (existing_df['detailed_description'].notna()) & 
            (existing_df['detailed_description'] != 'Unknown')
        ]
        
        already_enriched_names = valid_existing['company_name'].tolist()
        # Convert existing data back to a list of dictionaries so we can merge it later
        existing_records = existing_df.to_dict('records')
        
        print(f"Found {len(already_enriched_names)} companies already enriched in Silver layer.")
    except Exception:
        print("Silver table does not exist yet or is empty. Proceeding with all companies.")

    # 3. Filter out the ones we already did
    companies_to_process = [c for c in all_companies if c not in already_enriched_names]
    
    print(f"Companies left to process: {len(companies_to_process)}")
    
    # 4. Loop through ONLY the new companies
    new_enriched_results = []
    for company in companies_to_process:
        print(f"Processing NEW company: {company}")
        
        # Get LLM Data
        domain = enricher.get_company_url(company)
        clean_text = enricher.get_clean_text_from_url(domain) if domain else None
        llm_data = enricher.extract_company_info(clean_text) if clean_text else {}
        
        # Get API Data
        api_data = enricher.get_google_reviews(company)
        
        # Merge safely
        final_company_data = {
            "company_name": company,
            "industry": llm_data.get("industry", "Unknown"),
            "detailed_description": llm_data.get("detailed_description", "Unknown"),
            "company_size": llm_data.get("company_size", "Unknown"),
            "tech_team_size": llm_data.get("tech_team_size", "Unknown"),
            "public_sentiment": llm_data.get("public_sentiment", "Unknown"),
            "company_rating": api_data.get("rating", "Unknown"),
            "review_count": api_data.get("review_count", 0)
        }
        new_enriched_results.append(final_company_data)
        
        # Anti-ban sleep
        time.sleep(3)
        
    # 5. Combine the old data with the new data and Save
    if new_enriched_results:
        # Keep all old records EXCEPT the ones we just re-processed (to avoid duplicates)
        new_names = [r['company_name'] for r in new_enriched_results]
        old_records_to_keep = [r for r in existing_records if r['company_name'] not in new_names]
        
        final_combined_data = old_records_to_keep + new_enriched_results
        
        db_manager.save_silver_companies(final_combined_data)
    else:
        print("✅ No new companies needed enrichment. Silver layer is up to date!")

def generate_dashboard_task():
    """Generates an interactive Single-Page Application (SPA) dashboard."""
    print("Generating interactive profile dashboard...")
    from scripts.bronze_raw import DatabaseManager
    import pandas as pd
    import json
    
    db = DatabaseManager()
    
    # 1. The Gold Query: Join Silver Companies with Bronze Jobs
    query = """
        SELECT 
            c.company_name, c.industry, c.company_rating, c.review_count, c.detailed_description,
            j.title as job_title, j.location as job_location, j.link as job_link, j.description as job_desc
        FROM silver_companies c
        LEFT JOIN raw_jobs j ON c.company_name = j.company
    """
    
    try:
        df = pd.read_sql(query, db.engine)
        
        # 2. Transform the flat SQL table into a nested Python Dictionary
        companies_data = {}
        for _, row in df.iterrows():
            comp_name = row['company_name']
            if pd.isna(comp_name):
                continue
                
            # If we haven't seen this company yet, create its profile
            if comp_name not in companies_data:
                companies_data[comp_name] = {
                    "name": comp_name,
                    "industry": row['industry'] if not pd.isna(row['industry']) else "Unknown",
                    "rating": row['company_rating'] if not pd.isna(row['company_rating']) else "N/A",
                    "reviews": row['review_count'] if not pd.isna(row['review_count']) else 0,
                    "description": row['detailed_description'] if not pd.isna(row['detailed_description']) else "No description available.",
                    "jobs": []
                }
            
            # Append the job to the company's job list
            if not pd.isna(row['job_title']):
                companies_data[comp_name]["jobs"].append({
                    "title": row['job_title'],
                    "location": row['job_location'] if not pd.isna(row['job_location']) else "Remote",
                    "link": row['job_link'],
                    # Truncate job description slightly if it's too massive
                    "desc": str(row['job_desc'])[:500] + "..." if not pd.isna(row['job_desc']) else "No description available."
                })

        # 3. Convert Python Dictionary to a JSON string for JavaScript
        companies_list = list(companies_data.values())
        json_data = json.dumps(companies_list)
        
        # 4. The HTML/JS Template
        html_template = f"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <title>Company Profiles</title>
            <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;800&display=swap" rel="stylesheet">
            <style>
                body {{ font-family: 'Inter', sans-serif; background: #f4f7f6; color: #333; margin: 0; padding: 40px 20px; display: flex; flex-direction: column; align-items: center; }}
                .top-bar {{ width: 100%; max-width: 800px; display: flex; justify-content: space-between; margin-bottom: 20px; align-items: center; }}
                select {{ padding: 12px; border-radius: 8px; border: 1px solid #ddd; font-size: 16px; font-family: 'Inter', sans-serif; width: 300px; cursor: pointer; }}
                .container {{ width: 100%; max-width: 800px; background: white; padding: 40px; border-radius: 16px; box-shadow: 0 10px 30px rgba(0,0,0,0.08); }}
                .header {{ display: flex; justify-content: space-between; align-items: flex-start; border-bottom: 2px solid #f0f0f0; padding-bottom: 20px; margin-bottom: 20px; }}
                .header h1 {{ margin: 0; color: #2c3e50; font-size: 28px; }}
                .badge {{ background: #e8f4f8; color: #3498db; padding: 5px 12px; border-radius: 20px; font-size: 14px; font-weight: 600; display: inline-block; margin-top: 10px; }}
                .rating {{ font-size: 18px; font-weight: bold; color: #f39c12; background: #fff8e1; padding: 8px 15px; border-radius: 8px; }}
                h3 {{ color: #2c3e50; border-left: 4px solid #3498db; padding-left: 10px; margin-top: 30px; }}
                .desc-text {{ line-height: 1.7; color: #555; font-size: 16px; }}
                .job-card {{ background: #f8fafc; border: 1px solid #e2e8f0; padding: 20px; border-radius: 10px; margin-bottom: 15px; transition: transform 0.2s; }}
                .job-card:hover {{ transform: translateX(5px); border-color: #3498db; }}
                .job-card h4 {{ margin: 0 0 10px 0; font-size: 18px; }}
                .job-card a {{ color: #2c3e50; text-decoration: none; }}
                .job-card a:hover {{ color: #3498db; }}
                .nav-buttons {{ display: flex; justify-content: space-between; width: 100%; max-width: 800px; margin-top: 30px; }}
                button {{ padding: 12px 24px; border: none; background: #2c3e50; color: white; border-radius: 8px; font-size: 16px; cursor: pointer; font-weight: 600; transition: background 0.2s; }}
                button:hover {{ background: #3498db; }}
                button:disabled {{ background: #cbd5e1; cursor: not-allowed; }}
            </style>
        </head>
        <body>
            
            <div class="top-bar">
                <a href="index.html" style="text-decoration: none; color: #3498db; font-weight: 600;">⬅️ Back to Hub</a>
                <select id="companySelector" onchange="jumpToCompany()"></select>
            </div>

            <div class="container" id="profileCard">
                </div>

            <div class="nav-buttons">
                <button id="prevBtn" onclick="navigate(-1)">⬅️ Previous</button>
                <button id="nextBtn" onclick="navigate(1)">Next ➡️</button>
            </div>

            <script>
                // Airflow injects the JSON data directly into this variable!
                const companiesData = {json_data};
                let currentIndex = 0;
            
                function renderCompany(index) {{
                    if (companiesData.length === 0) {{
                        document.getElementById('profileCard').innerHTML = "<p>No data available. Run your pipeline!</p>";
                        return;
                    }}
                    
                    const company = companiesData[index];
                    
                    // Loop through jobs and create HTML blocks
                    let jobsHtml = company.jobs.map(job => `
                        <div class="job-card">
                            <h4><a href="${job.link}" target="_blank">${job.title}</a></h4>
                            <div style="color: #64748b; font-size: 14px; margin-bottom: 10px;">📍 ${job.location}</div>
                            <div style="font-size: 14px; color: #475569; line-height: 1.5;">${job.desc}</div>
                        </div>
                    `).join('');

                    if (company.jobs.length === 0) jobsHtml = "<p style='color: #64748b;'>No specific open positions stored in database.</p>";

                    // --- THE BUG FIX: Safely handle missing or broken ratings ---
                    let ratingDisplay = "";
                    const ratingStr = String(company.rating).toLowerCase();
                    
                    if (ratingStr === "unknown" || ratingStr === "n/a" || ratingStr === "nan" || ratingStr === "null") {{
                        // Display a grey "No Data" badge if RapidAPI failed
                        ratingDisplay = `<div class="rating" style="color: #94a3b8; background: #f1f5f9; font-size: 14px;">No rating data</div>`;
                    }} else {{
                        // Display the gold stars if the API succeeded
                        ratingDisplay = `<div class="rating">⭐ ${company.rating} <span style="font-size: 14px; color: #666; font-weight: normal;"><br>${company.reviews} reviews</span></div>`;
                    }}
                    // ------------------------------------------------------------

                    // Inject all company data into the DOM
                    document.getElementById('profileCard').innerHTML = `
                        <div class="header">
                            <div>
                                <h1>${company.name}</h1>
                                <div class="badge">${company.industry}</div>
                            </div>
                            ${ratingDisplay} </div>
                        
                        <div>
                            <h3>Company Overview (AI Analysis)</h3>
                            <p class="desc-text">${company.description}</p>
                        </div>
                        
                        <div>
                            <h3>Open Positions (${company.jobs.length})</h3>
                            ${jobsHtml}
                        </div>
                    `;

                    document.getElementById('companySelector').value = index;
                    document.getElementById('prevBtn').disabled = index === 0;
                    document.getElementById('nextBtn').disabled = index === companiesData.length - 1;
                }}

                function navigate(direction) {{
                    currentIndex += direction;
                    renderCompany(currentIndex);
                    window.scrollTo(0, 0); // Scroll to top when changing companies
                }}

                function jumpToCompany() {{
                    currentIndex = parseInt(document.getElementById('companySelector').value);
                    renderCompany(currentIndex);
                }}

                // When the page loads, populate the dropdown and show the first company
                window.onload = () => {{
                    const selector = document.getElementById('companySelector');
                    companiesData.forEach((c, i) => {{
                        let option = document.createElement('option');
                        option.value = i;
                        option.text = c.name;
                        selector.appendChild(option);
                    }});
                    renderCompany(currentIndex);
                }};
            </script>
        </body>
        </html>
        """
        
        # 5. Save the file to your dags folder
        output_path = '/opt/airflow/dags/job_market_dashboard.html'
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_template)
            
        print(f"✅ Dashboard successfully generated at: {output_path}")
        
    except Exception as e:
        print(f"Failed to generate dashboard: {e}")
# --- DAG DEFINITION ---

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='linkedin_bronze_to_silver',
    default_args=default_args,
    description='Scrape LinkedIn jobs',
    schedule=timedelta(hours=8), 
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['scraping', 'etl', 'ai'],
) as dag:

    # Define the Airflow Tasks
    t1_setup = PythonOperator(
        task_id='setup_database',
        python_callable=setup_db_task,
    )

    t2_fetch_bronze = PythonOperator(
        task_id='fetch_and_load_bronze',
        python_callable=fetch_and_load_bronze_task,
    )

    t3_enrich_descriptions = PythonOperator(
        task_id='enrich_descriptions',
        python_callable=enrich_bronze_descriptions_task,
    )
    # Define the strict ETL execution order
    t1_setup >> t2_fetch_bronze >> t3_enrich_descriptions
    
with DAG(
    dag_id='enrich_companies',
    default_args=default_args,
    description='enrich jobs with Gemini & RapidAPI',
    schedule=timedelta(hours=8), 
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['scraping', 'etl', 'ai'],
) as dag:

    t1_enrich_companies = PythonOperator(
    task_id='enrich_companies_silver',
    python_callable=enrich_companies_silver_task,
    )
    
    t2_generate_dashboard = PythonOperator(
    task_id='generate_html_dashboard',
    python_callable=generate_dashboard_task,
    )
    
    t1_enrich_companies >> t2_generate_dashboard