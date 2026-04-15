from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import time
import random
import json
from sqlalchemy import text
 
# Import your custom scripts (Ensure these exist in your scripts/ folder!)
from scripts.bronze_raw import LinkedInScraper, DatabaseManager
from scripts.company_enrichment import CompanyEnricher

# --- TASK FUNCTIONS ---

def setup_db_task():
    """Initializes the database connections and logs startup."""
    print("Connecting to database to verify setup...")
    db = DatabaseManager()
    print("Database connection verified.")

def fetch_and_load_bronze_task(**kwargs):
    """Scrapes job data, skipping known jobs, searching until it finds 50 NEW ones per query."""
    
    # Extract external parameters if triggered manually by Streamlit
    dag_run = kwargs.get('dag_run')
    conf = dag_run.conf if dag_run else {}
    
    # Defaults ensure backward compatibility for scheduled autonomous runs
    location = conf.get("location", "Quebec, Canada")
    search_queries = conf.get("level_queries", ["Data Engineer", "Ingénieur de données", "Data Architect"])
    
    scraper = LinkedInScraper()
    db = DatabaseManager()
    
    existing_links = db.get_existing_job_links()
    print(f"Loaded {len(existing_links)} existing jobs from the database.")
    
    all_new_jobs = []
    
    for query in search_queries:
        print(f"\n--- Fetching new jobs for: '{query}' in '{location}' ---")
        new_jobs_for_query = 0
        page_start = 0
        max_pages = 10 
        
        while new_jobs_for_query < 50 and page_start < (max_pages * 25):
            print(f"Scanning page {page_start//25 + 1}...")
            
            # 1. Fetch the raw page blindly, passing the dynamic location
            raw_batch = scraper.fetch_quebec_jobs(keywords=query, location=location, start=page_start)
            
            # 2. If LinkedIn literally returns 0 cards, we hit the end of the search results
            if not raw_batch:
                print("No more jobs found on LinkedIn for this query (End of results).")
                break 
                
            # 3. Filter the batch against our Memory
            new_jobs_in_batch = []
            for job in raw_batch:
                if job['link'] not in existing_links:
                    new_jobs_in_batch.append(job)
                    existing_links.add(job['link']) # Update memory instantly
            
            # 4. Add truly new jobs to our count
            all_new_jobs.extend(new_jobs_in_batch)
            new_jobs_for_query += len(new_jobs_in_batch)
            
            print(f"  -> Page {page_start//25 + 1} had {len(new_jobs_in_batch)} NEW jobs. (Total for query: {new_jobs_for_query}/50)")
            
            page_start += 25
            
            if new_jobs_for_query < 50:
                time.sleep(random.uniform(4, 8))
                
    if all_new_jobs:
        df = pd.DataFrame(all_new_jobs)
        df = df.drop_duplicates(subset=['link'], keep='last')
        db.fill_db(df)
        print(f"\n✅ Success: Saved {len(df)} completely NEW jobs to the Bronze layer.")
    else:
        print("\n✅ No new jobs found on LinkedIn today.")

def enrich_bronze_descriptions_task():
    """Finds jobs missing descriptions and scrapes them."""
    scraper = LinkedInScraper()
    db = DatabaseManager()
    
    print("Starting Job Description enrichment process...")
    scraper.enrich_job_descriptions(db.engine)
    print("Job Description enrichment complete.")
    
def enrich_companies_silver_task():
    """Extracts unique companies, checks for missing data, batches them, queries AI, and saves."""
    print("Starting Batch Company Enrichment (Silver Layer)...")
    enricher = CompanyEnricher()
    db_manager = DatabaseManager()
    
    all_companies = db_manager.get_unique_companies()
    if not all_companies:
        print("No companies found in Bronze layer.")
        return

    existing_records = []
    try:
        existing_df = pd.read_sql("SELECT * FROM silver_companies", db_manager.engine)
        existing_records = existing_df.to_dict('records')
    except Exception:
        print("Silver table does not exist yet. Proceeding with all companies.")

    existing_data_map = {r['company_name']: r for r in existing_records}
    
    # 1. GATHER ALL COMPANIES THAT NEED ENRICHMENT FIRST
    companies_to_enrich = []
    
    for company in all_companies:
        needs_enrichment = False
        existing_data = existing_data_map.get(company)
        
        if not existing_data:
            needs_enrichment = True
        else:
            missing_triggers = [None, "", "Unknown", "N/A", "nan", 0, 0.0]
            if (str(existing_data.get('company_rating')) in missing_triggers or 
                str(existing_data.get('industry')) in missing_triggers or 
                str(existing_data.get('detailed_description')) in missing_triggers):
                needs_enrichment = True
                
        if needs_enrichment:
            print(f"INFO - Gathering data for: {company}")
            # We scrape the text BEFORE calling the AI
            domain = enricher.get_company_url(company)
            clean_text = enricher.get_clean_text_from_url(domain) if domain else "No website text available."
            companies_to_enrich.append({
                "company_name": company,
                "clean_text": clean_text
            })
            time.sleep(2) # Be polite to Google/DuckDuckGo when searching domains

    # 2. BATCH PROCESS THE AI CALLS
    new_enriched_results = []
    batch_size = 15 # Process 5 companies per 1 API token!

    if companies_to_enrich:
        print(f"Found {len(companies_to_enrich)} companies to enrich. Starting AI batches...")
        
        for i in range(0, len(companies_to_enrich), batch_size):
            batch = companies_to_enrich[i:i+batch_size]
            print(f"Processing company batch {i//batch_size + 1}...")
            
            # --- We will build this new method in Step 2 ---
            batch_results = enricher.extract_batch_company_info(batch)
            
            for llm_data in batch_results:
                final_company_data = {
                    "company_name": llm_data.get("company_name", "Unknown"),
                    "industry": llm_data.get("industry", "Unknown"),
                    "website": llm_data.get("website", "Unknown"),
                    "detailed_description": llm_data.get("detailed_description", "Unknown"),
                    "company_size": llm_data.get("company_size", "Unknown"),
                    "tech_team_size": llm_data.get("tech_team_size", "Unknown"),
                    "public_sentiment": llm_data.get("public_sentiment", "Unknown"),
                    "company_rating": llm_data.get("rating", "Unknown"),
                    "review_count": llm_data.get("reviews", 0)
                }
                new_enriched_results.append(final_company_data)
                
            time.sleep(4) # Wait 15 seconds between batches to respect the 5 RPM limit
            
    # 3. SAVE TO DATABASE
    if new_enriched_results:
        print(f"Saving {len(new_enriched_results)} newly enriched companies...")
        new_names = [r['company_name'] for r in new_enriched_results]
        old_records_to_keep = [r for r in existing_records if r['company_name'] not in new_names]
        
        final_combined_data = old_records_to_keep + new_enriched_results
        db_manager.save_silver_companies(final_combined_data)
    else:
        print("✅ All companies are fully enriched. Silver layer is up to date!")
        
def generate_dashboard_task():
    """Generates an interactive Single-Page Application (SPA) dashboard."""
    print("Generating interactive profile dashboard...")
    
    db = DatabaseManager()
    
    query = """
        SELECT DISTINCT
            c.company_name, c.industry, c.company_rating, c.review_count, c.detailed_description,
            c.website, c.company_size, c.tech_team_size, c.public_sentiment,
            j.title as job_title, j.location as job_location, j.link as job_link, j.description as job_desc, j.workplace_type,
            j.tech_stack, j.salary 
        FROM silver_companies c
        LEFT JOIN raw_jobs j ON c.company_name = j.company AND j.is_active = TRUE
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
                    "website": row['website'] if 'website' in row and not pd.isna(row['website']) else "#",
                    "industry": row['industry'] if not pd.isna(row['industry']) else "Unknown",
                    "company_size": row['company_size'] if 'company_size' in row and not pd.isna(row['company_size']) else "Unknown",
                    "tech_team_size": row['tech_team_size'] if 'tech_team_size' in row and not pd.isna(row['tech_team_size']) else "Unknown",
                    "public_sentiment": row['public_sentiment'] if 'public_sentiment' in row and not pd.isna(row['public_sentiment']) else "No sentiment data.",
                    "rating": row['company_rating'] if not pd.isna(row['company_rating']) else "N/A",
                    "reviews": row['review_count'] if not pd.isna(row['review_count']) else 0,
                    "description": row['detailed_description'] if not pd.isna(row['detailed_description']) else "No description available.",
                    "jobs": [],
                }
            
            # Append the job to the company's job list
            if not pd.isna(row['job_title']):
                companies_data[comp_name]["jobs"].append({
                    "title": row['job_title'],
                    "location": row['job_location'] if not pd.isna(row['job_location']) else "Remote",
                    # FIX 2: Safely get workplace_type
                    "workplace_type": row['workplace_type'] if 'workplace_type' in row and not pd.isna(row['workplace_type']) else "Unknown",
                    "link": row['job_link'],
                    "desc": str(row['job_desc'])if not pd.isna(row['job_desc']) else "No description available.",
                    "desc": str(row['job_desc']) if not pd.isna(row['job_desc']) else "No description available.",
                    "salary": row['salary'] if 'salary' in row and not pd.isna(row['salary']) else "Not disclosed",
                    "tech_stack": row['tech_stack'] if 'tech_stack' in row and not pd.isna(row['tech_stack']) else "[]"
                })

        # 3. Convert Python Dictionary to a JSON string for JavaScript
        companies_list = list(companies_data.values())
        json_data = json.dumps(companies_list)
        
        # 4. The HTML/JS Template (UPDATED WITH NEW UI ELEMENTS)
       # 4. The HTML/JS Template (UPDATED WITH INTERACTIVE FILTERS)
        html_template = """
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8"> <title>Company Profiles</title>
            <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;800&display=swap" rel="stylesheet">
            <style>
                body { font-family: 'Inter', sans-serif; background: #f4f7f6; color: #333; margin: 0; padding: 40px 20px; display: flex; flex-direction: column; align-items: center; }
                .top-bar { width: 100%; max-width: 800px; display: flex; justify-content: space-between; margin-bottom: 20px; align-items: center; }
                select, input { padding: 12px; border-radius: 8px; border: 1px solid #ddd; font-size: 16px; font-family: 'Inter', sans-serif; outline: none;}
                
                /* NEW FILTER PANEL STYLES */
                .filter-panel { width: 100%; max-width: 800px; background: #ffffff; padding: 20px; border-radius: 12px; box-shadow: 0 4px 15px rgba(0,0,0,0.05); margin-bottom: 20px; display: flex; gap: 15px; flex-wrap: wrap; align-items: center; border-left: 5px solid #3498db;}
                .filter-group { display: flex; flex-direction: column; gap: 5px; flex-grow: 1; }
                .filter-group label { font-size: 12px; font-weight: bold; color: #64748b; text-transform: uppercase; letter-spacing: 0.5px;}
                
                .container { width: 100%; max-width: 800px; background: white; padding: 40px; border-radius: 16px; box-shadow: 0 10px 30px rgba(0,0,0,0.08); }
                .header { display: flex; justify-content: space-between; align-items: flex-start; border-bottom: 2px solid #f0f0f0; padding-bottom: 20px; margin-bottom: 20px; }
                .header h1 { margin: 0; color: #2c3e50; font-size: 28px; }
                .badge { background: #e8f4f8; color: #3498db; padding: 5px 12px; border-radius: 20px; font-size: 14px; font-weight: 600; display: inline-block; margin-top: 10px; margin-right: 5px; }
                .badge-dark { background: #1e293b; color: #f8fafc; padding: 5px 12px; border-radius: 20px; font-size: 14px; font-weight: 600; display: inline-block; margin-top: 10px; }
                .rating { font-size: 18px; font-weight: bold; color: #f39c12; background: #fff8e1; padding: 8px 15px; border-radius: 8px; text-align: right; }
                h3 { color: #2c3e50; border-left: 4px solid #3498db; padding-left: 10px; margin-top: 30px; }
                .desc-text { line-height: 1.7; color: #555; font-size: 16px; }
                .sentiment-box { background: #f0fdf4; border-left: 4px solid #22c55e; padding: 15px; border-radius: 0 8px 8px 0; margin-top: 15px; color: #166534; font-size: 15px; line-height: 1.6; }
                .job-card { background: #f8fafc; border: 1px solid #e2e8f0; padding: 20px; border-radius: 10px; margin-bottom: 15px; transition: transform 0.2s; }
                .job-card:hover { transform: translateX(5px); border-color: #3498db; }
                .job-card h4 { margin: 0 0 10px 0; font-size: 18px; }
                .job-card a { color: #2c3e50; text-decoration: none; }
                .job-card a:hover { color: #3498db; }
                .job-meta { display: inline-block; background: #e2e8f0; color: #475569; padding: 3px 8px; border-radius: 4px; font-size: 12px; font-weight: bold; margin-bottom: 10px; margin-right: 5px;}
                .nav-buttons { display: flex; justify-content: space-between; width: 100%; max-width: 800px; margin-top: 30px; }
                button.nav-btn { padding: 12px 24px; border: none; background: #2c3e50; color: white; border-radius: 8px; font-size: 16px; cursor: pointer; font-weight: 600; transition: background 0.2s; }
                button.nav-btn:hover { background: #3498db; }
                button.nav-btn:disabled { background: #cbd5e1; cursor: not-allowed; }
            </style>
        </head>
        <body>
            
            <div class="top-bar">
                <a href="index.html" style="text-decoration: none; color: #3498db; font-weight: 600;">⬅️ Back to Hub</a>
                <select id="companySelector" onchange="jumpToCompany()" style="width: 300px;"></select>
            </div>

            <div class="filter-panel">
                <div class="filter-group">
                    <label>🔍 Required Skill</label>
                    <input type="text" id="skillFilter" placeholder="e.g. Python, SQL, AWS" onkeyup="applyFilters()">
                </div>
                <div class="filter-group">
                    <label>🏢 Workplace</label>
                    <select id="workplaceFilter" onchange="applyFilters()">
                        <option value="All">All Types</option>
                        <option value="Remote">Remote</option>
                        <option value="Hybrid">Hybrid</option>
                        <option value="On-site">On-site</option>
                    </select>
                </div>
                <div class="filter-group">
                    <label>💰 Salary</label>
                    <select id="salaryFilter" onchange="applyFilters()">
                        <option value="All">All Jobs</option>
                        <option value="Disclosed">Only Disclosed Salary</option>
                    </select>
                </div>
            </div>

            <div class="container" id="profileCard">
                </div>

            <div class="nav-buttons">
                <button class="nav-btn" id="prevBtn" onclick="navigate(-1)">⬅️ Previous</button>
                <button class="nav-btn" id="nextBtn" onclick="navigate(1)">Next ➡️</button>
            </div>

            <script>
                const companiesData = __JSON_DATA__;
                let currentIndex = 0;
            
                function renderCompany(index) {
                    if (companiesData.length === 0) {
                        document.getElementById('profileCard').innerHTML = "<p>No data available. Run your pipeline!</p>";
                        return;
                    }
                    
                    const company = companiesData[index];
                    
                    // --- APPLY FILTERS LOGIC ---
                    const skillTerm = document.getElementById('skillFilter').value.toLowerCase();
                    const workplaceTerm = document.getElementById('workplaceFilter').value;
                    const salaryTerm = document.getElementById('salaryFilter').value;
                    
                    // Filter the jobs array before mapping it to HTML
                    const filteredJobs = company.jobs.filter(job => {
                        // 1. Skill Filter
                        let hasSkill = true;
                        if (skillTerm !== "") {
                            hasSkill = job.tech_stack.toLowerCase().includes(skillTerm) || job.desc.toLowerCase().includes(skillTerm);
                        }
                        
                        // 2. Workplace Filter
                        let matchesWorkplace = true;
                        if (workplaceTerm !== "All") {
                            matchesWorkplace = job.workplace_type === workplaceTerm;
                        }
                        
                        // 3. Salary Filter
                        let matchesSalary = true;
                        if (salaryTerm === "Disclosed") {
                            matchesSalary = job.salary !== "Not disclosed" && job.salary !== null;
                        }
                        
                        return hasSkill && matchesWorkplace && matchesSalary;
                    });
                    
                    // --- GENERATE JOB HTML ---
                    let jobsHtml = filteredJobs.map(job => {
                        let salaryHtml = job.salary !== "Not disclosed" 
                            ? `<div style="color: #16a34a; font-weight: 700; font-size: 14px; margin-bottom: 8px;">💰 ${job.salary}</div>` 
                            : '';
                            
                        let techTags = "";
                        try {
                            let tools = JSON.parse(job.tech_stack);
                            if (tools && tools.length > 0) {
                                techTags = `<div style="margin-top: 10px; margin-bottom: 10px;">` + 
                                    tools.map(t => `<span style="background: #1e293b; color: #f8fafc; padding: 4px 8px; border-radius: 4px; font-size: 11px; margin-right: 5px; font-weight: bold; letter-spacing: 0.5px;">${t}</span>`).join('') +
                                    `</div>`;
                            }
                        } catch(e) {}
                        
                        return `
                        <div class="job-card">
                            <h4><a href="${job.link}" target="_blank">${job.title}</a></h4>
                            ${salaryHtml}
                            <div>
                                <span class="job-meta">📍 ${job.location}</span>
                                <span class="job-meta">🏢 ${job.workplace_type}</span>
                            </div>
                            ${techTags}
                            <div style="font-size: 14px; color: #475569; line-height: 1.6; margin-top: 15px; max-height: 250px; overflow-y: auto; padding-right: 10px; white-space: pre-wrap; border-top: 1px dashed #cbd5e1; padding-top: 10px;">
                                ${job.desc}
                            </div>
                        </div>
                        `;
                    }).join('');

                    if (filteredJobs.length === 0) {
                        jobsHtml = `<p style='color: #e74c3c; font-weight: bold; background: #fdf2f0; padding: 15px; border-radius: 8px;'>No open positions match your current filters.</p>`;
                    }

                    // --- GENERATE COMPANY HEADER HTML ---
                    let ratingDisplay = "";
                    const ratingStr = String(company.rating).toLowerCase();
                    if (ratingStr === "unknown" || ratingStr === "n/a" || ratingStr === "nan" || ratingStr === "null") {
                        ratingDisplay = `<div class="rating" style="color: #94a3b8; background: #f1f5f9; font-size: 14px;">No rating data</div>`;
                    } else {
                        ratingDisplay = `<div class="rating">⭐ ${company.rating} <br><span style="font-size: 14px; color: #666; font-weight: normal;">${company.reviews} reviews</span></div>`;
                    }

                    let websiteLink = company.website !== "#" && company.website !== "Unknown" 
                        ? `<a href="${company.website}" target="_blank" style="color: #3498db; text-decoration: none; font-size: 14px; display: block; margin-top: 5px;">🔗 Visit Website</a>` 
                        : '';

                    document.getElementById('profileCard').innerHTML = `
                        <div class="header">
                            <div>
                                <h1>${company.name}</h1>
                                ${websiteLink}
                                <div class="badge">${company.industry}</div>
                                <div class="badge-dark">👥 ${company.company_size} employees</div>
                            </div>
                            ${ratingDisplay} 
                        </div>
                        
                        <div>
                            <h3>Company Overview</h3>
                            <p class="desc-text">${company.description}</p>
                            
                            <div class="sentiment-box">
                                <strong>💡 AI Sentiment Analysis:</strong><br>
                                ${company.public_sentiment}
                            </div>
                        </div>
                        
                        <div>
                            <h3>Open Positions (${filteredJobs.length})</h3>
                            ${jobsHtml}
                        </div>
                    `;

                    document.getElementById('companySelector').value = index;
                    document.getElementById('prevBtn').disabled = index === 0;
                    document.getElementById('nextBtn').disabled = index === companiesData.length - 1;
                }

                function applyFilters() {
                    // Re-render the current company to apply the new filters
                    renderCompany(currentIndex);
                }

                function navigate(direction) {
                    currentIndex += direction;
                    // Clear the skill search when jumping to a new company to avoid confusion
                    document.getElementById('skillFilter').value = "";
                    document.getElementById('workplaceFilter').value = "All";
                    document.getElementById('salaryFilter').value = "All";
                    renderCompany(currentIndex);
                    window.scrollTo(0, 0);
                }

                function jumpToCompany() {
                    currentIndex = parseInt(document.getElementById('companySelector').value);
                    // Clear the filters
                    document.getElementById('skillFilter').value = "";
                    document.getElementById('workplaceFilter').value = "All";
                    document.getElementById('salaryFilter').value = "All";
                    renderCompany(currentIndex);
                }

                window.onload = () => {
                    const selector = document.getElementById('companySelector');
                    companiesData.forEach((c, i) => {
                        let option = document.createElement('option');
                        option.value = i;
                        option.text = c.name;
                        selector.appendChild(option);
                    });
                    renderCompany(currentIndex);
                };
            </script>
        </body>
        </html>
        """
        
        # 5. Save the file
        final_html = html_template.replace("__JSON_DATA__", json_data)
        
        output_path = '/opt/airflow/dags/job_market_dashboard.html'
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(final_html)
        
        print(f"✅ Dashboard successfully generated at: {output_path}")
        
    except Exception as e:
        print(f"❌ Failed to generate dashboard: {e}")
        
def clean_database_duplicates_task():
    """Uses a SQL Window Function to deactivate old duplicates instead of deleting them."""
    print("Starting Database Cleanup (Soft Delete)...")
    db = DatabaseManager()

    deactivate_query = """
        UPDATE raw_jobs
        SET is_active = FALSE
        WHERE link IN (
            SELECT link
            FROM (
                SELECT link, 
                       ROW_NUMBER() OVER(PARTITION BY title, company, location ORDER BY date DESC, link DESC) as rn
                FROM raw_jobs
            ) subquery
            WHERE rn > 1
        );
    """
    
    try:
        with db.engine.connect() as conn:
            result = conn.execute(text(deactivate_query))
            conn.commit()
            print(f"🧹 Successfully deactivated {result.rowcount} duplicate jobs. Historical memory preserved!")
    except Exception as e:
        print(f"Error during database cleanup: {e}")
        
def enrich_individual_jobs_task():
    """Uses AI to extract Salary and Tech Stack using API Batching."""
    print("Starting Batch Job-Level AI Enrichment...")
        
    db = DatabaseManager()
    enricher = CompanyEnricher()
    
    # 1. Add the new columns to the database if they don't exist
    with db.engine.connect() as conn:
        try:
            conn.execute(text("ALTER TABLE raw_jobs ADD COLUMN tech_stack TEXT;"))
            conn.execute(text("ALTER TABLE raw_jobs ADD COLUMN salary TEXT;"))
            conn.commit()
            print("✅ Added 'tech_stack' and 'salary' columns to database.")
        except Exception:
            pass 

    # 2. Fetch un-enriched jobs
    try:
        query = "SELECT link, description FROM raw_jobs WHERE tech_stack IS NULL AND description != 'N/A' AND is_active = TRUE"
        df = pd.read_sql(query, db.engine)
    except Exception as e:
        print(f"Error reading raw_jobs: {e}")
        return

    if df.empty:
        print("✅ All jobs are already enriched with AI details.")
        return

    print(f"Found {len(df)} jobs requiring AI analysis. Processing in batches...")
    
    # Convert DataFrame to a list of dictionaries
    jobs_list = df.to_dict('records')
    batch_size = 30
    
    with db.engine.connect() as conn:
        for i in range(0, len(jobs_list), batch_size):
            batch = jobs_list[i:i+batch_size]
            print(f"Processing batch {i//batch_size + 1} (Jobs {i+1} to {min(i+batch_size, len(jobs_list))})...")
            
            # Send the batch to Gemini
            ai_results = enricher.extract_batch_job_details(batch)
            
            # Loop through the JSON array returned by Gemini
            for ai_data in ai_results:
                link = ai_data.get('link')
                tech_stack_json = json.dumps(ai_data.get('tech_stack', []))
                salary_str = ai_data.get('salary', 'Not disclosed')
                
                # Update the database for each job in the batch
                update_stmt = text("""
                    UPDATE raw_jobs 
                    SET tech_stack = :tech, salary = :sal 
                    WHERE link = :link
                """)
                conn.execute(update_stmt, {"tech": tech_stack_json, "sal": salary_str, "link": link})
            
            conn.commit()
            time.sleep(15) # Standard anti-ban sleep between batches
            
    print("Job-Level Enrichment Complete.")
    
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
    
    t4_clean_duplicates = PythonOperator(
        task_id='clean_historical_duplicates',
        python_callable=clean_database_duplicates_task,
    )
        
    # Define the strict ETL execution order
    t1_setup >> t2_fetch_bronze >> t3_enrich_descriptions >> t4_clean_duplicates
    
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
    
    t2_enrich_jobs = PythonOperator(
        task_id='enrich_individual_jobs',
        python_callable=enrich_individual_jobs_task,
    )
    
    t3_generate_dashboard = PythonOperator(
    task_id='generate_html_dashboard',
    python_callable=generate_dashboard_task,
    )
    
    t1_enrich_companies >> t2_enrich_jobs >> t3_generate_dashboard