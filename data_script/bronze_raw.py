import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
import random
 
load_dotenv()

class LinkedInScraper:
    def __init__(self):
        # The class stores its own list of user agents
        self.user_agents = [
            # Chrome on Windows 11
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            # Firefox on Windows 10
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
            # Safari on macOS (Apple Silicon)
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3.1 Safari/605.1.15",
            # Chrome on macOS
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            # Edge on Windows 11
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0",
            # Chrome on Linux (Ubuntu)
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            # Firefox on Linux
            "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:123.0) Gecko/20100101 Firefox/123.0",
            # Chrome on Android (Mobile)
            "Mozilla/5.0 (Linux; Android 14; SM-S911B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.6261.64 Mobile Safari/537.36",
            # Safari on iPhone (iOS 17)
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_3_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3.1 Mobile/15E148 Safari/604.1",
            # Opera on Windows
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 OPR/108.0.0.0"
        ]

        # We can also store the base URL here to keep things clean
        self.base_search_url = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"

    def get_random_header(self):
        # Notice we use 'self.user_agents' to access the class's memory
        return {
            "User-Agent": random.choice(self.user_agents),
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.google.com/"
        }
        
    def determine_workplace_type(self, location_string):
        """Parses the LinkedIn location string to find the workplace type."""
        loc_lower = str(location_string).lower()
        
        if "remote" in loc_lower or "télétravail" in loc_lower:
            return "Remote"
        elif "hybrid" in loc_lower or "hybride" in loc_lower:
            return "Hybrid"
        else:
            return "On-site"

    def fetch_quebec_jobs(self, keywords="Data Engineer", location="Quebec, Canada", start=0):
        url = f"https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords={keywords}&location={location}&start={start}"
        
        # connect at url as header
        response = requests.get(url, headers=self.get_random_header())
        if response.status_code != 200:
            print(f"Error: {response.status_code}")
            return []
        # if we conect sucessfully convert the text of page understandable data
        soup = BeautifulSoup(response.text, "html.parser")
        # get all elements in differents lists
        job_cards = soup.find_all("li")
        
        jobs = []
        # for each element, we search if it match with the patern wish 
        for card in job_cards:
            try:
                scraped_location = card.find("span", class_="job-search-card__location").get_text(strip=True)
                job = {
                    "title": card.find("h3", class_="base-search-card__title").get_text(strip=True),
                    "company": card.find("h4", class_="base-search-card__subtitle").get_text(strip=True),
                    "location": scraped_location,
                    "link": card.find("a", class_="base-card__full-link")["href"],
                    "date": card.find("time")["datetime"] if card.find("time") else None,
                    "workplace_type": self.determine_workplace_type(scraped_location)
                }
                jobs.append(job)
            except Exception as e:
                continue
                
        return jobs

    def descriptions_parser(self, conn, link, response):
        soup = BeautifulSoup(response.text, "html.parser")
        # Find the description div - LinkedIn Guest API uses this class
        desc_div = soup.find("div", class_="description__text")
        
        if desc_div:
            # Clean the text: replace newlines with spaces and strip whitespace
            description = desc_div.get_text(separator=" ", strip=True)
            
            # UPDATE the database
            # We use ':desc' and ':link' as placeholders to prevent SQL injection
            update_stmt = text("UPDATE raw_jobs SET description = :desc WHERE link = :link")
            conn.execute(update_stmt, {"desc": description, "link": link})
        else:
            print("Description tag not found, marking as N/A")
            conn.execute(text("UPDATE raw_jobs SET description = 'N/A' WHERE link = :link"), {"link": link})

    def enrich_job_descriptions(self, engine):
        # 1. Get the list of links that need a description
        with engine.connect() as conn:
            result = conn.execute(text("SELECT link FROM raw_jobs WHERE description IS NULL"))
            links = [row[0] for row in result]
        
        if not links:
            print("No pending descriptions to fetch.")
            return

        counter = 0
        batch_size = 10
        print(f"Starting enrichment for {len(links)} jobs...")
        with engine.connect() as conn:
            for link in links:
                try:
                    print(f"Fetching description for: {link[:50]}...")
                    response = requests.get(link, headers=self.get_random_header())
                    
                    if response.status_code == 200:
                        self.descriptions_parser(conn, link, response)

                    counter += 1

                    # 2. The Batch Check: Commit every 10 items
                    if counter % batch_size  == 0 and counter > 0:
                        conn.commit()
                    # 3. ANTI-BAN
                    wait_time = random.uniform(5, 12)
                    print(f"Sleeping for {wait_time:.2f} seconds...")
                    time.sleep(wait_time)

                except Exception as e:
                    print(f"Error processing {link}: {e}")
            conn.commit()

class DatabaseManager:
    def __init__(self):
        user = os.getenv("USER_DB")
        password = os.getenv("PW_DB")
        db_name = os.getenv("TABLE_DB")
        host = "db"
        port = "5432"
        self.engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db_name}")
    
    def save_to_database(self, df):
        try:
            df.to_sql('temp_jobs', self.engine, if_exists='replace', index=False)
            
            insert_query = """
            INSERT INTO raw_jobs (title, company, location, link, date)
            SELECT title, company, location, link, date, workplace_type FROM temp_jobs
            ON CONFLICT (link) DO NOTHING;
            """
            
            with self.engine.connect() as conn:
                conn.execute(text(insert_query))
                conn.commit()
        except Exception as e:
            print(f"Error: {e}")
    
    def save_silver_companies(self, enriched_data_list):
        """
        Takes a list of dictionaries containing LLM and API data 
        and saves it to the Silver layer table.
        """
        if not enriched_data_list:
            print("No enriched data to save.")
            return

        # 1. Convert the list of Python dictionaries into a Pandas DataFrame
        df = pd.DataFrame(enriched_data_list)

        # 2. Safety Check: Ensure all columns exist even if the API failed
        expected_columns = [
            "company_name", "industry", "detailed_description", 
            "company_size", "tech_team_size", "public_sentiment", 
            "company_rating", "review_count"
        ]
        
        for col in expected_columns:
            if col not in df.columns:
                df[col] = None  # Fill missing columns with NULL for the database

        try:
            print(f"Saving {len(df)} enriched companies to the database...")
            
            # 3. Save to PostgreSQL
            # We use a new table called 'silver_companies' to keep our raw data safe.
            # if_exists='replace' means it overwrites the Silver table with fresh data every run,
            # which is standard for an idempotent Airflow pipeline.
            df.to_sql(
                name="silver_companies", 
                con=self.engine, 
                if_exists="replace", 
                index=False
            )
            print("Silver layer successfully updated! ✅")
            
        except Exception as e:
            print(f"FATAL: Database insertion failed: {e}")
    
    def fill_db(self, df):
        empty_df = pd.DataFrame(columns=["title", "company", "location", "link", "date", "workplace_type"])
        empty_df.to_sql('raw_jobs', self.engine, if_exists='append', index=False)

        # 3. Add the Constraint (Only if it doesn't exist)
        with self.engine.connect() as conn:
            try:
                conn.execute(text("ALTER TABLE raw_jobs ADD CONSTRAINT unique_job_link UNIQUE (link);"))

                print("Unique constraint added.")
            except Exception:
                # If it already exists, Postgres throws an error, we just ignore it
                pass
            
            try:
                conn.execute(text("ALTER TABLE raw_jobs ADD COLUMN description TEXT;"))
            except Exception:
                pass # Column already exists

            conn.commit()

        self.save_to_database(df)

    def get_unique_companies(self):
        """Extracts a distinct list of companies from the raw jobs table."""
        try:
            with self.engine.connect() as conn:
                # SELECT DISTINCT ensures we only get each company exactly once
                result = conn.execute(text("SELECT DISTINCT company FROM raw_jobs WHERE company IS NOT NULL;"))
                # Convert the SQL result into a clean Python list
                companies = [row[0] for row in result]
                
                print(f"Found {len(companies)} unique companies in the database.")
                return companies
        except Exception as e:
            print(f"Error fetching companies: {e}")
            return []