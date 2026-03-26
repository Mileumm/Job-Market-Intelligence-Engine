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
                job = {
                    "title": card.find("h3", class_="base-search-card__title").get_text(strip=True),
                    "company": card.find("h4", class_="base-search-card__subtitle").get_text(strip=True),
                    "location": card.find("span", class_="job-search-card__location").get_text(strip=True),
                    "link": card.find("a", class_="base-card__full-link")["href"],
                    "date": card.find("time")["datetime"] if card.find("time") else None
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
            SELECT title, company, location, link, date FROM temp_jobs
            ON CONFLICT (link) DO NOTHING;
            """
            
            with self.engine.connect() as conn:
                conn.execute(text(insert_query))
                conn.commit()
        except Exception as e:
            print(f"Error: {e}")
    
    def fill_db(self, df):
        empty_df = pd.DataFrame(columns=["title", "company", "location", "link", "date"])
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

# Execution
if __name__ == "__main__":
    
    search_queries = ["Data Engineer", "Ingénieur de données", "Data Architect"]
    my_scraper = LinkedInScraper()
    all_jobs = []
    for query in search_queries:
        print(f"--- Starting search for: {query} ---")
        for i in range(0, 50, 25):  # Get first 2 pages (25 jobs per page)
            print(f"Fetching {query} starting at {i}...")
            batch = my_scraper.fetch_quebec_jobs(keywords=query, start=i)
            all_jobs.extend(batch)
            # Sleep between batches to respect rate limits
            time.sleep(random.uniform(3, 7))

    df = pd.DataFrame(all_jobs)

    # 1. Connect to the database we just started in Docker
    # Format: postgresql://user:password@localhost:port/database_name
    my_db = DatabaseManager()
    my_db.fill_db(df)
    my_scraper.enrich_job_descriptions(my_db.engine)

    print("Your database is now enriched with job details.")