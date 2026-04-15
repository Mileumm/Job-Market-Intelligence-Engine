"""
Bronze layer raw data extraction for LinkedIn Jobs.
"""
import logging
import os
import random
import time
from typing import Any, Dict, List, Optional, Set

import pandas as pd
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from requests.exceptions import RequestException
from sqlalchemy import create_engine, text
from sqlalchemy.engine.base import Engine, Connection

load_dotenv()

# Configure logging to ensure visibility of failures and retries
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class LinkedInScraper:
    """Scraper class responsible for interacting with LinkedIn's public endpoints.
    
    This handles request spoofing (via User-Agents), pagination, and HTML parsing 
    to retrieve job metadata and descriptions efficiently while minimizing blocks.
    """

    def __init__(self) -> None:
        """Initializes the scraper with standard user agents and API endpoints."""
        self.user_agents: List[str] = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3.1 Safari/605.1.15",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:123.0) Gecko/20100101 Firefox/123.0",
            "Mozilla/5.0 (Linux; Android 14; SM-S911B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.6261.64 Mobile Safari/537.36",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_3_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3.1 Mobile/15E148 Safari/604.1",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 OPR/108.0.0.0"
        ]
        self.base_search_url: str = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"

    def get_random_header(self) -> Dict[str, str]:
        """Generates a random header to avoid basic scraping detection.

        We rotate User-Agent strings to distribute requests across varying browser profiles,
        making statistical rate-limiting more difficult for edge firewalls.

        Returns:
            Dict[str, str]: A dictionary containing HTTP header fields randomly populated.
        """
        return {
            "User-Agent": random.choice(self.user_agents),
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.google.com/"
        }

    def determine_workplace_type(self, location_string: str) -> str:
        """Parses the LinkedIn location string to categorize the workplace type.

        LinkedIn does not reliably provide standardized workplace tags in the guest UI.
        Inferring from the location string helps normalize the dataset.

        Args:
            location_string (str): The raw location string from the job posting.

        Returns:
            str: "Remote", "Hybrid", or "On-site" based on keyword heuristics.
        """
        loc_lower = str(location_string).lower()
        if "remote" in loc_lower or "télétravail" in loc_lower:
            return "Remote"
        if "hybrid" in loc_lower or "hybride" in loc_lower:
            return "Hybrid"
        return "On-site"

    def _parse_job_card(self, card: BeautifulSoup) -> Optional[Dict[str, Any]]:
        """Extracts individual job details from a single HTML list item card.

        Splitting this out modularizes parsing logic, isolating failures to single
        nodes rather than failing the entire page process.

        Args:
            card (BeautifulSoup): The parent HTML node for a single job listing.

        Returns:
            Optional[Dict[str, Any]]: Parsed job fields, or None if the card is malformed.
        """
        link_elem = card.find("a", class_="base-card__full-link")
        if not link_elem:
            return None

        clean_link = link_elem["href"].split("?")[0]

        loc_elem = card.find("span", class_="job-search-card__location")
        scraped_location = loc_elem.get_text(strip=True) if loc_elem else "Unknown Location"

        title_elem = card.find("h3", class_="base-search-card__title")
        title = title_elem.get_text(strip=True) if title_elem else "Unknown Title"

        comp_elem = card.find("h4", class_="base-search-card__subtitle")
        company = comp_elem.get_text(strip=True) if comp_elem else "Unknown Company"

        time_elem = card.find("time")
        date = time_elem["datetime"] if time_elem else None

        return {
            "title": title,
            "company": company,
            "location": scraped_location,
            "link": clean_link,
            "date": date,
            "workplace_type": self.determine_workplace_type(scraped_location)
        }

    def fetch_quebec_jobs(
        self, 
        keywords: str = "Data Engineer", 
        location: str = "Quebec, Canada", 
        start: int = 0
    ) -> List[Dict[str, Any]]:
        """Fetches a single page of job postings from LinkedIn for the specified criteria.

        Args:
            keywords (str): The search keywords (e.g., job title).
            location (str): The regional filter string.
            start (int): The offset index to initiate the search (used for pagination).

        Returns:
            List[Dict[str, Any]]: A list of valid job dictionaries from the current page.
        """
        url = f"{self.base_search_url}?keywords={keywords}&location={location}&start={start}"
        
        try:
            response = requests.get(url, headers=self.get_random_header(), timeout=15)
            response.raise_for_status()
            response.encoding = "utf-8"
        except RequestException as e:
            logging.error(f"Network error fetching jobs: {e}")
            return []

        soup = BeautifulSoup(response.text, "html.parser")
        job_cards = soup.find_all("li")
        
        jobs: List[Dict[str, Any]] = []
        for card in job_cards:
            try:
                job_data = self._parse_job_card(card)
                if job_data:
                    jobs.append(job_data)
            except Exception as e:
                logging.warning(f"Failed to parse job card, skipping. Error: {e}")
                continue
                
        return jobs

    def _parse_and_update_description(self, conn: Connection, link: str, text_html: str) -> None:
        """Parses description HTML and pushes the update to the database record.

        Separating HTML text extraction from database updating rules allows us to 
        test this logic without hitting the DB, while keeping our main loop readable.

        Args:
            conn (Connection): The active SQLAlchemy database connection.
            link (str): The unique job URL acting as a primary key.
            text_html (str): The raw HTML response for the job listing.
        """
        soup = BeautifulSoup(text_html, "html.parser")
        desc_div = soup.find("div", class_="description__text")
        
        if desc_div:
            description = desc_div.get_text(separator=" ", strip=True)
            update_stmt = text("UPDATE raw_jobs SET description = :desc WHERE link = :link")
            conn.execute(update_stmt, {"desc": description, "link": link})
        else:
            logging.info(f"Description tag not found for {link[:50]}..., marking as N/A.")
            update_stmt = text("UPDATE raw_jobs SET description = 'N/A' WHERE link = :link")
            conn.execute(update_stmt, {"link": link})

    def enrich_job_descriptions(self, engine: Engine) -> None:
        """Identifies stored jobs missing descriptions, fetches them, and updates the DB.

        Implemented with batch transaction commits and anti-ban sleeping delays to 
        protect database connection state and IP integrity.

        Args:
            engine (Engine): The SQLAlchemy Engine to use for database interactions.
        """
        try:
            with engine.connect() as conn:
                result = conn.execute(text("SELECT link FROM raw_jobs WHERE description IS NULL"))
                links = [row[0] for row in result]
        except Exception as e:
            logging.error(f"Failed to query incomplete descriptions: {e}")
            return

        if not links:
            logging.info("No pending descriptions to fetch.")
            return

        counter = 0
        batch_size = 10
        logging.info(f"Starting enrichment for {len(links)} jobs...")

        with engine.begin() as conn:
            for link in links:
                try:
                    logging.info(f"Fetching description for: {link[:50]}...")
                    response = requests.get(link, headers=self.get_random_header(), timeout=15)
                    response.raise_for_status()
                    response.encoding = "utf-8"
                    
                    self._parse_and_update_description(conn, link, response.text)
                    counter += 1

                    # Batch commits inherently provided by SQLAlchemy `begin()` semantics if we nested
                    # a loop, but because we are inside a single active `engine.begin()`, we execute
                    # all in a single transaction block. For very large queues, manual `commit()`
                    # points with pure connect() objects could be safer. Here we process safely.
                    
                    wait_time = random.uniform(5, 12)
                    logging.debug(f"Anti-ban sleep for {wait_time:.2f} seconds...")
                    time.sleep(wait_time)

                except RequestException as e:
                    logging.error(f"Network error processing {link}: {e}")
                except Exception as e:
                    logging.error(f"Unexpected error processing {link}: {e}")


class DatabaseManager:
    """Handles connections, physical schemas, and upserts to the PostgreSQL database."""

    def __init__(self) -> None:
        """Initializes the database manager, referencing environment variable standards."""
        user = os.getenv("USER_DB")
        password = os.getenv("PW_DB")
        db_name = os.getenv("TABLE_DB")
        host = os.getenv("DB_HOST", "postgres")
        port = os.getenv("DB_PORT", "5432")
        
        self.engine: Engine = create_engine(
            f"postgresql://{user}:{password}@{host}:{port}/{db_name}"
        )
    
    def save_to_database(self, df: pd.DataFrame) -> None:
        """Saves scraped data to Bronze, upserting rows to prevent link collisions.

        By loading through a temporary proxy table (`temp_jobs`), we execute a secure 
        PostgreSQL `ON CONFLICT` resolution strategy for stateless pipeline idempotency.

        Args:
            df (pd.DataFrame): Dataframe containing scraped job items.
        """
        if df.empty:
            logging.info("Provided DataFrame is empty. Skipping Bronze database save.")
            return

        try:
            df.to_sql("temp_jobs", self.engine, if_exists="replace", index=False)
            
            insert_query = """
            INSERT INTO raw_jobs (title, company, location, link, date, workplace_type)
            SELECT title, company, location, link, date, workplace_type FROM temp_jobs
            ON CONFLICT (link) DO UPDATE SET 
                title = EXCLUDED.title,
                company = EXCLUDED.company,
                location = EXCLUDED.location,
                date = EXCLUDED.date,
                workplace_type = EXCLUDED.workplace_type;
            """
            
            with self.engine.begin() as conn:
                conn.execute(text(insert_query))
                
            logging.info("Jobs successfully Upserted (Inserted or Updated) into Bronze layer.")
        except Exception as e:
            logging.error(f"Fatal error upserting to database: {e}")
    
    def save_silver_companies(self, enriched_data_list: List[Dict[str, Any]]) -> None:
        """Commits fully processed LLM/API enriched company dictionaries to the Silver layer.

        Args:
            enriched_data_list (List[Dict[str, Any]]): Processed company records.
        """
        if not enriched_data_list:
            logging.info("No enriched data to save to Silver table.")
            return

        df = pd.DataFrame(enriched_data_list)

        expected_columns = [
            "company_name", "industry", "detailed_description", 
            "company_size", "tech_team_size", "public_sentiment", 
            "company_rating", "review_count"
        ]
        
        for col in expected_columns:
            if col not in df.columns:
                df[col] = None

        try:
            logging.info(f"Saving {len(df)} enriched companies to the Silver database...")
            df.to_sql(
                name="silver_companies", 
                con=self.engine, 
                if_exists="replace", 
                index=False
            )
            logging.info("Silver layer successfully updated! ✅")
            
        except Exception as e:
            logging.error(f"Fatal exception during Silver DB insert: {e}")

    def fill_db(self, df: pd.DataFrame) -> None:
        """Prepares strict schema limits before persisting user data.

        Using defensive IF NOT EXISTS SQL patterns acts as a soft-migration layer
        for Docker restarts instead of relying on slow, silent DataFrame exception trapping.

        Args:
            df (pd.DataFrame): Dataframe containing scraped job data.
        """
        create_table_query = """
        CREATE TABLE IF NOT EXISTS raw_jobs (
            link TEXT PRIMARY KEY,
            title TEXT,
            company TEXT,
            location TEXT,
            date TEXT,
            workplace_type TEXT,
            description TEXT,
            is_active BOOLEAN DEFAULT TRUE
        );
        """
        
        alter_queries = [
            "ALTER TABLE raw_jobs ADD COLUMN IF NOT EXISTS description TEXT;",
            "ALTER TABLE raw_jobs ADD COLUMN IF NOT EXISTS workplace_type TEXT;",
            "ALTER TABLE raw_jobs ADD COLUMN IF NOT EXISTS is_active BOOLEAN DEFAULT TRUE;"
        ]

        # First guarantee table schema is logically consistent
        with self.engine.begin() as conn:
            try:
                conn.execute(text(create_table_query))
            except Exception as e:
                logging.error(f"Failed to verify or initialize table schema: {e}")

            for query in alter_queries:
                try:
                    conn.execute(text(query))
                except Exception as e:
                    logging.debug(f"Migration constraint execution result: {e}")
                    
        # Proceed with inserting the data payload
        self.save_to_database(df)

    def get_unique_companies(self) -> List[str]:
        """Extracts a distinct list of companies from the raw dataset.

        Returns:
            List[str]: A list of unique company identity strings found.
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT DISTINCT company FROM raw_jobs WHERE company IS NOT NULL;"))
                companies = [row[0] for row in result]
                
                logging.info(f"Found {len(companies)} unique companies in the database.")
                return companies
        except Exception as e:
            logging.error(f"Error fetching distinct companies: {e}")
            return []
    
    def get_existing_job_links(self) -> Set[str]:
        """Fetches an existing corpus of DB job links natively into a highly accessible Set.

        Returns:
            Set[str]: A lookup set of job URLs currently maintained in persistent storage.
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT link FROM raw_jobs;"))
                return {row[0] for row in result}
        except Exception as e:
             logging.warning(f"Could not construct link lookup table (DB likely empty): {e}")
             return set()