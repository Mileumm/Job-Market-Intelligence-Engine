"""
Company and job description enrichment logic using Clearbit and Gemini AI.
"""

import json
import logging
import os
import time
from typing import Any, Dict, List, Optional

import requests
from bs4 import BeautifulSoup
from google import genai
from requests.exceptions import RequestException

# Configure logging for Airflow observability
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class CompanyEnricher:
    """Service class targeting external APIs to augment raw job posting records.

    Handles resolving official domains via Clearbit, scraping corporate content, 
    and prompting Gemini for summarized feature extraction with quota fallback mechanisms.
    """

    # Static list of models ordered by preference to handle Airflow daily quotas automatically
    models_to_try: List[str] = [
        'gemini-3-flash-preview',  # The Smartest: 20 requests per day
        'gemini-2.5-flash',       # The Reliable Backup: 20 requests per day
        'gemini-3.1-flash-lite',  # The Workhorse: 500 requests per day!
        'gemini-2.5-flash-lite'   # The Final Safety Net: 20 requests per day
    ]

    def __init__(self) -> None:
        """Initializes the enrichment service and establishes client connections."""
        api_key = os.getenv("GEMINI_API_KEY")
        if not api_key:
            raise ValueError("GEMINI_API_KEY not found in environment variable configurations!")
            
        self.client = genai.Client(api_key=api_key)

    def get_company_url(self, company_name: str) -> Optional[str]:
        """Queries Clearbit's Autocomplete API to find the official domain.

        Args:
            company_name (str): The raw string extracted from the job hosting platform.

        Returns:
            Optional[str]: The top matching domain name, or None if network/parsing fails.
        """
        try:
            url = f"https://autocomplete.clearbit.com/v1/companies/suggest?query={company_name}"
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            if isinstance(data, list) and len(data) > 0:
                domain = data[0].get('domain')
                if domain:
                    return domain
        except RequestException as e:
            logging.warning(f"Network error finding domain for {company_name}: {e}")
        except Exception as e:
            logging.warning(f"Unexpected parsing error for {company_name}: {e}")
            
        return None

    def get_clean_text_from_url(self, domain: str) -> str:
        """Fetches the corporate website and extracts cleaned semantic text.

        Args:
            domain (str): The root domain to scrape (e.g., "example.com").

        Returns:
            str: Truncated clean HTML body text, or empty string if inaccessible.
        """
        target_url = f"https://www.{domain}"
        try:
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
            response = requests.get(target_url, headers=headers, timeout=10)
            
            soup = BeautifulSoup(response.text, 'html.parser')
            for tag in soup(["script", "style", "nav", "footer"]):
                tag.extract()
                
            text = soup.get_text(separator=' ', strip=True)
            return text[:4000] 
        except RequestException as e:
            logging.warning(f"Failed to scrape {target_url}: {e}")
            return ""
        except Exception as e:
            logging.warning(f"HTML processing failed for {target_url}: {e}")
            return ""

    def _generate_with_fallback(self, prompt: str, batch_size: int) -> Optional[List[Dict[str, Any]]]:
        """A single source of truth for making Gemini requests with limit resilience.

        Iterates downward through `models_to_try` if quota blocks are hit. Handles
        cleaning AI markdown structures to parse raw JSON.

        Args:
            prompt (str): The engineered prompt payload.
            batch_size (int): Expected array length for contextual logging.

        Returns:
            Optional[List[Dict[str, Any]]]: Array of JSON dictionaries if parsing succeeds, None otherwise.
        """
        for model_name in self.models_to_try:
            try:
                logging.info(f"🧠 Asking {model_name} to process batch of {batch_size} items...")
                response = self.client.models.generate_content(
                    model=model_name,
                    contents=prompt,
                )
                
                raw_text = response.text.strip()
                
                # Sanitize markdown artifacts common in LLMs
                if raw_text.startswith("```json"):
                    raw_text = raw_text[7:-3].strip()
                elif raw_text.startswith("```"):
                    raw_text = raw_text[3:-3].strip()
                    
                return json.loads(raw_text)
                
            except json.JSONDecodeError as j_err:
                logging.error(f"❌ {model_name} returned malformed JSON: {j_err}. Skipping to next...")
                continue
            except Exception as e:
                error_msg = str(e)
                if "429" in error_msg or "RESOURCE_EXHAUSTED" in error_msg:
                    logging.warning(f"⚠️ {model_name} quota exhausted! Switching to next model fallback...")
                else:
                    logging.error(f"❌ API Exception with {model_name}: {e}. Shifting models...")
                
                # Briefly sleep to avoid bursting Google's rate limiters immediately
                time.sleep(2)
                continue
                    
        logging.error("❌ CRITICAL: All fallback models failed for this batch sequence.")
        return None

    def extract_batch_company_info(self, company_batch_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Queries Gemini to summarize and score corporate context for scraped data.

        Args:
            company_batch_list (List[Dict[str, Any]]): Array of items containing 'company_name' and 'clean_text'.

        Returns:
            List[Dict[str, Any]]: The enriched records. Returns stub records if all AI attempts fail.
        """
        batch_text = json.dumps(company_batch_list, ensure_ascii=False)
        
        prompt = f"""
        You are an expert tech recruiter and data analyst. 
        I am giving you a JSON array containing {len(company_batch_list)} companies and the raw text scraped from their websites.
        
        Your job is to analyze EACH company and return a single JSON array containing {len(company_batch_list)} objects.
        
        INPUT DATA:
        {batch_text}
        
        OUTPUT FORMAT REQUIREMENTS:
        Return ONLY a valid JSON array of objects. Do not use markdown wrappers like ```json.
        Each object in the array MUST contain these exact keys:
        - "company_name": (The exact name provided in the input)
        - "industry": (e.g., "Fintech", "E-commerce", "Consulting")
        - "website": (The URL, or "Unknown")
        - "detailed_description": (A 3-sentence summary of what the company does)
        - "company_size": (e.g., "10-50", "500-1000", "10000+")
        - "tech_team_size": (Estimate based on context, or "Unknown")
        - "public_sentiment": (1 sentence on their reputation)
        - "rating": (Out of 5.0, e.g., 4.2)
        - "reviews": (Integer count of reviews)
        """
        
        # Fire AI processing logic
        results = self._generate_with_fallback(prompt, len(company_batch_list))
        
        if results is not None:
            return results
            
        # Ultimate safety fallback guaranteeing pipeline continuation
        fallback_results = []
        for comp in company_batch_list:
            fallback_results.append({"company_name": comp.get('company_name', 'Unknown')})
            
        return fallback_results
    
    def extract_batch_job_details(self, jobs_batch: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Analyzes multiple job descriptions iteratively to structure hard-to-parse unstructured data.

        Args:
            jobs_batch (List[Dict[str, Any]]): Array of dicts containing a 'link' identity and 'description'.

        Returns:
            List[Dict[str, Any]]: Records mapping 'link' to extracted metrics ('tech_stack', 'salary'), 
                or an empty array if completely failed.
        """
        prompt = """
        You are an expert IT Recruiter AI. Analyze the following batch of job descriptions.
        For EACH job, extract:
        1. TECH STACK: A list of core technical skills/tools (max 6).
        2. SALARY: The specific compensation mentioned. If none, output "Not disclosed".

        Output ONLY a valid JSON array of objects. Do not include markdown formatting.
        
        Required JSON Format:
        [
            {
                "link": "The exact JOB_ID provided",
                "tech_stack": ["Tool1", "Tool2"],
                "salary": "Extracted salary or 'Not disclosed'"
            }
        ]
        
        JOBS TO ANALYZE:
        """
        
        for job in jobs_batch:
            desc = str(job.get('description', ''))[:8000] if job.get('description') not in ['N/A', None] else "No description"
            prompt += f"\n--- JOB_ID: {job.get('link', 'Unknown')} ---\n{desc}\n"
            
        results = self._generate_with_fallback(prompt, len(jobs_batch))
        
        if results is not None:
            return results
            
        return []