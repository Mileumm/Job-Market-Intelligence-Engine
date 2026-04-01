import os
import json
import requests
from bs4 import BeautifulSoup
from google import genai

class CompanyEnricher:

    models_to_try = [
            'gemini-3-flash-preview',       # The Smartest: 20 requests per day
            'gemini-2.5-flash',       # The Reliable Backup: 20 requests per day
            'gemini-3.1-flash-lite',  # The Workhorse: 500 requests per day!
            'gemini-2.5-flash-lite'   # The Final Safety Net: 20 requests per day
        ]
    def __init__(self):
        # Initialize variables inside __init__ instead of at the class level
        self.url = ""
        
        api_key = os.getenv("GEMINI_API_KEY")
        if not api_key:
            raise ValueError("GEMINI_API_KEY not found in environment!")
            
        self.client = genai.Client(api_key=api_key)

    def get_company_url(self, company_name):
        """Step 1: Find the official website URL."""
        try:
            # Clearbit Autocomplete API
            response = requests.get(f"https://autocomplete.clearbit.com/v1/companies/suggest?query={company_name}", timeout=10)
            if response.status_code == 200 and len(response.json()) > 0:
                return response.json()[0]['domain']
        except Exception as e:
            print(f"⚠️ Could not find domain for {company_name}: {e}")
        return None

    def get_clean_text_from_url(self, domain):
        """Step 2: Download the page and strip all HTML."""
        self.url = f"https://www.{domain}"
        try:
            # Modern User-Agent to prevent 403 Forbidden errors
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
            response = requests.get(self.url, headers=headers, timeout=10)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Remove useless tags that waste AI token limits
            for script in soup(["script", "style", "nav", "footer"]):
                script.extract()
                
            text = soup.get_text(separator=' ', strip=True)
            return text[:4000] 
        except Exception as e:
            print(f"⚠️ Failed to scrape {domain}: {e}")
            return "" # Returning an empty string is safer than None for the AI prompt
    
    def extract_batch_company_info(self, company_batch_list):
        """
        Takes a list of dictionaries [{'company_name': 'X', 'clean_text': 'Y'}, ...]
        and asks Gemini to return a JSON array containing the enriched data for ALL of them.
        """
        # Convert the Python list to a string so the AI can read it
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
        for model_name in self.models_to_try:
            try:
                response = self.client.models.generate_content(
                    model=model_name,
                    contents=prompt,
                )
                
                raw_text = response.text.strip()
                # Clean up potential markdown formatting
                if raw_text.startswith("```json"):
                    raw_text = raw_text[7:-3].strip()
                elif raw_text.startswith("```"):
                    raw_text = raw_text[3:-3].strip()
                    
                return json.loads(raw_text)
                
            except Exception as e:
                error_msg = str(e)
                if "429" in error_msg or "RESOURCE_EXHAUSTED" in error_msg:
                    print(f"⚠️ {model_name} quota exhausted! Switching to next model...")
                else:
                    print(f"❌ API Error with {model_name}: {e}. Skipping to next model...")
                
                # NO BREAK STATEMENT HERE! Always try the next model.
                continue
                    
        # If the loop finishes and we are down here, ALL models failed.
        print("❌ CRITICAL: All models failed for this company batch.")
        fallback_results = []
        for comp in company_batch_list:
            fallback_results.append({"company_name": comp['company_name']})
        return fallback_results
    
    def extract_batch_job_details(self, jobs_batch):
        """Analyzes multiple job descriptions with Automatic Model Fallback."""
        import time 
        import json
        
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
            desc = str(job['description'])[:8000] if job['description'] not in ['N/A', None] else "No description"
            prompt += f"\n--- JOB_ID: {job['link']} ---\n{desc}\n"
            
        # 🌟 THE SWITCH: The pipeline will try Model 1. If it hits a quota limit, it switches to Model 2.

        for model_name in self.models_to_try:
            try:
                print(f"🧠 Asking {model_name} to analyze batch of {len(jobs_batch)} jobs...")
                
                response = self.client.models.generate_content(
                    model=model_name,
                    contents=prompt,
                )
                
                result_text = response.text.strip()
                if result_text.startswith("```json"):
                    result_text = result_text[7:-3].strip()
                elif result_text.startswith("```"):
                    result_text = result_text[3:-3].strip()
                    
                return json.loads(result_text)
                
            except Exception as e:
                error_msg = str(e)
                if "429" in error_msg or "RESOURCE_EXHAUSTED" in error_msg:
                    print(f"⚠️ {model_name} quota exhausted! Switching to next model...")
                else:
                    print(f"❌ API Error with {model_name}: {e}. Skipping to next model...")
                
                # NO BREAK STATEMENT HERE! Always try the next model.
                continue
                    
        print("❌ CRITICAL: All fallback models have been exhausted for the day.")
        return []