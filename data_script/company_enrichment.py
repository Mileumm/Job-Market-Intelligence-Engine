import os
import json
import requests
import time
from bs4 import BeautifulSoup
from google import genai
from duckduckgo_search import DDGS
import re
import time
import random

class CompanyEnricher:
    def __init__(self):
        api_key = os.getenv("GEMINI_API_KEY")
        if not api_key:
            raise ValueError("GEMINI_API_KEY not found in environment!")
            
        # 1. NEW INITIALIZATION (Only the Client is needed now)
        self.client = genai.Client(api_key=api_key)

    def get_company_url(self, company_name):
        """Step 1: Find the official website URL."""
        try:
            response = requests.get(f"https://autocomplete.clearbit.com/v1/companies/suggest?query={company_name}")
            if response.status_code == 200 and len(response.json()) > 0:
                return response.json()[0]['domain']
        except Exception as e:
            print(f"Could not find domain for {company_name}: {e}")
        return None

    def get_clean_text_from_url(self, domain):
        """Step 2: Download the page and strip all HTML."""
        url = f"https://www.{domain}"
        try:
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
            response = requests.get(url, headers=headers, timeout=10)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            for script in soup(["script", "style", "nav", "footer"]):
                script.extract()
                
            text = soup.get_text(separator=' ', strip=True)
            return text[:4000] 
        except Exception as e:
            print(f"Failed to scrape {domain}: {e}")
            return None

    def extract_company_info(self, clean_text):
        """Step 3: Ask Gemini to extract deep insights and output strict JSON."""
        prompt = f"""
        You are an expert Data Analyst AI. Read the following text extracted from a company's official website.
        Extract the requested information and output ONLY a valid JSON object. 
        If a specific piece of information is not mentioned in the text, use the exact string "Unknown".
        Do not include markdown formatting like ```json.
        
        Required JSON Format: 
        {{
            "industry": "Primary sector (e.g., FinTech, Video Games, E-commerce)",
            "detailed_description": "A comprehensive 4 to 5 sentence summary of what the company does, its main products, and its mission.",
            "company_size": "Total number of employees if mentioned (e.g., '10-50', 'Over 1000', or 'Unknown')",
            "public_sentiment": "A short summary of the work culture or employee testimonials AS DESCRIBED ON THE WEBSITE, otherwise 'Unknown'"
        }}
        
        Text to analyze: {clean_text}
        """
        
        try:
            print("Asking Gemini to extract detailed info...")
            
            # 2. NEW GENERATION SYNTAX
            response = self.client.models.generate_content(
                model='gemini-2.5-flash',
                contents=prompt,
            )
            
            result_text = response.text.strip()
            
            # Safety check: Remove markdown code blocks
            if result_text.startswith("```json"):
                result_text = result_text[7:-3].strip()
            elif result_text.startswith("```"):
                result_text = result_text[3:-3].strip()
                
            return json.loads(result_text)
            
        except Exception as e:
            print(f"Gemini API Error or JSON Parsing Failed: {e}")
            # 3. CRITICAL SAFETY NET: Return empty dictionary to prevent DAG crash
            return {} 
        
    def get_google_reviews(self, company_name):
        """Extracts company ratings from DuckDuckGo search results."""
        print(f"🔍 Searching rating for {company_name} via DuckDuckGo...")
        sleep_time = random.uniform(2.0, 5.0)
        print(f"⏳ Sleeping for {sleep_time:.2f} seconds to respect rate limits...")
        time.sleep(sleep_time)
        try:
            with DDGS() as ddgs:
                # Let's make the query even more specific
                query = f"{company_name} glassdoor reviews rating"
                results = list(ddgs.text(query, max_results=3))
                
                print("\n--- RAW SEARCH ENGINE RESULTS ---")
                for i, r in enumerate(results):
                    # Safely extract whatever keys the library returns
                    content = str(r) 
                    print(f"Result {i+1}: {content}")
                    
                    # Try to extract a rating like 4.1 or 3.8
                    match = re.search(r'\b([1-4]\.\d|5\.0)\b', content)
                    
                    if match:
                        rating = float(match.group(1))
                        print(f"✅ Found rating {rating} for {company_name}")
                        print("---------------------------------\n")
                        return {
                            "rating": rating,
                            "review_count": "N/A" 
                        }
                print("---------------------------------\n")
            
                        
        except Exception as e:
            print(f"⚠️ DDG Search failed for {company_name}: {e}")
            
        return {
            "rating": "Unknown",
            "review_count": 0
        }