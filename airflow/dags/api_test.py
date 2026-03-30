from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json

def test_rapidapi_connection():
    """Tests the RapidAPI connection and logs the raw response."""
    print("🚀 Starting API Diagnostic Test...")
    
    try:
        # Import inside the task to ensure it uses the Docker container's paths
        from scripts.company_enrichment import CompanyEnricher
        
        enricher = CompanyEnricher()
        test_company = "Ubisoft"
        
        print(f"📡 Testing connection to RapidAPI for company: {test_company}")
        
        # Call your existing function
        result = enricher.get_google_reviews(test_company)
        
        print("\n✅ --- RAW API RESPONSE ---")
        print(json.dumps(result, indent=4))
        print("--------------------------\n")
        
        # Simple validation check
        if result.get("rating") in ["Unknown", "N/A", "NaN", None, "nan"]:
            print("⚠️ WARNING: The API returned empty/unknown data.")
            print("Possible causes: Expired key, exceeded monthly quota, or the API endpoint changed.")
        else:
            print("🎉 SUCCESS: The API returned valid rating data!")
            
    except Exception as e:
        print(f"\n❌ CRITICAL ERROR: The API request failed entirely.")
        print(f"Details: {e}")
        raise e  # Force the task to fail in the UI

with DAG(
    dag_id="diagnostic_api_connection",
    start_date=datetime(2024, 1, 1),
    schedule=None, # Run manually only
    catchup=False,
    tags=["diagnostic", "api"],
) as dag:
    
    run_test = PythonOperator(
        task_id="test_rapidapi_key",
        python_callable=test_rapidapi_connection
    )