from sqlalchemy import text
import pandas as pd
from sqlalchemy import create_engine
from database_utils import get_engine
from sqlalchemy import inspect

engine = get_engine()
inspector = inspect(engine)


if not inspector.has_table("raw_jobs"):
#     exit
# else:

    silver_sql = """
    CREATE OR REPLACE VIEW silver_jobs_skills AS
    SELECT 
        title,
        company,
        -- 1. Tech Skills Extraction (using case-insensitive regex)
        (description ~* 'python') AS has_python,
        (description ~* 'sql|postgresql|snowflake|bigquery') AS has_sql,
        (description ~* 'spark|pyspark|databricks') AS has_spark,
        (description ~* 'aws|azure|gcp|cloud') AS has_cloud,
        (description ~* 'airflow|prefect|dagster') AS has_orchestration,
        
        -- 2. Language & Location Logic (Crucial for the Quebec Market)
        (description ~* 'french|français|bilingue|bilingual') AS is_bilingual,
        (location ~* 'montréal|montreal') AS is_in_mtl,
        
        -- 3. Career Level Logic
        (title ~* 'intern|stagiaire|stage|junior|student|étudiant') AS is_entry_level

    FROM raw_jobs;
    """

    try:
        with engine.connect() as conn:
            conn.execute(text(silver_sql))
            conn.commit()
        print("Transformation complete.")
    except Exception as e:
        print(f"Error creating view: {e}")

