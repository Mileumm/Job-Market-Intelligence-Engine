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
        (description ~* 'Python') AS programming,
        (description ~* 'AWS|GCP|Azure') AS cloud,
        (description ~* 'Spark|Snowflake|Airflow|dbt') AS tools,
        (description ~* 'French') AS language,
        (location ~* 'montréal|montreal') AS is_in_mtl,
        (title ~* 'intern|stagiaire|stage|junior|student|étudiant') AS is_entry_level
    FROM raw_jobs;
    """
    silver_sql_ia = """
    CREATE OR REPLACE VIEW companies_list AS
    SELECT DISTINCT
        company
    FROM silver_jobs_skills;
    """
    try:
        with engine.connect() as conn:
            conn.execute(text(silver_sql))
            conn.execute(text(silver_sql_ia))
            conn.commit()
        print("Transformation complete.")
    except Exception as e:
        print(f"Error creating view: {e}")

