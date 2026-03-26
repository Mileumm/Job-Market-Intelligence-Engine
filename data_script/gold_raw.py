from sqlalchemy import text
import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import os
from database_utils import get_engine
from sqlalchemy import inspect

engine = get_engine()

inspector = inspect(engine)

if not inspector.has_table("raw_jobs"):
#     print("No table found.")
# else:
    gold_sql = """
    CREATE OR REPLACE VIEW gold_market_stats AS
    SELECT 
        COUNT(*) as sample_size,
        -- Calculate percentage of jobs requiring each skill
        ROUND(AVG(has_python::int) * 100, 1) as python_demand_pct,
        ROUND(AVG(has_sql::int) * 100, 1) as sql_demand_pct,
        ROUND(AVG(has_spark::int) * 100, 1) as spark_demand_pct,
        ROUND(AVG(has_cloud::int) * 100, 1) as cloud_demand_pct,
        ROUND(AVG(is_bilingual::int) * 100, 1) as bilingual_requirement_pct,
        -- Calculate specific stats for Internships only
        COUNT(*) FILTER (WHERE is_entry_level) as total_internships,
        ROUND(AVG(has_python::int) FILTER (WHERE is_entry_level) * 100, 1) as python_demand_interns
    FROM silver_jobs_skills;
    """

    try:
        with engine.connect() as conn:
            conn.execute(text(gold_sql))
            conn.commit()
    except Exception as e:
        print(f"Error creating view: {e}")
        
    df_gold = pd.read_sql("SELECT * FROM gold_market_stats", engine)

    # Data from your Gold Layer
    labels = ['SQL', 'Bilingual', 'Python', 'Spark']
    values = [df_gold['sql_demand_pct'][0], df_gold['bilingual_requirement_pct'][0], df_gold['python_demand_pct'][0], df_gold['spark_demand_pct'][0]]
    colors = ['#3498db', '#e74c3c', '#2ecc71', '#f1c40f']

    plt.figure(figsize=(10, 6))
    bars = plt.barh(labels, values, color=colors)

    # Add titles and labels
    plt.title(f"Quebec Data Market: Top Requirements (Sample size: {df_gold['sample_size'][0]})", fontsize=14, pad=20)
    plt.xlabel('Percentage of Job Postings (%)', fontsize=12)
    plt.xlim(0, 100) # Scale to 100%

    # Add the percentage values on the bars
    for bar in bars:
        width = bar.get_width()
        plt.text(width + 1, bar.get_y() + bar.get_height()/2, f'{width}%', va='center', fontweight='bold')

    plt.tight_layout()
    os.makedirs('./result', exist_ok=True)
    plt.savefig('./result/quebec_market_report.png')
    print("View is ready for analysis.")