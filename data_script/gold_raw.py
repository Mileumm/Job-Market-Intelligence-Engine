from sqlalchemy import text
import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import os
from database_utils import get_engine
from sqlalchemy import inspect

engine = get_engine()

inspector = inspect(engine)

if not inspector.has_table("companies_list"):
#     print("No table found.")
# else:
    gold_sql = """
    CREATE OR REPLACE VIEW gold_market_stats AS
    SELECT
    FROM companies_list;
    """

    try:
        with engine.connect() as conn:
            conn.execute(text(gold_sql))
            conn.commit()
    except Exception as e:
        print(f"Error creating view: {e}")
        
    df_gold = pd.read_sql("SELECT * FROM companies_list", engine)
    company_list = df_gold.company.values.tolist()
    # # Data from your Gold Layer
    # labels = ['SQL', 'Bilingual', 'Python', 'Spark']
    # values = [df_gold['sql_demand_pct'][0], df_gold['bilingual_requirement_pct'][0], df_gold['python_demand_pct'][0], df_gold['spark_demand_pct'][0]]
    # colors = ['#3498db', '#e74c3c', '#2ecc71', '#f1c40f']

    # plt.figure(figsize=(10, 6))
    # bars = plt.barh(labels, values, color=colors)

    # # Add titles and labels
    # plt.title(f"Quebec Data Market: Top Requirements (Sample size: {df_gold['sample_size'][0]})", fontsize=14, pad=20)
    # plt.xlabel('Percentage of Job Postings (%)', fontsize=12)
    # plt.xlim(0, 100) # Scale to 100%

    # # Add the percentage values on the bars
    # for bar in bars:
    #     width = bar.get_width()
    #     plt.text(width + 1, bar.get_y() + bar.get_height()/2, f'{width}%', va='center', fontweight='bold')

    # plt.tight_layout()
    # os.makedirs('./result', exist_ok=True)
    # plt.savefig('./result/quebec_market_report.png')
    print("View is ready for analysis.")
    
    
    
    col_list = df.Courses.values.tolist()