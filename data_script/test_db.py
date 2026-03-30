import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv()
# Use the same engine setup as your ingest script
# engine = create_engine(db_url)



user = os.getenv("USER_DB")
password = os.getenv("PW_DB")
db_name = os.getenv("TABLE_DB")
host = "localhost"
port = "5432"

db_url = f"postgresql://{user}:{password}@{host}:{port}/{db_name}"
engine = create_engine(db_url)

df_check = pd.read_sql("SELECT * FROM companies_list ", engine)
print(df_check)