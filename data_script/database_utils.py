import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import urllib.parse

def get_engine():
    load_dotenv()
    user = urllib.parse.quote_plus(os.getenv("USER_DB"))
    password = urllib.parse.quote_plus(os.getenv("PW_DB"))
    db_name = os.getenv("TABLE_DB")
    host = "postgres"
    port = "5432"
    db_url = f"postgresql://{user}:{password}@{host}:{port}/{db_name}"
    return create_engine(db_url)