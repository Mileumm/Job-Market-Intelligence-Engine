"""
Database utility module for managing core data connections.
"""

import os
import urllib.parse
from typing import Optional

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine

# Module-level load ensures environments are ready before function invocations
load_dotenv()


def get_engine() -> Engine:
    """Constructs and returns a SQLAlchemy Engine using environment variables.

    Verifies the existence of critical database credentials and applying URL 
    encoding to username and password to prevent connection string injection 
    or parsing failures due to special characters.
    
    Raises:
        ValueError: If required database credentials are missing from the environment.

    Returns:
        Engine: A configured SQLAlchemy engine instance for PostgreSQL.
    """
    raw_user: Optional[str] = os.getenv("USER_DB")
    raw_password: Optional[str] = os.getenv("PW_DB")
    db_name: Optional[str] = os.getenv("TABLE_DB")
    
    if not raw_user or not raw_password or not db_name:
        raise ValueError(
            "Critical database configuration variables (USER_DB, PW_DB, TABLE_DB) missing."
        )

    user: str = urllib.parse.quote_plus(raw_user)
    password: str = urllib.parse.quote_plus(raw_password)
    
    # Do not hardcode infrastructure endpoints. Use fallbacks for Docker default behavior.
    host: str = os.getenv("DB_HOST", "postgres")
    port: str = os.getenv("DB_PORT", "5432")
    
    db_url: str = f"postgresql://{user}:{password}@{host}:{port}/{db_name}"
    
    # Engine creation is lazy and does not connect until queried, 
    # but we can return the configured object securely.
    return create_engine(db_url)