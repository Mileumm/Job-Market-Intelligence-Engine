"""
Database testing script to verify connection logic from the local host machine.
"""

import logging
import os

import pandas as pd
from sqlalchemy.exc import SQLAlchemyError

from database_utils import get_engine

# Configure logging for clear visualization in the terminal
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def test_table_connection(table_name: str = "companies_list") -> None:
    """Tests the database connection and queries a specific table.

    Overrides the default Docker network host ('postgres') with 'localhost'
    so this script can be executed directly from the host operating system.

    Args:
        table_name (str): The name of the table or view to query.
    """
    # Force the local machine to route to localhost instead of Docker's internal network
    os.environ["DB_HOST"] = "localhost"
    
    try:
        logging.info("Initializing database connection from local host...")
        engine = get_engine()
        
        logging.info(f"Executing test query against '{table_name}'...")
        
        # We add LIMIT 10 to ensure the test runs quickly even if the table grows to millions of rows
        query = f"SELECT * FROM {table_name} LIMIT 10"
        
        # pd.read_sql manages connection checking and cursor closing implicitly
        df_check = pd.read_sql(query, engine)
        
        if df_check.empty:
            logging.warning(f"Connection successful, but '{table_name}' is currently empty.")
        else:
            logging.info(f"Success! Retrieved {len(df_check)} sample rows.")
            print("\n--- Data Sample ---")
            print(df_check)
            print("-------------------\n")
            
    except SQLAlchemyError as err:
        logging.error("Failed to connect or query the database.")
        logging.error(f"SQLAlchemy Details: {err}")
        logging.info("Hint: Make sure your Docker container is running and port 5432 is exposed!")
    except ValueError as err:
        logging.error(f"Configuration error: {err}")
    except Exception as err:
        logging.error(f"Unexpected error during connection test: {err}")


def main() -> None:
    """Main execution block triggering the isolated connection test."""
    test_table_connection()


if __name__ == "__main__":
    main()