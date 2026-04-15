"""
Gold layer processing for aggregating and analyzing data.
"""

import logging
from typing import List

import pandas as pd
from sqlalchemy import inspect, text
from sqlalchemy.engine.base import Engine
from sqlalchemy.exc import SQLAlchemyError

from database_utils import get_engine

# Configure logging for observability in the pipeline
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class GoldLayerProcessor:
    """Handles the creation of analytics-ready views and data extraction for the Gold layer."""

    def __init__(self, engine: Engine) -> None:
        """Initializes the processor with an active SQLAlchemy engine.

        Args:
            engine (Engine): App-level SQLAlchemy engine object for DB interactions.
        """
        self.engine = engine

    def _table_exists(self, table_name: str) -> bool:
        """Verifies if a specific table exists within the current database schema.

        Args:
            table_name (str): The target table name to introspect.

        Returns:
            bool: True if the table exists, rendering schema operations safe.
        """
        inspector = inspect(self.engine)
        return inspector.has_table(table_name)

    def create_market_stats_view(self) -> None:
        """Creates or replaces the primary Gold layer analytical view.

        This view serves as the foundation for downstream BI dashboards or reports.
        Execution is bypassed safely if prerequisite tables are missing.
        """
        # Architectural Fix: Assert table exists before querying, fixing the original broken logic
        if not self._table_exists("companies_list"):
            logging.warning("Skipping view creation: 'companies_list' table not found.")
            return

        # Fixed syntax error in original SQL (missing columns from SELECT)
        gold_sql = """
        CREATE OR REPLACE VIEW gold_market_stats AS
        SELECT *
        FROM companies_list;
        """

        try:
            # Using begin() implicitly handles the commit transaction safely
            with self.engine.begin() as conn:
                conn.execute(text(gold_sql))
            logging.info("Analytical view 'gold_market_stats' synchronized successfully.")
        except SQLAlchemyError as e:
            logging.error(f"Fatal error generating gold view: {e}")

    def fetch_company_list(self) -> List[str]:
        """Extracts the authoritative universe of processed companies.

        Returns:
            List[str]: Names of companies extracted from the curated gold data table.
        """
        if not self._table_exists("companies_list"):
            logging.warning("Cannot fetch companies: 'companies_list' table not found.")
            return []

        try:
            df_gold = pd.read_sql("SELECT * FROM companies_list", self.engine)
            
            if "company" in df_gold.columns:
                company_list: List[str] = df_gold["company"].dropna().tolist()
                logging.info(f"Loaded {len(company_list)} companies from Gold layer.")
                return company_list
            else:
                logging.warning("Schema mismatch: 'company' column missing from table.")
                return []
                
        except Exception as e:
            logging.error(f"Failed to extract company list DataFrame: {e}")
            return []


def main() -> None:
    """Entry point for executing the Gold layer ETL transformations."""
    try:
        engine = get_engine()
        processor = GoldLayerProcessor(engine)
        
        processor.create_market_stats_view()
        processor.fetch_company_list()
        
        logging.info("Gold layer materialization pipeline completed.")
        
    except Exception as e:
         logging.critical(f"Gold layer pipeline sequence failed: {e}")


if __name__ == "__main__":
    main()