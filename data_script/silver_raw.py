"""
Silver layer transformations using SQL views for regex-based feature extraction.
"""

import logging

from sqlalchemy import inspect, text
from sqlalchemy.engine.base import Engine
from sqlalchemy.exc import SQLAlchemyError

from database_utils import get_engine

# Configure logging for pipeline observability
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class SilverLayerProcessor:
    """Handles the creation of normalized business-logic views in the Silver layer."""

    def __init__(self, engine: Engine) -> None:
        """Initializes the processor with an active SQLAlchemy database engine.

        Args:
            engine (Engine): App-level SQLAlchemy engine object.
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

    def create_silver_views(self) -> None:
        """Generates derivative SQL views utilizing regular expressions on raw data.

        Extracts core skills, location flags, and seniority levels directly via
        PostgreSQL regex operators (~*). Also creates a distinct companies lookup.
        """
        # Architectural Fix: Ensure base table exists, reverting the previously broken logic
        if not self._table_exists("raw_jobs"):
            logging.warning("Skipping operations: 'raw_jobs' table not found.")
            return

        silver_sql = """
        CREATE OR REPLACE VIEW silver_jobs_skills AS
        SELECT 
            title,
            company,
            (description ~* 'Python') AS programming,
            (description ~* 'AWS|GCP|Azure') AS cloud,
            (description ~* 'Spark|Snowflake|Airflow|dbt') AS tools,
            (description ~* 'French|Français') AS language,
            (location ~* 'montréal|montreal') AS is_in_mtl,
            (title ~* 'intern|stagiaire|stage|junior|student|étudiant') AS is_entry_level
        FROM raw_jobs;
        """
        
        # Defensive programming: filter out null companies to prevent downstream API crashes
        silver_sql_ia = """
        CREATE OR REPLACE VIEW companies_list AS
        SELECT DISTINCT
            company
        FROM silver_jobs_skills
        WHERE company IS NOT NULL;
        """
        
        try:
            # Using begin() handles the atomic commit securely automatically
            with self.engine.begin() as conn:
                conn.execute(text(silver_sql))
                conn.execute(text(silver_sql_ia))
                
            logging.info("Silver transformation views synchronized successfully.")
            
        except SQLAlchemyError as e:
            logging.error(f"Fatal SQL error during Silver view derivation: {e}")


def main() -> None:
    """Entry point for executing the Silver layer materialization."""
    try:
        engine = get_engine()
        processor = SilverLayerProcessor(engine)
        processor.create_silver_views()
        logging.info("Silver layer processing completed.")
    except Exception as e:
        logging.critical(f"Silver layer execution sequence failed: {e}")


if __name__ == "__main__":
    main()
