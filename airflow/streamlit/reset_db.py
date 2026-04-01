from scripts.bronze_raw import DatabaseManager
from sqlalchemy import text

def reset_database():
    print("⚠️ Initiating Database Reset...")
    db = DatabaseManager()
    
    try:
        with db.engine.connect() as conn:
            # 1. Drop the tables completely
            conn.execute(text("DROP TABLE IF EXISTS raw_jobs;"))
            conn.execute(text("DROP TABLE IF EXISTS silver_companies;"))
            conn.commit()
            print("✅ Successfully destroyed 'raw_jobs' and 'silver_companies' tables.")
            print("🧹 You are now starting from scratch!")
            
    except Exception as e:
        print(f"❌ Error dropping tables: {e}")

if __name__ == "__main__":
    reset_database()