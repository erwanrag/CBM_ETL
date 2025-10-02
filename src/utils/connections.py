# src/utils/connections.py
import pyodbc
import os
from sqlalchemy import create_engine
from pathlib import Path
from dotenv import load_dotenv
from src.utils.progress_breaker import with_progress_breaker  # NOUVEAU

# Charger .env
current_dir = Path(__file__).resolve()
for parent in [current_dir.parent.parent.parent, current_dir.parent.parent]:
    env_file = parent / ".env"
    if env_file.exists():
        load_dotenv(env_file)
        break
else:
    load_dotenv()

@with_progress_breaker  
def get_progress_connection():
    """Connexion Progress avec circuit breaker"""
    dsn = f"DSN={os.getenv('PROGRESS_DSN')};UID={os.getenv('PROGRESS_USER')};PWD={os.getenv('PROGRESS_PWD')}"
    
    print("🔌 Connexion Progress...")
    
    try:
        conn = pyodbc.connect(dsn, timeout=30)
        print("✅ Connexion Progress établie")
        return conn
    except Exception as e:
        print(f"❌ Échec connexion Progress : {e}")
        raise

def get_sqlserver_connection():
    """Connexion SQL Server via pyodbc"""
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={os.getenv('SQL_SERVER')};"
        f"DATABASE={os.getenv('SQL_DATABASE')};"
        "Trusted_Connection=yes;"
        "Connection Timeout=300;"
        "Command Timeout=600;"
    )
    return pyodbc.connect(conn_str)

def get_sql_engine():
    """Engine SQLAlchemy"""
    server = os.getenv('SQL_SERVER')
    database = os.getenv('SQL_DATABASE')
    
    conn_str = (
        f"mssql+pyodbc://{server}/{database}"
        "?driver=ODBC+Driver+17+for+SQL+Server"
        "&trusted_connection=yes"
        "&fast_executemany=True"
    )
    
    return create_engine(
        conn_str,
        pool_pre_ping=True,
        pool_recycle=3600,
        echo=False
    )