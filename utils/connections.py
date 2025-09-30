import pyodbc
import os
from sqlalchemy import create_engine
from pathlib import Path
from dotenv import load_dotenv

# Charger .env depuis la racine du projet
env_path = Path(__file__).parent.parent / ".env"
load_dotenv(env_path)

def get_progress_connection():
    """Connexion Progress OpenEdge"""
    dsn = f"DSN={os.getenv('PROGRESS_DSN')};UID={os.getenv('PROGRESS_USER')};PWD={os.getenv('PROGRESS_PWD')}"
    return pyodbc.connect(dsn)

def get_sqlserver_connection():
    """Connexion SQL Server via pyodbc (pour requêtes DDL/config)"""
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
    """Engine SQLAlchemy pour chargements rapides (méthode multi)"""
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