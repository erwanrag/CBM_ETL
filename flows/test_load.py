import sys
import os
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from etl_logger import ETLLogger

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")

import pandas as pd
import pyodbc

# Connexions
SQLSERVER_CONN = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    f"SERVER={os.getenv('SQL_SERVER')};"
    f"DATABASE={os.getenv('SQL_DATABASE')};"
    "Trusted_Connection=yes;"
)

# Test simple sans Prefect
logger = ETLLogger(SQLSERVER_CONN)
print(f"Logger initialisé avec RunId: {logger.run_id}")

try:
    logger.log_step("produit", "test", "started")
    print("✅ Log écrit avec succès!")
    
    # Vérifier dans la base
    conn = pyodbc.connect(SQLSERVER_CONN)
    df = pd.read_sql("SELECT TOP 5 * FROM etl.ETL_Log ORDER BY LogTs DESC", conn)
    conn.close()
    print("\nDerniers logs:")
    print(df)
    
except Exception as e:
    print(f"❌ Erreur: {e}")
    import traceback
    traceback.print_exc()