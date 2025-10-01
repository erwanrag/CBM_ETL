import sys
import os
from pathlib import Path
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent.parent / ".env")

import pyodbc
import pandas as pd
from src.utils.connections import get_progress_connection, get_sqlserver_connection
from src.utils.connections import get_sql_engine
from src.utils.parquet_cache import CACHE_DIR

def check_progress_connection():
    """Teste connexion Progress"""
    try:
        conn = get_progress_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM PUB.produit")
        count = cursor.fetchone()[0]
        conn.close()
        print(f"✅ Progress : Accessible ({count:,} produits)")
        return True
    except Exception as e:
        print(f"❌ Progress : {e}")
        return False

def check_sqlserver_connection():
    """Teste connexion SQL Server"""
    try:
        engine = get_sql_engine()
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM config.ETL_Tables"))  # AJOUTER text()
            count = result.fetchone()[0]
        print(f"✅ SQL Server : Accessible ({count} tables configurées)")
        return True
    except Exception as e:
        print(f"❌ SQL Server : {e}")
        return False

def check_table_config(table_name):
    """Valide config d'une table spécifique"""
    try:
        engine = get_sql_engine()
        
        # Config table - SANS params car SQLAlchemy != pyodbc
        df_table = pd.read_sql(text(f"""
            SELECT * FROM config.ETL_Tables WHERE TableName = '{table_name}'
        """), engine)
        
        if df_table.empty:
            print(f"❌ {table_name} : Non configurée dans ETL_Tables")
            return False
        
        config = df_table.iloc[0]
        issues = []
        
        # Vérifications
        if not config['PrimaryKeyCols']:
            issues.append("PK manquante")
        
        if not config['DestinationTable']:
            issues.append("DestinationTable non définie")
        
        # Colonnes actives
        df_cols = pd.read_sql(text(f"""
            SELECT COUNT(*) as cnt 
            FROM config.ETL_Columns 
            WHERE TableName = '{table_name}' AND IsExcluded = 0
        """), engine)
        
        col_count = df_cols.iloc[0]['cnt']
        if col_count == 0:
            issues.append("Aucune colonne active")
        
        if issues:
            print(f"⚠️  {table_name} : {', '.join(issues)}")
            return False
        else:
            print(f"✅ {table_name} : {col_count} colonnes actives, PK OK")
            return True
            
    except Exception as e:
        print(f"❌ {table_name} : {e}")
        return False

def check_disk_space():
    """Vérifie espace disque pour cache"""
    try:
        # Utiliser CACHE_DIR depuis parquet_cache.py
        cache_dir = CACHE_DIR
        
        # Taille cache actuel
        cache_files = list(cache_dir.glob("*.parquet"))
        cache_size = sum(f.stat().st_size for f in cache_files)
        cache_mb = cache_size / (1024 * 1024)
        
        # Espace disque disponible
        import shutil
        disk = shutil.disk_usage(cache_dir)
        free_gb = disk.free / (1024 ** 3)
        
        print(f"✅ Disque : {free_gb:.1f} GB libres")
        print(f"   Cache : {len(cache_files)} fichiers ({cache_mb:.1f} MB)")
        
        if free_gb < 2:
            print(f"⚠️  Espace disque faible : {free_gb:.1f} GB")
            return False
        
        return True
        
    except Exception as e:
        print(f"❌ Disque : {e}")
        return False

def run_preflight_check(table_name=None):
    """Exécute toutes les vérifications"""
    print("="*80)
    print("🔍 PREFLIGHT CHECK - Vérification environnement ETL")
    print("="*80)
    
    checks = {
        "Progress": check_progress_connection(),
        "SQL Server": check_sqlserver_connection(),
        "Disque": check_disk_space()
    }
    
    if table_name:
        print(f"\n📋 Vérification table : {table_name}")
        checks[f"Config {table_name}"] = check_table_config(table_name)
    
    print("\n" + "="*80)
    success = all(checks.values())
    
    if success:
        print("✅ PREFLIGHT CHECK : OK - Vous pouvez lancer l'ETL")
    else:
        failed = [k for k, v in checks.items() if not v]
        print(f"❌ PREFLIGHT CHECK : ÉCHEC")
        print(f"   Checks échoués : {', '.join(failed)}")
    
    print("="*80)
    
    return success

if __name__ == "__main__":
    table_name = sys.argv[1] if len(sys.argv) > 1 else None
    success = run_preflight_check(table_name)
    sys.exit(0 if success else 1)