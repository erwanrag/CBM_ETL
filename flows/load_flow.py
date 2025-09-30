import sys
import os
from pathlib import Path

# Configuration Prefect AVANT import
os.environ['PREFECT_API_URL'] = 'http://127.0.0.1:4200/api'

from prefect import flow
from datetime import datetime

# Ajouter le chemin parent pour les imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")

from etl_logger import ETLLogger
from tasks.config_tasks import get_table_config, get_included_columns, build_where_clause
from tasks.extract_tasks import extract_to_parquet
from tasks.transform_tasks import transform_from_parquet
from tasks.staging_config_tasks import ensure_stg_table
from tasks.staging_tasks import load_staging_from_parquet
from tasks.ods_tasks import ensure_ods_table, merge_to_ods, update_last_success

# Connexion SQL Server pour le logger
SQLSERVER_CONN = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    f"SERVER={os.getenv('SQL_SERVER')};"
    f"DATABASE={os.getenv('SQL_DATABASE')};"
    "Trusted_Connection=yes;"
    "Connection Timeout=300;"
    "Command Timeout=600;"
)

@flow(name="ETL Principal", retries=2, retry_delay_seconds=60)
def load_flow(table_name: str, mode: str = "incremental"):
    """
    Flow principal de chargement ETL
    
    Args:
        table_name: Nom de la table à charger
        mode: 'full' (chargement complet) ou 'incremental' (delta)
    """
    logger = ETLLogger(SQLSERVER_CONN)
    start_time = datetime.now()
    
    print(f"🚀 ETL {table_name} ({mode}) - Run ID: {logger.run_id}")
    print("=" * 60)
    
    try:
        logger.log_step(table_name, "flow_start", "started")
        
        # 1. Configuration
        print("\n📋 Étape 1/5 : Chargement configuration")
        config = get_table_config(table_name)
        columns = get_included_columns(table_name)
        where_clause = build_where_clause(config, mode=mode)
        print(f"   Table : {config.DestinationTable}")
        print(f"   Colonnes : {len(columns)}")
        print(f"   PK : {config.PrimaryKeyCols}")
        
        # 2. Extraction Progress → Parquet
        print("\n📊 Étape 2/5 : Extraction depuis Progress")
        extract_start = datetime.now()
        parquet_path = extract_to_parquet(table_name, where_clause=where_clause, page_size=50000)
        extract_duration = (datetime.now() - extract_start).total_seconds()
        logger.log_step(table_name, "extract", "success", duration=extract_duration)
        
        # 3. Transformation Parquet
        print("\n🔧 Étape 3/5 : Transformation données")
        transform_start = datetime.now()
        transformed_path = transform_from_parquet(config)
        transform_duration = (datetime.now() - transform_start).total_seconds()
        logger.log_step(table_name, "transform", "success", duration=transform_duration)
        
        # 4. Chargement Staging
        print("\n📥 Étape 4/5 : Chargement staging")
        ensure_stg_table(table_name, config.PrimaryKeyCols)
        load_start = datetime.now()
        rows_loaded = load_staging_from_parquet(table_name)
        load_duration = (datetime.now() - load_start).total_seconds()
        logger.log_step(table_name, "load_staging", "success", rows=rows_loaded, duration=load_duration)
        
        # 5. Merge ODS
        print("\n🔄 Étape 5/5 : Merge vers ODS")
        ensure_ods_table(config.DestinationTable, table_name, config.PrimaryKeyCols)
        merge_start = datetime.now()
        rows_merged = merge_to_ods(
            config.DestinationTable,
            table_name,
            config.PrimaryKeyCols,
            columns,
            mode
        )
        merge_duration = (datetime.now() - merge_start).total_seconds()
        logger.log_step(table_name, "merge_ods", "success", duration=merge_duration)
        
        # 6. Mise à jour timestamp succès
        if mode == "incremental":
            update_last_success(table_name)
        
        # 7. Résumé final
        total_duration = (datetime.now() - start_time).total_seconds()
        logger.log_step(table_name, "flow_complete", "success", rows=rows_loaded, duration=total_duration)
        
        print("\n" + "=" * 60)
        print(f"✅ ETL terminé avec succès")
        print(f"   Lignes traitées : {rows_loaded:,}")
        print(f"   Lignes mergées : {rows_merged:,}")
        print(f"   Durée totale : {total_duration:.1f}s")
        print(f"   - Extract : {extract_duration:.1f}s")
        print(f"   - Transform : {transform_duration:.1f}s")
        print(f"   - Load staging : {load_duration:.1f}s")
        print(f"   - Merge ODS : {merge_duration:.1f}s")
        print("=" * 60)
        
        return {
            "status": "success",
            "rows_loaded": rows_loaded,
            "rows_merged": rows_merged,
            "duration": total_duration
        }
        
    except Exception as e:
        error_duration = (datetime.now() - start_time).total_seconds()
        logger.log_step(table_name, "flow_complete", "failed", error=str(e), duration=error_duration)
        logger.create_alert(table_name, "failure", "critical", f"ETL échoué: {str(e)}")
        
        print("\n" + "=" * 60)
        print(f"❌ ETL échoué après {error_duration:.1f}s")
        print(f"   Erreur : {str(e)}")
        print("=" * 60)
        
        raise

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("=" * 60)
        print("Usage: python load_flow.py <table_name> [full|incremental]")
        print("=" * 60)
        print("\nExemples:")
        print("  python load_flow.py produit full")
        print("  python load_flow.py client incremental")
        print("  python load_flow.py commande")
        print("\nMode par défaut : incremental")
        print("=" * 60)
        sys.exit(1)
    
    table_name = sys.argv[1]
    mode = sys.argv[2] if len(sys.argv) > 2 else "incremental"
    
    # Validation mode
    if mode not in ["full", "incremental"]:
        print(f"❌ Mode invalide : {mode}")
        print("   Modes valides : full, incremental")
        sys.exit(1)
    
    load_flow(table_name, mode)