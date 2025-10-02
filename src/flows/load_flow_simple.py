import sys
import os
import warnings
from pathlib import Path
from datetime import datetime

warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=FutureWarning)

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent.parent / ".env")

from src.etl_logger import ETLLogger
from src.tasks.config_tasks import get_table_config, get_included_columns, build_where_clause
from src.tasks.extract_tasks import extract_to_parquet
from src.tasks.transform_tasks import transform_from_parquet
from src.tasks.staging_config_tasks import ensure_stg_table
from src.tasks.staging_tasks import load_staging_from_parquet
from src.tasks.ods_tasks import ensure_ods_table, merge_to_ods, update_last_success
from src.utils.parquet_cache import load_from_cache  
SQLSERVER_CONN = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    f"SERVER={os.getenv('SQL_SERVER')};"
    f"DATABASE={os.getenv('SQL_DATABASE')};"
    "Trusted_Connection=yes;"
    "Connection Timeout=300;"
    "Command Timeout=600;"
)

def load_flow_simple(table_name: str, mode: str = "incremental"):
    """Flow ETL sans orchestration Prefect"""
    logger = ETLLogger(SQLSERVER_CONN)
    start_time = datetime.now()
    
    print(f"ETL {table_name} ({mode}) - Run ID: {logger.run_id}")
    print("=" * 60)
    
    try:
        logger.log_step(table_name, "flow_start", "started")
        
        print("\nEtape 1/5 : Configuration")
        config = get_table_config(table_name)
        columns = get_included_columns(table_name)
        where_clause = build_where_clause(config, mode=mode)
        print(f"   Table : {config.DestinationTable}")
        print(f"   Colonnes : {len(columns)}")
        
        print("\nEtape 2/5 : Extraction")
        extract_start = datetime.now()
        parquet_path = extract_to_parquet(table_name, where_clause=where_clause)
        logger.log_step(table_name, "extract", "success", duration=(datetime.now()-extract_start).total_seconds())
        
        print("\nEtape 3/5 : Transformation")
        transform_start = datetime.now()
        transformed_path = transform_from_parquet(config)
        logger.log_step(table_name, "transform", "success", duration=(datetime.now()-transform_start).total_seconds())
        
        print("\n🔍 Étape 3.5/5 : Data Quality")
        from src.utils.data_quality import DataQualityValidator
        import json

        # Charger config DQ
        dq_config_file = Path(__file__).parent.parent.parent / f"config/data_quality/{table_name}.json"
        if dq_config_file.exists():
            with open(dq_config_file) as f:
                dq_config = json.load(f)['checks']
            
            # Valider
            validator = DataQualityValidator(table_name)
            df_to_check = load_from_cache(table_name, "transformed")
            validator.run_all_checks(df_to_check, dq_config)
            
            # Logger résultats
            from src.utils.data_quality import DataQualityMonitor
            monitor = DataQualityMonitor(SQLSERVER_CONN)
            for result in validator.results:
                monitor.log_quality_check(result, table_name)
            
            # Bloquer si critique
            if validator.get_summary()['has_critical_failure']:
                raise Exception(f"❌ Échec Data Quality critique sur {table_name}")
            
            validator.print_report()

        print("\nEtape 4/5 : Staging")
        ensure_stg_table(table_name, config.PrimaryKeyCols)
        load_start = datetime.now()
        rows_loaded = load_staging_from_parquet(table_name)
        logger.log_step(table_name, "load_staging", "success", rows=rows_loaded, duration=(datetime.now()-load_start).total_seconds())
        
        print("\nEtape 5/5 : ODS")
        ensure_ods_table(config.DestinationTable, table_name, config.PrimaryKeyCols)
        merge_start = datetime.now()
        rows_merged = merge_to_ods(config.DestinationTable, table_name, config.PrimaryKeyCols, columns, mode)
        logger.log_step(table_name, "merge_ods", "success", duration=(datetime.now()-merge_start).total_seconds())
        
        if mode == "incremental":
            update_last_success(table_name)
        
        total_duration = (datetime.now() - start_time).total_seconds()
        logger.log_step(table_name, "flow_complete", "success", rows=rows_loaded, duration=total_duration)
        
        print("\n" + "=" * 60)
        print(f"ETL termine - {rows_loaded:,} lignes en {total_duration:.1f}s")
        print("=" * 60)
        
    except Exception as e:
        logger.log_step(table_name, "flow_complete", "failed", error=str(e))
        print(f"\nErreur : {str(e)}")
        raise

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python load_flow_simple.py <table> [full|incremental]")
        sys.exit(1)
    
    table_name = sys.argv[1]
    mode = sys.argv[2] if len(sys.argv) > 2 else "incremental"
    load_flow_simple(table_name, mode)
