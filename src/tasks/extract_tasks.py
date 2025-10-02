# src/tasks/extract_tasks.py
from prefect import task
import pandas as pd
from src.utils.connections import get_progress_connection
from src.utils.parquet_cache import save_to_cache
from src.tasks.config_tasks import get_table_columns
from src.utils.resilience import retry_with_backoff, timeout_decorator
import pyodbc

@task
@retry_with_backoff(
    max_attempts=3,
    initial_delay=5,
    backoff_factor=2,
    exceptions=(pyodbc.Error, pyodbc.OperationalError, ConnectionError)
)
@timeout_decorator(600)  # 10 min max
def extract_to_parquet(table_name: str, where_clause: str = "", page_size: int = 50000):
    """
    Extrait Progress → Parquet avec retry automatique
    
    Args:
        table_name: Nom table Progress
        where_clause: Filtre WHERE (sans le mot-clé)
        page_size: Taille page (non utilisé actuellement)
    
    Returns:
        str: Chemin fichier Parquet créé
    """
    print(f"🔄 Extraction {table_name} (avec retry & timeout)")
    
    conn = get_progress_connection()
    
    try:
        # Récupérer colonnes
        config_columns = get_table_columns(table_name)
        cols_expr = [
            row["SourceExpression"] 
            for _, row in config_columns.iterrows() 
            if row["IsExcluded"] == 0
        ]
        
        if not cols_expr:
            raise ValueError(f"Aucune colonne valide pour {table_name}")
        
        # Construire requête
        query = f'SELECT {", ".join(cols_expr)} FROM PUB.{table_name}'
        if where_clause:
            query += f" WHERE {where_clause}"
        
        print(f"🔎 Requête : {query[:150]}...")
        
        # Exécution avec gestion timeout automatique
        df_final = pd.read_sql(query, conn)
        print(f"✅ {len(df_final):,} lignes extraites")
        
    except pyodbc.Error as e:
        print(f"❌ Erreur ODBC Progress : {e}")
        raise  # Retry va relancer
        
    except Exception as e:
        print(f"❌ Erreur extraction : {e}")
        raise
        
    finally:
        try:
            conn.close()
        except:
            pass
    
    # Appliquer noms SQL-safe
    sql_names = [
        row["SqlName"] 
        for _, row in config_columns.iterrows() 
        if row["IsExcluded"] == 0
    ]
    df_final.columns = sql_names
    
    # Sauvegarder cache
    return save_to_cache(df_final, table_name, "raw")