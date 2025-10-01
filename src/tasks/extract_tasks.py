from prefect import task
import pandas as pd
from src.utils.connections import get_progress_connection
from src.utils.parquet_cache import save_to_cache
from src.tasks.config_tasks import get_table_columns

@task
def extract_to_parquet(table_name: str, where_clause: str = "", page_size: int = 50000):
    """
    Extrait Progress → applique alias SQL-safe → sauvegarde en Parquet.

    Args:
        table_name: Nom de la table (ex: "produit")
        where_clause: Optionnel, clause WHERE sans le mot-clé (ex: "cod_pro LIKE 'A%'")
        page_size: Non utilisé (future pagination possible)
    """
    conn = get_progress_connection()

    # Récupérer la liste des colonnes depuis config.ETL_Columns
    config_columns = get_table_columns(table_name)
    cols_expr = [row["SourceExpression"] for _, row in config_columns.iterrows() if row["IsExcluded"] == 0]

    if not cols_expr:
        raise ValueError(f"Aucune colonne valide trouvée pour {table_name}")

    # Construire la requête SQL Progress
    query = f'SELECT {", ".join(cols_expr)} FROM PUB.{table_name}'
    if where_clause:
        query += f" WHERE {where_clause}"

    print(f"🔎 Extraction avec alias explicites :")
    print(f"   {query[:150]}...")

    try:
        df_final = pd.read_sql(query, conn)
        print(f"✅ Extraction terminée : {len(df_final):,} lignes, {len(df_final.columns)} colonnes")
    except Exception as e:
        print(f"❌ Erreur extraction : {e}")
        conn.close()
        raise

    conn.close()

    # Appliquer les noms SQL-safe (SqlName)
    sql_names = [row["SqlName"] for _, row in config_columns.iterrows() if row["IsExcluded"] == 0]
    df_final.columns = sql_names

    # Sauvegarde en Parquet
    return save_to_cache(df_final, table_name, "raw")