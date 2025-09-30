from prefect import task
import pandas as pd
from utils.connections import get_progress_connection
from utils.parquet_cache import save_to_cache

@task
def extract_to_parquet(query: str, table_name: str, page_size: int = 50000):
    """Extrait Progress par pages → sauvegarde Parquet"""
    conn = get_progress_connection()
    cursor = conn.cursor()
    
    # Compter total
    count_query = f"SELECT COUNT(*) FROM ({query}) AS total"
    cursor.execute(count_query)
    total_rows = cursor.fetchone()[0]
    print(f"📊 Total à extraire : {total_rows:,} lignes")
    
    if total_rows == 0:
        cursor.close()
        conn.close()
        return save_to_cache(pd.DataFrame(), table_name, "raw")
    
    # Extraire par pages
    all_data = []
    offset = 0
    page = 1
    
    while offset < total_rows:
        paginated_query = f"{query} OFFSET {offset} ROWS FETCH NEXT {page_size} ROWS ONLY"
        print(f"  Page {page}: lignes {offset:,} à {min(offset + page_size, total_rows):,}")
        df_page = pd.read_sql(paginated_query, conn)
        all_data.append(df_page)
        offset += page_size
        page += 1
    
    cursor.close()
    conn.close()
    df_final = pd.concat(all_data, ignore_index=True)
    print(f"✅ {len(df_final):,} lignes extraites en {page-1} pages")
    
    # Sauvegarder en Parquet
    return save_to_cache(df_final, table_name, "raw")