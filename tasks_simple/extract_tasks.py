import pandas as pd
from utils.connections import get_progress_connection
from utils.parquet_cache import save_to_cache

def extract_to_parquet(query: str, table_name: str, page_size: int = 50000):
    """Extrait Progress par pages → sauvegarde Parquet"""
    conn = get_progress_connection()
    cursor = conn.cursor()
    
    # Compter sans sous-requête - extraire la table de FROM
    # Exemple: "SELECT ... FROM PUB.produit WHERE ..." -> "PUB.produit"
    try:
        from_index = query.upper().find(" FROM ")
        where_index = query.upper().find(" WHERE ")
        
        if where_index > 0:
            table_part = query[from_index + 6:where_index].strip()
            where_part = query[where_index + 7:].strip()
            count_query = f"SELECT COUNT(*) FROM {table_part} WHERE {where_part}"
        else:
            table_part = query[from_index + 6:].strip()
            count_query = f"SELECT COUNT(*) FROM {table_part}"
        
        cursor.execute(count_query)
        total_rows = cursor.fetchone()[0]
        print(f"Total a extraire : {total_rows:,} lignes")
    except Exception as e:
        print(f"Impossible de compter, extraction directe : {e}")
        total_rows = None
    
    if total_rows == 0:
        cursor.close()
        conn.close()
        return save_to_cache(pd.DataFrame(), table_name, "raw")
    
    # Extraire par pages
    all_data = []
    offset = 0
    page = 1
    
    while True:
        paginated_query = f"{query} OFFSET {offset} ROWS FETCH NEXT {page_size} ROWS ONLY"
        
        if total_rows:
            print(f"  Page {page}: lignes {offset:,} a {min(offset + page_size, total_rows):,}")
        else:
            print(f"  Page {page}: lignes {offset:,}+")
        
        df_page = pd.read_sql(paginated_query, conn)
        
        if df_page.empty:
            break
        
        all_data.append(df_page)
        offset += page_size
        page += 1
        
        if total_rows and offset >= total_rows:
            break
    
    cursor.close()
    conn.close()
    
    if not all_data:
        return save_to_cache(pd.DataFrame(), table_name, "raw")
    
    df_final = pd.concat(all_data, ignore_index=True)
    df_final.columns = [col.replace("-", "_") for col in df_final.columns]
    
    print(f"Extraction terminee : {len(df_final):,} lignes en {page-1} pages")
    
    return save_to_cache(df_final, table_name, "raw")
