import pyodbc
import pandas as pd
import numpy as np
from utils.connections import get_sqlserver_connection
from utils.parquet_cache import load_from_cache

def load_staging_from_parquet(table_name: str):
    """Charge Parquet → stg.table via pyodbc"""
    df = load_from_cache(table_name, "transformed")
    
    conn = get_sqlserver_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute(f"""
            SELECT 
                c.COLUMN_NAME,
                c.DATA_TYPE,
                c.CHARACTER_MAXIMUM_LENGTH
            FROM INFORMATION_SCHEMA.COLUMNS c
            WHERE c.TABLE_SCHEMA = 'stg' AND c.TABLE_NAME = ?
            ORDER BY c.ORDINAL_POSITION
        """, (table_name,))
        
        col_info = {row[0]: {'type': row[1], 'max_len': row[2]} for row in cursor.fetchall()}
        df_to_load = df[[col for col in col_info.keys() if col in df.columns]].copy()
        
        for col in df_to_load.columns:
            sql_type = col_info[col]['type']
            max_len = col_info[col]['max_len']
            
            if sql_type == 'bit':
                if df_to_load[col].dtype == 'bool':
                    df_to_load[col] = df_to_load[col].astype(int)
                df_to_load[col] = df_to_load[col].fillna(0).astype(int)
            
            elif sql_type in ['int', 'bigint']:
                df_to_load[col] = df_to_load[col].where(pd.notna(df_to_load[col]), None)
                df_to_load[col] = df_to_load[col].apply(lambda x: int(x) if pd.notna(x) else None)
            
            elif sql_type in ['decimal', 'numeric']:
                df_to_load[col] = df_to_load[col].where(pd.notna(df_to_load[col]), None)
            
            elif sql_type in ['varchar', 'nvarchar']:
                df_to_load[col] = df_to_load[col].astype(str)
                df_to_load[col] = df_to_load[col].replace({'nan': None, 'None': None, '<NA>': None})
                
                if max_len and max_len > 0:
                    df_to_load[col] = df_to_load[col].str[:max_len]
            
            elif sql_type in ['date', 'datetime2']:
                df_to_load[col] = pd.to_datetime(df_to_load[col], errors='coerce')
                df_to_load[col] = df_to_load[col].where(pd.notna(df_to_load[col]), None)
        
        df_to_load = df_to_load.replace({pd.NaT: None, np.nan: None})
        
        cursor.execute(f"TRUNCATE TABLE stg.{table_name}")
        conn.commit()
        
        cols = ",".join([f"[{c}]" for c in df_to_load.columns])
        placeholders = ",".join(["?"] * len(df_to_load.columns))
        sql = f"INSERT INTO stg.{table_name} ({cols}) VALUES ({placeholders})"
        
        cursor.fast_executemany = True
        batch_size = 1000
        total_rows = len(df_to_load)
        
        for i in range(0, total_rows, batch_size):
            batch = df_to_load.iloc[i:i+batch_size]
            cursor.executemany(sql, batch.values.tolist())
            conn.commit()
            
            if (i + batch_size) % 5000 == 0 or i + batch_size >= total_rows:
                print(f"  {min(i + batch_size, total_rows):,}/{total_rows:,} lignes")
        
        print(f"Chargement termine : {total_rows:,} lignes dans stg.{table_name}")
        return total_rows
        
    finally:
        cursor.close()
        conn.close()