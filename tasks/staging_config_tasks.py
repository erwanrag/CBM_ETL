from prefect import task
import pandas as pd
import pyodbc
from utils.connections import get_sqlserver_connection
from utils.type_mapping import map_progress_to_sql

@task
def ensure_stg_table(table_name: str, primary_keys: str):
    """Crée la table staging si elle n'existe pas"""
    conn = get_sqlserver_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT COUNT(*)
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = 'stg' AND TABLE_NAME = ?
    """, (table_name,))
    exists = cursor.fetchone()[0]

    if exists == 0:
        print(f"Création de la table stg.{table_name}...")
        cols = pd.read_sql("""
            SELECT ColumnName, DataType, Width, Scale, NullFlag
            FROM meta.ProginovColumns
            WHERE TableName = ?
        """, conn, params=[table_name])

        col_defs = [map_progress_to_sql(row) for _, row in cols.iterrows()]
        col_defs += [
            "[hashdiff] NVARCHAR(40) NOT NULL",
            "[ts_source] DATETIME2 NULL",
            "[load_ts] DATETIME2 NOT NULL"
        ]

        create_sql = f"CREATE TABLE stg.{table_name} ({', '.join(col_defs)});"
        cursor.execute(create_sql)
        
        pk_list = [pk.strip() for pk in primary_keys.split(",")]
        pk_cols = ", ".join([f"[{pk}]" for pk in pk_list])
        sql_idx = f"CREATE INDEX IX_{table_name}_PK ON stg.{table_name} ({pk_cols});"
        cursor.execute(sql_idx)
        conn.commit()
        print(f"✅ Table stg.{table_name} créée")

    cursor.close()
    conn.close()