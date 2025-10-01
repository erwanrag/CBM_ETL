from prefect import task
import pyodbc
import pandas as pd
from sqlalchemy import text
from datetime import datetime
from src.utils.connections import get_sqlserver_connection, get_sql_engine
from src.utils.type_mapping import map_progress_to_sql

@task
def ensure_ods_table(destination_table: str, table_name: str, primary_keys: str):
    """Crée la table ODS si elle n'existe pas"""
    conn = get_sqlserver_connection()
    cursor = conn.cursor()
    schema, table = destination_table.split(".")

    cursor.execute("""
        SELECT COUNT(*)
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
    """, (schema, table))
    exists = cursor.fetchone()[0]

    if exists == 0:
        cols = pd.read_sql("""
            SELECT ColumnName, DataType, Width, Scale, NullFlag
            FROM meta.ProginovColumns
            WHERE TableName = ?
        """, conn, params=[table_name])

        pk_list = [pk.strip() for pk in primary_keys.split(",")]
        col_defs = []
        for _, row in cols.iterrows():
            col_def = map_progress_to_sql(row)
            if row["ColumnName"] in pk_list:
                col_def = col_def.replace(" NULL", " NOT NULL")
            col_defs.append(col_def)

        col_defs += [
            "[hashdiff] NVARCHAR(40) NOT NULL",
            "[ts_source] DATETIME2 NULL",
            "[load_ts] DATETIME2 NOT NULL"
        ]

        create_sql = f"CREATE TABLE {destination_table} ({', '.join(col_defs)});"
        cursor.execute(create_sql)
        
        if primary_keys:
            pk_name = f"PK_{table}"
            pk_cols = ", ".join([f"[{pk.strip()}]" for pk in primary_keys.split(",")])
            cursor.execute(f"ALTER TABLE {destination_table} ADD CONSTRAINT {pk_name} PRIMARY KEY ({pk_cols});")
        
        conn.commit()
        print(f"✅ Table {destination_table} créée")

    cursor.close()
    conn.close()

@task
def merge_to_ods(destination_table: str, table_name: str, primary_keys: str, columns: list, mode: str):
    """Effectue l'upsert des données vers la table ODS"""
    engine = get_sql_engine()
    
    with engine.begin() as conn:
        if mode == "full":
            conn.execute(text(f"TRUNCATE TABLE {destination_table}"))
            print(f"Table {destination_table} vidée (mode FULL)")

        insert_cols = columns + ["hashdiff", "ts_source", "load_ts"]
        pk_list = [pk.strip() for pk in primary_keys.split(',')]

        merge_sql = f"""
        MERGE {destination_table} AS tgt
        USING stg.{table_name} AS src
        ON {" AND ".join([f"tgt.[{pk}] = src.[{pk}]" for pk in pk_list])}
        WHEN MATCHED AND tgt.hashdiff <> src.hashdiff THEN
            UPDATE SET {", ".join([f"tgt.[{col}] = src.[{col}]" for col in insert_cols])}
        WHEN NOT MATCHED THEN
            INSERT ({",".join([f"[{col}]" for col in insert_cols])})
            VALUES ({",".join([f"src.[{col}]" for col in insert_cols])});
        """
        
        result = conn.execute(text(merge_sql))
        rows_affected = result.rowcount
        print(f"✅ MERGE terminé - {rows_affected:,} lignes affectées")
    
    return rows_affected

@task
def update_last_success(table_name: str):
    """Met à jour le timestamp de dernier succès"""
    conn = get_sqlserver_connection()
    cursor = conn.cursor()
    now = datetime.now()
    cursor.execute("UPDATE config.ETL_Tables SET LastSuccessTs = ? WHERE TableName = ?", (now, table_name))
    conn.commit()
    cursor.close()
    conn.close()
    print(f"✅ LastSuccessTs mis à jour pour {table_name}")