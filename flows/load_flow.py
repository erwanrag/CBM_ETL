from prefect import flow, task
import pandas as pd
import pyodbc
from datetime import datetime, timedelta
import hashlib
import sys
import os
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from etl_logger import ETLLogger

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")

# ------------------------------
# Connexions
# ------------------------------
PROGRESS_DSN = f"DSN={os.getenv('PROGRESS_DSN')};UID={os.getenv('PROGRESS_USER')};PWD={os.getenv('PROGRESS_PWD')}"
SQLSERVER_CONN = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    f"SERVER={os.getenv('SQL_SERVER')};"
    f"DATABASE={os.getenv('SQL_DATABASE')};"
    "Trusted_Connection=yes;"
)

# ------------------------------
# Utilitaires
# ------------------------------
def get_sql_type(col):
    """Convertit un type Progress vers un type SQL Server"""
    dtype = str(col["DataType"]).lower()
    width = None
    if not pd.isna(col["Width"]) and str(col["Width"]).strip() != "":
        width = int(float(col["Width"]))
    scale = None
    if not pd.isna(col["Scale"]) and str(col["Scale"]).strip() != "":
        scale = int(float(col["Scale"]))

    if dtype == "varchar":
        return f"NVARCHAR({width})" if width and width < 4000 else "NVARCHAR(MAX)"
    elif dtype == "integer":
        return "INT"
    elif dtype == "bigint":
        return "BIGINT"
    elif dtype == "bit":
        return "BIT"
    elif dtype == "numeric":
        precision = width if width and width > 0 else 18
        scale_val = scale if scale is not None else 0
        if scale_val > precision:
            scale_val = precision
        if precision > 38:
            precision = 38
            scale_val = min(scale_val, 38)
        return f"DECIMAL({precision},{scale_val})"
    elif dtype == "date":
        return "DATE"
    elif dtype == "datetime":
        return "DATETIME2"
    else:
        return "NVARCHAR(MAX)"

def map_progress_to_sql(col):
    """Retourne la définition SQL complète pour un CREATE TABLE"""
    sql_type = get_sql_type(col)
    nullflag = "NULL" if str(col["NullFlag"]).upper() == "Y" else "NOT NULL"
    return f"[{col['ColumnName']}] {sql_type} {nullflag}"

# ------------------------------
# Configuration
# ------------------------------
@task
def get_table_config(table_name: str):
    """Récupère la configuration de la table depuis ETL_Tables"""
    conn = pyodbc.connect(SQLSERVER_CONN)
    query = """
    SELECT TableName, DestinationTable, PrimaryKeyCols,
           HasTimestamps, DateCreaCol, DateModifCol,
           FilterClause, LastSuccessTs,
           DateModifPrecision, LookbackInterval
    FROM config.ETL_Tables
    WHERE TableName = ?
    """
    df = pd.read_sql(query, conn, params=[table_name])
    conn.close()
    if df.empty:
        raise ValueError(f"Table {table_name} non trouvée dans ETL_Tables")
    return df.iloc[0]

@task
def get_included_columns(table_name: str):
    """Récupère la liste des colonnes à inclure"""
    conn = pyodbc.connect(SQLSERVER_CONN)
    query = """
    SELECT ColumnName
    FROM config.ETL_Columns
    WHERE TableName = ? AND IsExcluded = 0
    """
    df = pd.read_sql(query, conn, params=[table_name])
    conn.close()
    if df.empty:
        raise ValueError(f"Aucune colonne valide trouvée pour {table_name}")
    return df["ColumnName"].tolist()

# ------------------------------
# Extraction
# ------------------------------
@task
def build_query(config, columns, mode="incremental"):
    """Construit la requête Progress avec les filtres appropriés"""
    select_cols = ", ".join([f'"{c}"' for c in columns])
    query = f"SELECT {select_cols} FROM PUB.{config.TableName}"
    where_clauses = []

    if config.FilterClause and str(config.FilterClause).strip() != "nan":
        where_clauses.append(config.FilterClause)

    if mode == "incremental" and config.HasTimestamps:
        last_ts = config.LastSuccessTs
        if not pd.isna(last_ts):
            lookback = config.LookbackInterval or "0d"
            unit = lookback[-1]
            value = int(lookback[:-1])
            delta = {"d": timedelta(days=value), "h": timedelta(hours=value), "m": timedelta(minutes=value)}[unit]
            start_ts = last_ts - delta
            if config.DateModifPrecision == "date":
                start_ts_str = start_ts.strftime("%Y-%m-%d")
            else:
                start_ts_str = start_ts.strftime("%Y-%m-%d %H:%M:%S")
            where_clauses.append(f'"{config.DateModifCol}" >= \'{start_ts_str}\'')

    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)

    return query

@task
def extract_progress_paginated(query: str, page_size: int = 50000):
    """Extrait les données depuis Progress par pages pour éviter les timeouts"""
    conn = pyodbc.connect(PROGRESS_DSN)
    
    # Compter le total
    count_query = f"SELECT COUNT(*) FROM ({query}) AS total"
    cursor = conn.cursor()
    cursor.execute(count_query)
    total_rows = cursor.fetchone()[0]
    print(f"Total à extraire : {total_rows:,} lignes")
    
    if total_rows == 0:
        conn.close()
        return pd.DataFrame()
    
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
    
    conn.close()
    df_final = pd.concat(all_data, ignore_index=True)
    print(f"✅ {len(df_final):,} lignes extraites en {page-1} pages")
    return df_final

# ------------------------------
# Transformation
# ------------------------------
@task
def normalize_and_enrich(df: pd.DataFrame, config):
    """Normalise les données et ajoute les colonnes techniques"""
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            continue
        if pd.api.types.is_numeric_dtype(df[col]):
            continue
        df[col] = df[col].astype(str).str.strip()

    def compute_hash(row):
        concat = "|".join([str(v) for v in row.values])
        return hashlib.sha1(concat.encode("utf-8")).hexdigest()

    df["hashdiff"] = df.apply(compute_hash, axis=1)

    if config.HasTimestamps and config.DateModifCol in df.columns:
        df["ts_source"] = pd.to_datetime(df[config.DateModifCol], errors="coerce")
    else:
        df["ts_source"] = pd.NaT
    
    if df["ts_source"].dtype == 'object':
        df["ts_source"] = pd.to_datetime(df["ts_source"], errors="coerce")

    current_time = datetime.utcnow()
    df["load_ts"] = pd.to_datetime([current_time] * len(df))
    
    print(f"Données enrichies : {len(df.columns)} colonnes, {len(df):,} lignes")
    return df

# ------------------------------
# Staging
# ------------------------------
@task
def ensure_stg_table(table_name: str, primary_keys: str):
    """Crée la table staging si elle n'existe pas"""
    conn = pyodbc.connect(SQLSERVER_CONN)
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

@task
def load_to_staging(df: pd.DataFrame, table_name: str):
    """Charge les données dans la table staging par batches"""
    df_stg = df.copy()

    # Conversion types
    for col in df_stg.columns:
        if col in ['ts_source', 'load_ts']:
            if df_stg[col].dtype == 'object':
                df_stg[col] = pd.to_datetime(df_stg[col], errors='coerce')
        elif pd.api.types.is_bool_dtype(df_stg[col]):
            df_stg[col] = df_stg[col].astype('bool').astype('int')
        elif pd.api.types.is_integer_dtype(df_stg[col]):
            df_stg[col] = df_stg[col].astype('Int64')
        elif pd.api.types.is_float_dtype(df_stg[col]):
            df_stg[col] = df_stg[col].astype('float64')
        else:
            df_stg[col] = df_stg[col].astype(str).str.strip().replace({"nan": None, "None": None, "": None})
    
    df_stg = df_stg.where(pd.notnull(df_stg), None)
    
    for col in df_stg.columns:
        if df_stg[col].dtype == 'object':
            continue
        elif 'int' in str(df_stg[col].dtype).lower():
            df_stg[col] = df_stg[col].map(lambda x: int(x) if pd.notnull(x) else None)
        elif 'float' in str(df_stg[col].dtype).lower():
            df_stg[col] = df_stg[col].map(lambda x: float(x) if pd.notnull(x) else None)
    
    conn = pyodbc.connect(SQLSERVER_CONN)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = 'stg' AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
    """, (table_name,))
    
    db_columns = [row[0] for row in cursor.fetchall()]
    available_columns = [col for col in db_columns if col in df_stg.columns]
    df_stg = df_stg[available_columns]
    
    # Conversion colonnes BIT
    bit_cols = pd.read_sql(f"""
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'stg' AND TABLE_NAME = ? AND DATA_TYPE = 'bit'
    """, conn, params=[table_name])

    for col_name in bit_cols['COLUMN_NAME'].tolist():
        if col_name in df_stg.columns:
            df_stg[col_name] = df_stg[col_name].astype(str).str.lower()
            df_stg[col_name] = df_stg[col_name].replace({'true': 1, 'false': 0, '1': 1, '0': 0, 'none': None, 'nan': None})
            df_stg[col_name] = pd.to_numeric(df_stg[col_name], errors='coerce').fillna(0).astype(int)

    # Truncation colonnes texte
    col_sizes = pd.read_sql(f"""
        SELECT COLUMN_NAME, CHARACTER_MAXIMUM_LENGTH
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'stg' AND TABLE_NAME = ?
        AND DATA_TYPE LIKE '%char%' AND CHARACTER_MAXIMUM_LENGTH > 0
    """, conn, params=[table_name])
    
    for _, row in col_sizes.iterrows():
        col_name = row['COLUMN_NAME']
        max_len = int(row['CHARACTER_MAXIMUM_LENGTH'])
        if col_name in df_stg.columns:
            df_stg[col_name] = df_stg[col_name].astype(str).str[:max_len].replace({'None': None, 'nan': None})
    
    # Vider et charger
    cursor.execute(f"TRUNCATE TABLE stg.{table_name}")
    conn.commit()

    cols = ",".join([f"[{c}]" for c in df_stg.columns])
    placeholders = ",".join(["?"] * len(df_stg.columns))
    sql = f"INSERT INTO stg.{table_name} ({cols}) VALUES ({placeholders})"
    
    cursor.fast_executemany = True
    batch_size = 10000
    total_rows = len(df_stg)
    rows_inserted = 0

    try:
        for i in range(0, total_rows, batch_size):
            batch = df_stg.iloc[i:i+batch_size]
            cursor.executemany(sql, batch.values.tolist())
            conn.commit()
            rows_inserted += len(batch)
            
            if (i // batch_size + 1) % 5 == 0 or rows_inserted == total_rows:
                print(f"  {rows_inserted:,}/{total_rows:,} lignes ({(rows_inserted/total_rows)*100:.1f}%)")

        print(f"✅ {total_rows:,} lignes insérées dans stg.{table_name}")
    except Exception as e:
        print(f"❌ Erreur : {e}")
        raise
    finally:
        cursor.close()
        conn.close()

# ------------------------------
# ODS
# ------------------------------
@task
def ensure_ods_table(destination_table: str, table_name: str, primary_keys: str):
    """Crée la table ODS si elle n'existe pas"""
    conn = pyodbc.connect(SQLSERVER_CONN)
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
def upsert_to_ods(destination_table: str, table_name: str, primary_keys: str, all_columns: list, mode: str):
    """Effectue l'upsert des données vers la table ODS"""
    conn = pyodbc.connect(SQLSERVER_CONN)
    cursor = conn.cursor()

    if mode == "full":
        cursor.execute(f"TRUNCATE TABLE {destination_table}")
        print(f"Table {destination_table} vidée (mode FULL)")

    insert_cols = all_columns + ["hashdiff", "ts_source", "load_ts"]
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
    
    cursor.execute(merge_sql)
    rows_affected = cursor.rowcount
    conn.commit()
    print(f"✅ MERGE terminé - {rows_affected:,} lignes affectées")
    cursor.close()
    conn.close()

@task
def update_last_success(table_name: str):
    """Met à jour le timestamp de dernier succès"""
    conn = pyodbc.connect(SQLSERVER_CONN)
    cursor = conn.cursor()
    now = datetime.now()
    cursor.execute("UPDATE config.ETL_Tables SET LastSuccessTs = ? WHERE TableName = ?", (now, table_name))
    conn.commit()
    cursor.close()
    conn.close()

# ------------------------------
# Flow principal
# ------------------------------
@flow(name="Flow de Chargement ETL", retries=2, retry_delay_seconds=60)
def load_flow(table_name: str, mode: str = "incremental"):
    """Flow principal de chargement ETL"""
    logger = ETLLogger(SQLSERVER_CONN)
    start_time = datetime.now()
    
    print(f"🚀 ETL {table_name} ({mode}) - Run ID: {logger.run_id}")
    
    try:
        logger.log_step(table_name, "flow_start", "started")
        
        config = get_table_config(table_name)
        columns = get_included_columns(table_name)
        
        query = build_query(config, columns, mode=mode)
        extract_start = datetime.now()
        df = extract_progress_paginated(query, page_size=50000)
        logger.log_step(table_name, "extract", "success", rows=len(df), duration=(datetime.now()-extract_start).total_seconds())
        
        transform_start = datetime.now()
        df = normalize_and_enrich(df, config)
        logger.log_step(table_name, "transform", "success", rows=len(df), duration=(datetime.now()-transform_start).total_seconds())
        
        ensure_stg_table(table_name, config.PrimaryKeyCols)
        load_start = datetime.now()
        load_to_staging(df, table_name)
        logger.log_step(table_name, "load_staging", "success", rows=len(df), duration=(datetime.now()-load_start).total_seconds())
        
        ensure_ods_table(config.DestinationTable, table_name, config.PrimaryKeyCols)
        merge_start = datetime.now()
        upsert_to_ods(config.DestinationTable, table_name, config.PrimaryKeyCols, columns, mode)
        logger.log_step(table_name, "merge_ods", "success", duration=(datetime.now()-merge_start).total_seconds())
        
        if mode == "incremental":
            update_last_success(table_name)
        
        total_duration = (datetime.now() - start_time).total_seconds()
        logger.log_step(table_name, "flow_complete", "success", rows=len(df), duration=total_duration)
        print(f"🎉 Terminé en {total_duration:.1f}s")
        
    except Exception as e:
        error_duration = (datetime.now() - start_time).total_seconds()
        logger.log_step(table_name, "flow_complete", "failed", error=str(e), duration=error_duration)
        logger.create_alert(table_name, "failure", "critical", f"ETL échoué: {str(e)}")
        raise

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python load_flow.py <Table> [full|incremental]")
        sys.exit(1)
    table_name = sys.argv[1]
    mode = sys.argv[2] if len(sys.argv) > 2 else "incremental"
    load_flow(table_name, mode)