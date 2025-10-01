from prefect import flow, task
import pandas as pd
import pyodbc
import os
from datetime import datetime

# ------------------------------
# Connexions
# ------------------------------
PROGRESS_DSN = "DSN=CBM gcow0918;UID=ODBCREADER;PWD=0200101eqMOQ"
SQLSERVER_CONN = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost;"       # 🔧 adapte si besoin
    "DATABASE=CBM_ETL;"
    "Trusted_Connection=yes;"
)

# ------------------------------
# Extraction Progress
# ------------------------------
@task
def extract_sample(table_name: str, table_owner: str = "PUB", sample_size: int = 50000):
    # Connexion SQL Server pour lire la config
    sql_conn = pyodbc.connect(SQLSERVER_CONN)
    query_conf = """
    SELECT FilterClause
    FROM config.ETL_Tables
    WHERE TableName = ? AND TableOwner = ?
    """
    df_conf = pd.read_sql(query_conf, sql_conn, params=[table_name, table_owner])
    filter_clause = df_conf.iloc[0]["FilterClause"] if not df_conf.empty else None

    # Construire la requête Progress
    base_query = f"SELECT * FROM {table_owner}.{table_name}"
    if filter_clause and str(filter_clause).strip().lower() != "nan":
        base_query += f" WHERE {filter_clause}"

    base_query += f" FETCH FIRST {sample_size} ROWS ONLY"

    print(f"Query Progress (profiling): {base_query}")

    # Exécution
    conn = pyodbc.connect(PROGRESS_DSN)
    df = pd.read_sql_query(base_query, conn)

    # Tirage aléatoire dans Pandas
    if len(df) > sample_size:
        df = df.sample(n=sample_size, random_state=42).reset_index(drop=True)
        print(f"{sample_size} lignes tirées aléatoirement depuis {table_owner}.{table_name}")
    else:
        print(f"{len(df)} lignes chargées (moins que la cible) depuis {table_owner}.{table_name}")

    return df



# ------------------------------
# Profiling colonnes
# ------------------------------
@task
def profile_dataframe(df: pd.DataFrame, table_name: str):
    profiling = pd.DataFrame({
        "ColumnName": df.columns,
        "TotalRows": len(df)
    })
    profiling["EmptyRows"] = [
        df[col].isna().sum() + (df[col].astype(str).str.strip() == "").sum()
        for col in df.columns
    ]
    profiling["EmptyPct"] = (profiling["EmptyRows"] / profiling["TotalRows"]) * 100
    profiling["DistinctCount"] = [df[col].nunique(dropna=True) for col in df.columns]
    profiling["IsStatic"] = profiling["DistinctCount"].apply(lambda x: x <= 1)

    output_dir = r"D:\SQLServer\CBM_ETL\ETL\profiling"
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, f"profiling_{table_name}.csv")
    profiling.to_csv(output_file, index=False, encoding="utf-8")
    print(f"Profiling exporté : {output_file}")
    return profiling

# ------------------------------
# Synchro & mise à jour exclusions
# ------------------------------
@task
def sync_and_update_exclusions(table_name: str, table_owner: str, profiling: pd.DataFrame):
    conn = pyodbc.connect(SQLSERVER_CONN)
    cursor = conn.cursor()

    # Vérifier si la table existe déjà dans config.ETL_Columns
    cursor.execute("""
        SELECT COUNT(*) 
        FROM config.ETL_Columns
        WHERE TableOwner = ? AND TableName = ?
    """, (table_owner, table_name))
    exists = cursor.fetchone()[0]

    if exists == 0:
        print(f"Table {table_owner}.{table_name} non trouvée → exécution de SyncConfigColumns")
        cursor.execute("EXEC etl.SyncConfigColumns")
        conn.commit()

    # Parcourir les colonnes profilées
    for _, row in profiling.iterrows():
        col = row["ColumnName"]
        empty_pct = row["EmptyPct"]
        is_static = row["IsStatic"]

        if empty_pct > 90 or is_static:
            cursor.execute("""
                UPDATE config.ETL_Columns
                SET IsExcluded = 1,
                    Notes = 'Auto-profiling: vide ou statique'
                WHERE TableOwner = ? AND TableName = ? AND ColumnName = ?
            """, (table_owner, table_name, col))
            print(f"❌ Exclusion automatique : {col}")

    conn.commit()
    print(f"Exclusions mises à jour dans config.ETL_Columns pour {table_name}")

# ------------------------------
# Flow principal Prefect
# ------------------------------
@flow(name="Profiling Flow Manuel")
def profiling_flow(table_name: str, table_owner: str = "PUB"):
    df = extract_sample(table_name, table_owner)
    profiling = profile_dataframe(df, table_name)
    sync_and_update_exclusions(table_name, table_owner, profiling)

# ------------------------------
# Exécution en CLI
# ------------------------------
if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python profiling_flow.py <TableName>")
        sys.exit(1)

    table_name = sys.argv[1]
    profiling_flow(table_name)
