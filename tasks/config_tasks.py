from prefect import task
import pandas as pd
from datetime import timedelta
from utils.connections import get_sql_engine

@task
def get_table_config(table_name: str):
    """Récupère la configuration de la table depuis ETL_Tables"""
    engine = get_sql_engine()
    query = """
    SELECT TableName, DestinationTable, PrimaryKeyCols,
           HasTimestamps, DateCreaCol, DateModifCol,
           FilterClause, LastSuccessTs,
           DateModifPrecision, LookbackInterval
    FROM config.ETL_Tables
    WHERE TableName = ?
    """
    df = pd.read_sql(query, engine, params=[table_name])
    if df.empty:
        raise ValueError(f"Table {table_name} non trouvée dans ETL_Tables")
    return df.iloc[0]

@task
def get_included_columns(table_name: str):
    """Récupère la liste des colonnes à inclure"""
    engine = get_sql_engine()
    query = """
    SELECT ColumnName
    FROM config.ETL_Columns
    WHERE TableName = ? AND IsExcluded = 0
    """
    df = pd.read_sql(query, engine, params=[table_name])
    if df.empty:
        raise ValueError(f"Aucune colonne valide trouvée pour {table_name}")
    return df["ColumnName"].tolist()

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
    