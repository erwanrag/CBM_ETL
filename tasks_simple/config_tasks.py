import pandas as pd
from datetime import timedelta
from sqlalchemy import text
from utils.connections import get_sql_engine


def get_table_columns(table_name: str):
    """Récupère toutes les colonnes de config.ETL_Columns avec SourceExpression et SqlName"""
    engine = get_sql_engine()
    query = text("""
    SELECT ColumnName, SqlName, SourceExpression, IsExcluded
    FROM config.ETL_Columns
    WHERE TableName = :table_name
    ORDER BY ColumnName
    """)
    
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={'table_name': table_name})
    
    if df.empty:
        raise ValueError(f"Aucune colonne trouvée pour {table_name}")
    return df

def get_table_config(table_name: str):
    """Récupère la configuration de la table depuis ETL_Tables"""
    engine = get_sql_engine()
    query = text("""
    SELECT TableName, DestinationTable, PrimaryKeyCols,
           HasTimestamps, DateCreaCol, DateModifCol,
           FilterClause, LastSuccessTs,
           DateModifPrecision, LookbackInterval
    FROM config.ETL_Tables
    WHERE TableName = :table_name
    """)
    
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={'table_name': table_name})
    
    if df.empty:
        raise ValueError(f"Table {table_name} non trouvée dans ETL_Tables")
    return df.iloc[0]

def get_included_columns(table_name: str):
    """Récupère la liste des colonnes à inclure (noms ORIGINAUX avec tirets)"""
    engine = get_sql_engine()
    query = text("""
    SELECT ColumnName
    FROM config.ETL_Columns
    WHERE TableName = :table_name AND IsExcluded = 0
    ORDER BY ColumnName
    """)
    
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={'table_name': table_name})
    
    if df.empty:
        raise ValueError(f"Aucune colonne valide trouvée pour {table_name}")
    
    # Retourner les noms ORIGINAUX (avec tirets si présents)
    return df["ColumnName"].tolist()

def build_query(config, columns, mode="incremental"):
    """Construit la requête Progress avec guillemets pour colonnes à tirets"""
    
    def quote_column(col):
        """Ajoute des guillemets doubles si la colonne contient un tiret"""
        if "-" in col:
            return f'"{col}"'
        return col
    
    # SELECT avec guillemets pour protéger les colonnes à tirets
    select_cols = ", ".join([quote_column(c) for c in columns])
    query = f"SELECT {select_cols} FROM PUB.{config.TableName}"
    
    where_clauses = []

    # Filtre métier
    if config.FilterClause and str(config.FilterClause).strip() != "nan":
        where_clauses.append(config.FilterClause)

    # Filtre incrémental
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
            
            # IMPORTANT : protéger le nom de colonne avec guillemets si tiret
            date_col = config.DateModifCol
            if "-" in date_col:
                where_clauses.append(f'"{date_col}" >= \'{start_ts_str}\'')
            else:
                where_clauses.append(f'{date_col} >= \'{start_ts_str}\'')

    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)

    return query

def build_where_clause(config, mode="incremental"):
    """Construit uniquement la clause WHERE (filtre métier + incrémental)"""
    where_clauses = []

    # Filtre métier
    if config.FilterClause and str(config.FilterClause).strip() != "nan":
        where_clauses.append(config.FilterClause)

    # Filtre incrémental
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

            date_col = config.DateModifCol
            if "-" in date_col:
                where_clauses.append(f'"{date_col}" >= \'{start_ts_str}\'')
            else:
                where_clauses.append(f'{date_col} >= \'{start_ts_str}\'')

    if where_clauses:
        return " AND ".join(where_clauses)
    return ""
