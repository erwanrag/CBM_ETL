import pandas as pd

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
        # CHANGEMENT : plafonner à 500 caractères max
        max_varchar = min(width, 500) if width else 255
        return f"NVARCHAR({max_varchar})"
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
        return "NVARCHAR(500)"  # Par défaut 500 aussi

def map_progress_to_sql(col):
    """Retourne la définition SQL complète pour un CREATE TABLE"""
    sql_type = get_sql_type(col)
    nullflag = "NULL" if str(col["NullFlag"]).upper() == "Y" else "NOT NULL"
    
    # Normaliser le nom de colonne
    clean_name = col['ColumnName'].replace("-", "_")
    
    return f"[{clean_name}] {sql_type} {nullflag}"