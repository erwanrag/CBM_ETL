import pandas as pd
import hashlib
from datetime import datetime

def normalize_dataframe(df: pd.DataFrame):
    """
    Normalise les données : trim strings, préserve types numériques/dates
    
    Args:
        df: DataFrame à normaliser
    
    Returns:
        pd.DataFrame: DataFrame normalisé
    """
    df = df.copy()
    
    for col in df.columns:
        # Ignorer datetime et numériques
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            continue
        if pd.api.types.is_numeric_dtype(df[col]):
            continue
        
        # Trim strings
        df[col] = df[col].astype(str).str.strip()
    
    return df

def compute_hashdiff(df: pd.DataFrame):
    """
    Calcule le hash SHA1 de chaque ligne (pour détection changements)
    
    Args:
        df: DataFrame source
    
    Returns:
        pd.Series: Hash de chaque ligne
    """
    def hash_row(row):
        concat = "|".join([str(v) for v in row.values])
        return hashlib.sha1(concat.encode("utf-8")).hexdigest()
    
    return df.apply(hash_row, axis=1)

def add_technical_columns(df: pd.DataFrame, config):
    """
    Ajoute les colonnes techniques : hashdiff, ts_source, load_ts
    
    Args:
        df: DataFrame source
        config: Configuration de la table (objet avec HasTimestamps, DateModifCol)
    
    Returns:
        pd.DataFrame: DataFrame enrichi
    """
    df = df.copy()
    
    # 1. Hashdiff (calculé sur données sources uniquement)
    df_for_hash = df.copy()
    df["hashdiff"] = compute_hashdiff(df_for_hash)
    
    # 2. Timestamp source (depuis colonne de modification si disponible)
    if config.HasTimestamps and config.DateModifCol in df.columns:
        df["ts_source"] = pd.to_datetime(df[config.DateModifCol], errors="coerce")
    else:
        df["ts_source"] = pd.NaT
    
    # 3. Timestamp de chargement (UTC)
    current_time = datetime.utcnow()
    df["load_ts"] = pd.to_datetime([current_time] * len(df))
    
    print(f"🔧 Colonnes techniques ajoutées : hashdiff, ts_source, load_ts")
    
    return df

def optimize_dtypes(df: pd.DataFrame):
    """
    Optimise les types de données pour réduire la mémoire
    
    Args:
        df: DataFrame source
    
    Returns:
        pd.DataFrame: DataFrame optimisé
    """
    df = df.copy()
    
    for col in df.columns:
        col_type = df[col].dtype
        
        # Optimiser les entiers
        if col_type == 'int64':
            c_min = df[col].min()
            c_max = df[col].max()
            
            if c_min > -128 and c_max < 127:
                df[col] = df[col].astype('int8')
            elif c_min > -32768 and c_max < 32767:
                df[col] = df[col].astype('int16')
            elif c_min > -2147483648 and c_max < 2147483647:
                df[col] = df[col].astype('int32')
        
        # Optimiser les flottants
        elif col_type == 'float64':
            df[col] = df[col].astype('float32')
    
    return df