from utils.parquet_cache import load_from_cache, save_to_cache
from utils.data_cleaning import normalize_dataframe, add_technical_columns

def transform_from_parquet(config):
    """Charge Parquet → transforme → sauvegarde Parquet enrichi"""
    df = load_from_cache(config.TableName, "raw")
    df = normalize_dataframe(df)
    df = add_technical_columns(df, config)
    
    print(f"Transformation : {len(df.columns)} colonnes, {len(df):,} lignes")
    
    return save_to_cache(df, config.TableName, "transformed")