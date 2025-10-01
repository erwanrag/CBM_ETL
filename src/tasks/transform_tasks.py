from prefect import task
from src.utils.parquet_cache import load_from_cache, save_to_cache
from src.utils.data_cleaning import normalize_dataframe, add_technical_columns

@task
def transform_from_parquet(config):
    """Charge Parquet → transforme → sauvegarde Parquet enrichi"""
    # Charger depuis cache
    df = load_from_cache(config.TableName, "raw")
    
    # Normaliser
    df = normalize_dataframe(df)
    
    # Ajouter colonnes techniques
    df = add_technical_columns(df, config)
    
    print(f"🔧 Transformation : {len(df.columns)} colonnes, {len(df):,} lignes")
    
    # Sauvegarder version transformée
    return save_to_cache(df, config.TableName, "transformed")