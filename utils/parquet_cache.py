import os
from pathlib import Path
import pandas as pd

# Chemin du cache (relatif au projet)
CACHE_DIR = Path(__file__).parent.parent / "cache" / "parquet"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

def get_cache_path(table_name: str, stage: str = "raw"):
    """
    Retourne le chemin du fichier Parquet
    
    Args:
        table_name: Nom de la table
        stage: 'raw' (après extract) ou 'transformed' (après transform)
    """
    return CACHE_DIR / f"{table_name}_{stage}.parquet"

def save_to_cache(df: pd.DataFrame, table_name: str, stage: str = "raw"):
    """
    Sauvegarde DataFrame en Parquet avec compression
    
    Args:
        df: DataFrame à sauvegarder
        table_name: Nom de la table
        stage: 'raw' ou 'transformed'
    
    Returns:
        str: Chemin du fichier créé
    """
    path = get_cache_path(table_name, stage)
    
    df.to_parquet(
        path,
        engine='pyarrow',
        compression='snappy',
        index=False
    )
    
    # Calcul taille fichier
    size_mb = path.stat().st_size / (1024 * 1024)
    print(f"💾 Cache Parquet : {path.name} ({len(df):,} lignes, {size_mb:.1f} MB)")
    
    return str(path)

def load_from_cache(table_name: str, stage: str = "raw"):
    """
    Charge DataFrame depuis Parquet
    
    Args:
        table_name: Nom de la table
        stage: 'raw' ou 'transformed'
    
    Returns:
        pd.DataFrame: Données chargées
    """
    path = get_cache_path(table_name, stage)
    
    if not path.exists():
        raise FileNotFoundError(f"Cache introuvable : {path}")
    
    df = pd.read_parquet(path, engine='pyarrow')
    size_mb = path.stat().st_size / (1024 * 1024)
    print(f"📂 Cache chargé : {path.name} ({len(df):,} lignes, {size_mb:.1f} MB)")
    
    return df

def cache_exists(table_name: str, stage: str = "raw"):
    """Vérifie si un cache existe"""
    path = get_cache_path(table_name, stage)
    return path.exists()

def clear_cache(table_name: str = None):
    """
    Supprime les fichiers Parquet
    
    Args:
        table_name: Si spécifié, supprime uniquement cette table.
                   Sinon, supprime tout le cache.
    """
    if table_name:
        for stage in ['raw', 'transformed']:
            path = get_cache_path(table_name, stage)
            if path.exists():
                path.unlink()
                print(f"🗑️ Cache supprimé : {path.name}")
    else:
        count = 0
        for file in CACHE_DIR.glob("*.parquet"):
            file.unlink()
            count += 1
        print(f"🗑️ {count} fichier(s) cache supprimé(s)")

def get_cache_info():
    """Retourne des informations sur le cache"""
    files = list(CACHE_DIR.glob("*.parquet"))
    
    if not files:
        print("ℹ️ Aucun fichier cache")
        return
    
    total_size = sum(f.stat().st_size for f in files)
    
    print(f"\n📊 Cache Parquet ({len(files)} fichier(s), {total_size/(1024*1024):.1f} MB)")
    print("─" * 60)
    
    for file in sorted(files):
        size_mb = file.stat().st_size / (1024 * 1024)
        print(f"  {file.name:<40} {size_mb:>8.1f} MB")