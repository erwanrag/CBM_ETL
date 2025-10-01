import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from datetime import datetime, timedelta
from src.utils.parquet_cache import CACHE_DIR

def cleanup_old_cache(days=7):
    """Supprime les caches Parquet de plus de X jours"""
    
    cutoff = datetime.now() - timedelta(days=days)
    deleted = 0
    freed_mb = 0
    
    print(f"🧹 Nettoyage cache (>{days} jours)")
    print("-"*60)
    
    for file in CACHE_DIR.glob("*.parquet"):
        mtime = datetime.fromtimestamp(file.stat().st_mtime)
        
        if mtime < cutoff:
            size_mb = file.stat().st_size / (1024 * 1024)
            file.unlink()
            deleted += 1
            freed_mb += size_mb
            print(f"  Supprimé : {file.name} ({mtime.strftime('%Y-%m-%d')})")
    
    print("-"*60)
    print(f"✅ {deleted} fichier(s) supprimé(s), {freed_mb:.1f} MB libérés")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--days', type=int, default=7, 
                       help='Supprimer les fichiers de plus de X jours')
    args = parser.parse_args()
    
    cleanup_old_cache(args.days)