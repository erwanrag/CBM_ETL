"""Configuration pytest - Setup PYTHONPATH + .env"""
import sys
import os
from pathlib import Path
from dotenv import load_dotenv

# Ajouter racine au PYTHONPATH
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# ✅ FORCER chargement .env AVANT tous les tests
env_file = project_root / ".env"
if env_file.exists():
    load_dotenv(env_file, override=True)
    print(f"✅ .env chargé: {env_file}")
    
    # Vérifier variables critiques
    sql_server = os.getenv('SQL_SERVER')
    sql_db = os.getenv('SQL_DATABASE')
    
    if sql_server and sql_db:
        print(f"✅ SQL_SERVER={sql_server}, SQL_DATABASE={sql_db}")
    else:
        print(f"⚠️  Variables SQL manquantes dans .env")
else:
    print(f"❌ .env introuvable: {env_file}")

import pytest

@pytest.fixture(scope="session", autouse=True)
def load_env():
    """Charge .env automatiquement avant tous les tests"""
    # Déjà fait ci-dessus, mais on peut recharger si besoin
    pass