"""Configuration pytest - Setup PYTHONPATH"""
import sys
from pathlib import Path

# Ajouter le dossier racine au PYTHONPATH
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

print(f"✅ PYTHONPATH configuré: {project_root}")