import sys
import os
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent.parent / \".env\")

# Forcer le rechargement du module
if 'tasks_simple.config_tasks' in sys.modules:
    del sys.modules['tasks_simple.config_tasks']

from src.tasks.config_tasks import get_table_config, get_included_columns, build_query

# Test avec produit
print("=" * 80)
print("TEST DE LA REQUÊTE GÉNÉRÉE")
print("=" * 80)

table_name = "produit"
config = get_table_config(table_name)
columns = get_included_columns(table_name)
query = build_query(config, columns, mode="full")

print(f"\n📊 Table : {table_name}")
print(f"📋 Nombre de colonnes : {len(columns)}")
print(f"\n🔍 Colonnes avec tirets détectées :")
cols_with_dash = [c for c in columns if "-" in c]
print(f"   {cols_with_dash}")

print(f"\n📝 REQUÊTE GÉNÉRÉE (premiers 800 caractères) :")
print("-" * 80)
print(query[:800])
print("...")
print("-" * 80)

# Vérification critique
print("\n🔎 VÉRIFICATION :")
errors = []

for col in cols_with_dash:
    quoted = f'"{col}"'
    unquoted = col
    
    if quoted in query:
        print(f"   ✅ {col} → trouvé comme {quoted}")
    elif unquoted in query and quoted not in query:
        print(f"   ❌ {col} → trouvé SANS guillemets !")
        errors.append(col)
    else:
        print(f"   ⚠️  {col} → non trouvé dans la requête")

if errors:
    print(f"\n❌ ERREUR : {len(errors)} colonne(s) sans guillemets")
    print(f"   Le module tasks_simple.config_tasks n'est pas à jour !")
else:
    print(f"\n✅ TOUTES les colonnes avec tirets sont correctement protégées")
    print(f"   Vous pouvez lancer l'ETL")

print("=" * 80)