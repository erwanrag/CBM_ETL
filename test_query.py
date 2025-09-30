import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")

from tasks_simple.config_tasks import get_table_config, get_included_columns, build_query

# Test
table_name = "produit"
config = get_table_config(table_name)
columns = get_included_columns(table_name)
query = build_query(config, columns, mode="full")

print("=" * 80)
print("REQUÊTE GÉNÉRÉE:")
print("=" * 80)
print(query[:500])
print("...")
print("=" * 80)

# Vérifier les guillemets
if '"gencod-v"' in query and '"mult-fou"' in query:
    print("✅ CORRECT : Les colonnes avec tirets sont protégées par des guillemets")
else:
    print("❌ ERREUR : Les colonnes avec tirets ne sont PAS protégées")
    if 'gencod-v' in query:
        print("   Trouvé 'gencod-v' sans guillemets")
    if 'mult-fou' in query:
        print("   Trouvé 'mult-fou' sans guillemets")