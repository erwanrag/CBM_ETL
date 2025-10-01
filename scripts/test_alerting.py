import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")

from utils.alerting import Alerter

def test_alerting():
    """Test des alertes Teams"""
    
    alerter = Alerter()
    
    print("="*60)
    print("Test alerting Teams")
    print("="*60)
    
    # Test 1 : Info
    print("\n1. Test alerte INFO")
    result = alerter.send_alert(
        subject="Test ETL - Alerte Info",
        message="Ceci est un test d'alerte de niveau informatif.",
        severity='info',
        details={'Test': 'OK', 'Environnement': 'DEV'}
    )
    print(f"   Résultat : {'✅ OK' if result else '❌ Échec'}")
    
    # Test 2 : Warning
    print("\n2. Test alerte WARNING")
    result = alerter.send_alert(
        subject="Test ETL - Alerte Warning",
        message="Ceci est un test d'alerte de niveau avertissement.",
        severity='warning',
        details=['Table en retard', 'Performance dégradée', 'Cache plein']
    )
    print(f"   Résultat : {'✅ OK' if result else '❌ Échec'}")
    
    # Test 3 : Critical
    print("\n3. Test alerte CRITICAL")
    alerter.alert_etl_failure(
        table_name='test_table',
        error_message='Ceci est une erreur de test pour validation du système d\'alerting.'
    )
    
    print("\n" + "="*60)
    print("Tests terminés. Vérifiez votre canal Teams.")
    print("="*60)

if __name__ == "__main__":
    test_alerting()