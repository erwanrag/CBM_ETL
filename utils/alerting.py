import os
import requests
import json
from datetime import datetime

class TeamsAlerter:
    """Envoi d'alertes vers Microsoft Teams via Power Automate"""
    
    def __init__(self):
        self.webhook_url = os.getenv('TEAMS_WEBHOOK_URL')
    
    def send_alert(self, subject, message, severity='warning', details=None):
        """
        Envoie une alerte Teams via Power Automate
        
        Args:
            subject: Titre de la carte
            message: Message principal
            severity: 'critical', 'warning', 'info'
            details: Dict ou liste avec détails
        """
        if not self.webhook_url:
            print("Teams webhook non configuré")
            return False
        
        icon = {
            'critical': '🔴',
            'warning': '⚠️',
            'info': 'ℹ️'
        }.get(severity, '📊')
        
        # Construire le message texte complet
        full_message = f"{icon} **{subject}**\n\n{message}\n\n"
        
        # Ajouter détails
        if details:
            full_message += "**Détails:**\n"
            
            if isinstance(details, dict):
                for key, value in details.items():
                    full_message += f"- **{key}:** {value}\n"
            elif isinstance(details, list):
                for item in details:
                    full_message += f"- {item}\n"
        
        # Footer
        full_message += f"\n---\n"
        full_message += f"*{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*\n"
        full_message += f"*Serveur: {os.getenv('SQL_SERVER', 'localhost')}*"
        
        # Payload simple pour Power Automate
        payload = {
            "text": full_message
        }
        
        try:
            response = requests.post(
                self.webhook_url,
                headers={'Content-Type': 'application/json'},
                data=json.dumps(payload),
                timeout=10
            )
            
            if response.status_code in [200, 202]:
                print(f"✅ Alerte Teams envoyée : {subject}")
                return True
            else:
                print(f"❌ Erreur Teams : {response.status_code}")
                print(f"   Response: {response.text}")
                return False
                
        except Exception as e:
            print(f"❌ Erreur envoi Teams : {e}")
            return False


class Alerter:
    """Gestionnaire d'alertes"""
    
    def __init__(self):
        self.teams = TeamsAlerter()
    
    def send_alert(self, subject, message, severity='warning', details=None):
        """Envoie alerte sur Teams"""
        return self.teams.send_alert(subject, message, severity, details)
    
    def alert_etl_failure(self, table_name, error_message):
        """Alerte échec ETL"""
        self.send_alert(
            subject=f"Échec ETL : {table_name}",
            message=f"Le chargement de la table {table_name} a échoué.",
            severity='critical',
            details={
                'Table': table_name,
                'Erreur': error_message[:200],
                'Action': 'Vérifier les logs SQL et relancer manuellement'
            }
        )
    
    def alert_sla_breach(self, tables):
        """Alerte dépassement SLA"""
        if not tables:
            return
        
        self.send_alert(
            subject=f"SLA Breach : {len(tables)} table(s) en retard",
            message=f"{len(tables)} table(s) n'ont pas été chargées dans les délais.",
            severity='warning',
            details=[f"{t['TableName']} : {t['HoursSinceSuccess']}h" for t in tables]
        )
    
    def alert_daily_summary(self, stats):
        """Résumé quotidien ETL"""
        self.send_alert(
            subject="Résumé ETL quotidien",
            message=f"ETL exécuté avec succès : {stats['success_rate']:.1f}%",
            severity='info' if stats['success_rate'] > 90 else 'warning',
            details={
                'Tables traitées': stats['tables_processed'],
                'Lignes chargées': f"{stats['rows_loaded']:,}",
                'Échecs': stats['failures'],
                'Durée totale': f"{stats['duration_min']:.1f} min"
            }
        )