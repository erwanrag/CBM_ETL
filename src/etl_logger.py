import pyodbc
import uuid
from datetime import datetime
from typing import Optional

class ETLLogger:
    """Gère le logging structuré des flows ETL"""
    
    def __init__(self, conn_string: str):
        self.conn_string = conn_string
        self.run_id = str(uuid.uuid4())
    
    def log_step(
        self,
        table_name: str,
        step_name: str,
        status: str,
        rows: Optional[int] = None,
        error: Optional[str] = None,
        duration: Optional[float] = None
    ):
        """
        Log une étape ETL
        
        Args:
            table_name: Nom de la table
            step_name: Nom de l'étape (extract, transform, load, merge)
            status: 'started', 'success', 'failed'
            rows: Nombre de lignes traitées
            error: Message d'erreur si échec
            duration: Durée en secondes
        """
        try:
            conn = pyodbc.connect(self.conn_string)
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO etl.ETL_Log 
                (RunId, TableName, StepName, Status, RowsProcessed, ErrorMessage, DurationSeconds)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (self.run_id, table_name, step_name, status, rows, error, duration))
            
            conn.commit()
            cursor.close()
            conn.close()
        except Exception as e:
            print(f"⚠️ Erreur lors du logging : {e}")
    
    def create_alert(
        self,
        table_name: str,
        alert_type: str,
        severity: str,
        message: str
    ):
        """
        Crée une alerte
        
        Args:
            table_name: Nom de la table
            alert_type: 'failure', 'latency', 'data_quality', 'schema_change'
            severity: 'critical', 'warning', 'info'
            message: Description de l'alerte
        """
        try:
            conn = pyodbc.connect(self.conn_string)
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO etl.ETL_Alerts (TableName, AlertType, Severity, Message)
                VALUES (?, ?, ?, ?)
            """, (table_name, alert_type, severity, message))
            
            conn.commit()
            cursor.close()
            conn.close()
        except Exception as e:
            print(f"⚠️ Erreur lors de la création d'alerte : {e}")