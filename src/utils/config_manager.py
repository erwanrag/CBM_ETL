"""
Configuration centralisée avec gestion des secrets
Support: .env, Azure Key Vault, Variables d'environnement
"""
import os
import json
from pathlib import Path
from typing import Any, Optional, Dict
from dataclasses import dataclass, field
from dotenv import load_dotenv

@dataclass
class DatabaseConfig:
    """Configuration base de données"""
    host: str
    database: str
    user: Optional[str] = None
    password: Optional[str] = None
    driver: str = "ODBC Driver 17 for SQL Server"
    trusted_connection: bool = True
    connection_timeout: int = 300
    command_timeout: int = 600
    
    def get_connection_string(self) -> str:
        """Génère string de connexion"""
        parts = [
            f"DRIVER={{{self.driver}}}",
            f"SERVER={self.host}",
            f"DATABASE={self.database}"
        ]
        
        if self.trusted_connection:
            parts.append("Trusted_Connection=yes")
        else:
            if self.user:
                parts.append(f"UID={self.user}")
            if self.password:
                parts.append(f"PWD={self.password}")
        
        parts.append(f"Connection Timeout={self.connection_timeout}")
        parts.append(f"Command Timeout={self.command_timeout}")
        
        return ";".join(parts)


@dataclass
class ETLConfig:
    """Configuration ETL globale"""
    # Databases
    progress_dsn: str
    progress_user: str
    progress_password: str
    sqlserver: DatabaseConfig
    
    # Performance
    batch_size: int = 1000
    max_workers: int = 4
    parquet_compression: str = "snappy"
    cache_retention_days: int = 7
    
    # Monitoring
    enable_metrics: bool = True
    enable_tracing: bool = True
    log_level: str = "INFO"
    
    # Alerting
    teams_webhook_url: Optional[str] = None
    alert_on_failure: bool = True
    alert_on_sla_breach: bool = True
    
    # SLO Targets
    slo_availability: float = 0.999
    slo_latency_p95: float = 120
    slo_error_rate: float = 0.01
    slo_data_freshness_hours: float = 24
    
    # Retry & Resilience
    max_retries: int = 3
    retry_backoff_factor: float = 2.0
    circuit_breaker_threshold: int = 5
    circuit_breaker_timeout: int = 60
    
    # Data Quality
    enable_data_quality: bool = True
    dq_fail_on_critical: bool = True
    dq_min_fill_rate: float = 0.95
    
    # Features toggles
    enable_parallel_execution: bool = False
    enable_incremental_by_default: bool = True
    enable_auto_schema_evolution: bool = False


class ConfigManager:
    """Gestionnaire de configuration centralisé"""
    
    def __init__(self, env_file: Optional[Path] = None):
        self.env_file = env_file or Path(__file__).parent.parent.parent / ".env"
        self.config: Optional[ETLConfig] = None
        self._secrets_backend = None  # Azure Key Vault si configuré
    
    def load(self) -> ETLConfig:
        """Charge configuration depuis sources multiples"""
        
        # 1. Charger .env
        if self.env_file.exists():
            load_dotenv(self.env_file)
            print(f"✅ Configuration chargée depuis {self.env_file}")
        else:
            print(f"⚠️  Fichier {self.env_file} introuvable - utilisation variables d'environnement")
        
        # 2. Tenter Azure Key Vault si configuré
        key_vault_url = os.getenv('AZURE_KEY_VAULT_URL')
        if key_vault_url:
            try:
                self._load_from_key_vault(key_vault_url)
            except Exception as e:
                print(f"⚠️  Échec Azure Key Vault: {e}")
        
        # 3. Construire config
        self.config = ETLConfig(
            # Progress
            progress_dsn=self._get_secret('PROGRESS_DSN', required=True),
            progress_user=self._get_secret('PROGRESS_USER', required=True),
            progress_password=self._get_secret('PROGRESS_PWD', required=True),
            
            # SQL Server
            sqlserver=DatabaseConfig(
                host=self._get_secret('SQL_SERVER', default='localhost'),
                database=self._get_secret('SQL_DATABASE', default='CBM_ETL'),
                user=self._get_secret('SQL_USER'),
                password=self._get_secret('SQL_PASSWORD'),
                trusted_connection=self._get_bool('SQL_TRUSTED_CONNECTION', default=True)
            ),
            
            # Performance
            batch_size=self._get_int('ETL_BATCH_SIZE', default=1000),
            max_workers=self._get_int('ETL_MAX_WORKERS', default=4),
            cache_retention_days=self._get_int('CACHE_RETENTION_DAYS', default=7),
            
            # Monitoring
            enable_metrics=self._get_bool('ENABLE_METRICS', default=True),
            enable_tracing=self._get_bool('ENABLE_TRACING', default=True),
            log_level=self._get_secret('LOG_LEVEL', default='INFO'),
            
            # Alerting
            teams_webhook_url=self._get_secret('TEAMS_WEBHOOK_URL'),
            alert_on_failure=self._get_bool('ALERT_ON_FAILURE', default=True),
            
            # SLO
            slo_availability=self._get_float('SLO_AVAILABILITY', default=0.999),
            slo_latency_p95=self._get_float('SLO_LATENCY_P95', default=120),
            
            # Retry
            max_retries=self._get_int('MAX_RETRIES', default=3),
            
            # Data Quality
            enable_data_quality=self._get_bool('ENABLE_DATA_QUALITY', default=True),
            dq_fail_on_critical=self._get_bool('DQ_FAIL_ON_CRITICAL', default=True),
            
            # Features
            enable_parallel_execution=self._get_bool('ENABLE_PARALLEL', default=False),
            enable_incremental_by_default=self._get_bool('ENABLE_INCREMENTAL_DEFAULT', default=True)
        )
        
        return self.config
    
    def _get_secret(self, key: str, default: Any = None, required: bool = False) -> Any:
        """Récupère secret depuis env ou Key Vault"""
        
        # 1. Tenter Key Vault si disponible
        if self._secrets_backend:
            try:
                value = self._secrets_backend.get_secret(key)
                if value:
                    return value
            except Exception:
                pass
        
        # 2. Variables d'environnement
        value = os.getenv(key, default)
        
        if required and value is None:
            raise ValueError(f"Configuration requise manquante: {key}")
        
        return value
    
    def _get_int(self, key: str, default: int) -> int:
        """Récupère entier"""
        value = self._get_secret(key, default=str(default))
        try:
            return int(value)
        except (ValueError, TypeError):
            return default
    
    def _get_float(self, key: str, default: float) -> float:
        """Récupère float"""
        value = self._get_secret(key, default=str(default))
        try:
            return float(value)
        except (ValueError, TypeError):
            return default
    
    def _get_bool(self, key: str, default: bool) -> bool:
        """Récupère booléen"""
        value = self._get_secret(key, default=str(default))
        
        if isinstance(value, bool):
            return value
        
        if isinstance(value, str):
            return value.lower() in ('true', '1', 'yes', 'on')