"""
Système de monitoring avancé pour ETL
Métriques, SLI/SLO, Distributed Tracing
"""
import time
import functools
from datetime import datetime, timedelta
from typing import Dict, Optional, Any, Callable
from dataclasses import dataclass, field
import json
from collections import defaultdict

@dataclass
class Metric:
    """Métrique de performance"""
    name: str
    value: float
    unit: str
    timestamp: datetime = field(default_factory=datetime.now)
    tags: Dict[str, str] = field(default_factory=dict)
    
    def to_dict(self) -> dict:
        return {
            'name': self.name,
            'value': self.value,
            'unit': self.unit,
            'timestamp': self.timestamp.isoformat(),
            'tags': self.tags
        }


class MetricsCollector:
    """Collecteur de métriques pour dashboard"""
    
    def __init__(self):
        self.metrics: list[Metric] = []
        self.counters: Dict[str, float] = defaultdict(float)
        self.gauges: Dict[str, float] = {}
        self.histograms: Dict[str, list] = defaultdict(list)
    
    def counter(self, name: str, value: float = 1, tags: Dict = None):
        """Incrémente un compteur"""
        key = f"{name}_{tags}" if tags else name
        self.counters[key] += value
        
        self.metrics.append(Metric(
            name=name,
            value=value,
            unit='count',
            tags=tags or {}
        ))
    
    def gauge(self, name: str, value: float, tags: Dict = None):
        """Enregistre une valeur instantanée"""
        key = f"{name}_{tags}" if tags else name
        self.gauges[key] = value
        
        self.metrics.append(Metric(
            name=name,
            value=value,
            unit='gauge',
            tags=tags or {}
        ))
    
    def histogram(self, name: str, value: float, tags: Dict = None):
        """Enregistre une valeur dans un histogramme"""
        key = f"{name}_{tags}" if tags else name
        self.histograms[key].append(value)
        
        self.metrics.append(Metric(
            name=name,
            value=value,
            unit='histogram',
            tags=tags or {}
        ))
    
    def timing(self, name: str, duration_seconds: float, tags: Dict = None):
        """Enregistre une durée"""
        self.histogram(name, duration_seconds, tags)
    
    def get_percentile(self, name: str, percentile: float) -> Optional[float]:
        """Calcule percentile d'un histogramme"""
        if name not in self.histograms or not self.histograms[name]:
            return None
        
        import numpy as np
        return np.percentile(self.histograms[name], percentile)
    
    def get_summary(self) -> dict:
        """Résumé de toutes les métriques"""
        import numpy as np
        
        summary = {
            'counters': dict(self.counters),
            'gauges': dict(self.gauges),
            'histograms': {}
        }
        
        for name, values in self.histograms.items():
            if values:
                summary['histograms'][name] = {
                    'count': len(values),
                    'min': min(values),
                    'max': max(values),
                    'mean': np.mean(values),
                    'median': np.median(values),
                    'p95': np.percentile(values, 95),
                    'p99': np.percentile(values, 99)
                }
        
        return summary
    
    def export_to_sql(self, conn_string: str, batch_size: int = 1000):
        """Exporte métriques vers SQL Server"""
        import pyodbc
        
        if not self.metrics:
            return
        
        conn = pyodbc.connect(conn_string)
        cursor = conn.cursor()
        
        # Créer table si nécessaire
        cursor.execute("""
            IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES 
                          WHERE TABLE_SCHEMA = 'etl' 
                          AND TABLE_NAME = 'Metrics')
            BEGIN
                CREATE TABLE etl.Metrics (
                    MetricId BIGINT IDENTITY(1,1) PRIMARY KEY,
                    MetricName NVARCHAR(100) NOT NULL,
                    MetricValue FLOAT NOT NULL,
                    Unit NVARCHAR(20),
                    Tags NVARCHAR(MAX),
                    MetricTs DATETIME2 NOT NULL,
                    INDEX IX_Metrics_NameDate (MetricName, MetricTs DESC)
                );
            END
        """)
        
        # Insertion par batch
        for i in range(0, len(self.metrics), batch_size):
            batch = self.metrics[i:i+batch_size]
            
            cursor.executemany("""
                INSERT INTO etl.Metrics (MetricName, MetricValue, Unit, Tags, MetricTs)
                VALUES (?, ?, ?, ?, ?)
            """, [
                (m.name, m.value, m.unit, json.dumps(m.tags), m.timestamp)
                for m in batch
            ])
            
            conn.commit()
        
        cursor.close()
        conn.close()
        
        print(f"📊 {len(self.metrics)} métriques exportées vers SQL")


class PerformanceMonitor:
    """Moniteur de performance avec contexte"""
    
    def __init__(self, collector: MetricsCollector):
        self.collector = collector
        self.spans: Dict[str, 'Span'] = {}
    
    def start_span(self, name: str, tags: Dict = None) -> 'Span':
        """Démarre un span de tracing"""
        span = Span(name, tags or {}, self.collector)
        self.spans[name] = span
        return span
    
    def measure_execution_time(self, name: str, tags: Dict = None):
        """Décorateur pour mesurer temps d'exécution"""
        def decorator(func: Callable) -> Callable:
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                start = time.time()
                
                try:
                    result = func(*args, **kwargs)
                    status = 'success'
                    return result
                    
                except Exception as e:
                    status = 'error'
                    raise
                    
                finally:
                    duration = time.time() - start
                    
                    metric_tags = tags.copy() if tags else {}
                    metric_tags['function'] = func.__name__
                    metric_tags['status'] = status
                    
                    self.collector.timing(name, duration, metric_tags)
            
            return wrapper
        return decorator


@dataclass
class Span:
    """Span de distributed tracing"""
    name: str
    tags: Dict[str, str]
    collector: MetricsCollector
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None
    children: list['Span'] = field(default_factory=list)
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.finish(error=exc_type is not None)
    
    def finish(self, error: bool = False):
        """Termine le span"""
        self.end_time = time.time()
        duration = self.end_time - self.start_time
        
        tags = self.tags.copy()
        tags['error'] = str(error)
        
        self.collector.timing(f"span.{self.name}", duration, tags)
    
    def child_span(self, name: str, tags: Dict = None) -> 'Span':
        """Crée un span enfant"""
        child_tags = self.tags.copy()
        if tags:
            child_tags.update(tags)
        
        child = Span(f"{self.name}.{name}", child_tags, self.collector)
        self.children.append(child)
        return child


class SLOMonitor:
    """Moniteur SLI/SLO (Service Level Indicators/Objectives)"""
    
    def __init__(self, conn_string: str):
        self.conn_string = conn_string
        self.slos = {
            'availability': 0.999,      # 99.9% uptime
            'latency_p95': 120,         # 95% des runs < 120s
            'error_rate': 0.01,         # <1% erreur
            'data_freshness': 24        # Données <24h
        }
    
    def check_slo_compliance(self, days: int = 7) -> Dict[str, Any]:
        """Vérifie conformité SLO sur N jours"""
        import pyodbc
        import pandas as pd
        
        conn = pyodbc.connect(self.conn_string)
        
        # 1. Availability (taux de succès)
        df_availability = pd.read_sql("""
            SELECT 
                COUNT(*) as TotalRuns,
                SUM(CASE WHEN Status = 'success' THEN 1 ELSE 0 END) as SuccessRuns
            FROM etl.ETL_Log
            WHERE StepName = 'flow_complete'
              AND LogTs >= DATEADD(day, -?, GETDATE())
        """, conn, params=[days])
        
        availability = df_availability.iloc[0]['SuccessRuns'] / df_availability.iloc[0]['TotalRuns']
        
        # 2. Latency P95
        df_latency = pd.read_sql("""
            SELECT DurationSeconds
            FROM etl.ETL_Log
            WHERE StepName = 'flow_complete'
              AND Status = 'success'
              AND LogTs >= DATEADD(day, -?, GETDATE())
        """, conn, params=[days])
        
        import numpy as np
        latency_p95 = np.percentile(df_latency['DurationSeconds'], 95) if len(df_latency) > 0 else 0
        
        # 3. Error Rate
        df_errors = pd.read_sql("""
            SELECT 
                COUNT(*) as TotalRuns,
                SUM(CASE WHEN Status = 'failed' THEN 1 ELSE 0 END) as FailedRuns
            FROM etl.ETL_Log
            WHERE StepName = 'flow_complete'
              AND LogTs >= DATEADD(day, -?, GETDATE())
        """, conn, params=[days])
        
        error_rate = df_errors.iloc[0]['FailedRuns'] / df_errors.iloc[0]['TotalRuns']
        
        # 4. Data Freshness
        df_freshness = pd.read_sql("""
            SELECT 
                TableName,
                DATEDIFF(hour, LastSuccessTs, GETDATE()) as HoursSinceSuccess
            FROM config.ETL_Tables
            WHERE HasTimestamps = 1
        """, conn)
        
        max_freshness = df_freshness['HoursSinceSuccess'].max() if len(df_freshness) > 0 else 0
        
        conn.close()
        
        # Résultats
        results = {
            'period_days': days,
            'timestamp': datetime.now().isoformat(),
            'slos': {
                'availability': {
                    'target': self.slos['availability'],
                    'actual': availability,
                    'compliant': availability >= self.slos['availability'],
                    'error_budget_remaining': (availability - self.slos['availability']) / (1 - self.slos['availability'])
                },
                'latency_p95': {
                    'target': self.slos['latency_p95'],
                    'actual': latency_p95,
                    'compliant': latency_p95 <= self.slos['latency_p95'],
                    'unit': 'seconds'
                },
                'error_rate': {
                    'target': self.slos['error_rate'],
                    'actual': error_rate,
                    'compliant': error_rate <= self.slos['error_rate']
                },
                'data_freshness': {
                    'target': self.slos['data_freshness'],
                    'actual': max_freshness,
                    'compliant': max_freshness <= self.slos['data_freshness'],
                    'unit': 'hours'
                }
            }
        }
        
        return results
    
    def print_slo_report(self, results: Dict):
        """Affiche rapport SLO"""
        print("\n" + "="*80)
        print(f"📊 RAPPORT SLO - Période: {results['period_days']} jours")
        print("="*80)
        
        for slo_name, slo_data in results['slos'].items():
            icon = "✅" if slo_data['compliant'] else "❌"
            
            print(f"\n{icon} {slo_name.upper()}")
            print(f"   Target  : {slo_data['target']}")
            print(f"   Actual  : {slo_data['actual']:.4f}")
            
            if 'error_budget_remaining' in slo_data:
                budget = slo_data['error_budget_remaining'] * 100
                print(f"   Budget  : {budget:.1f}% restant")
        
        print("\n" + "="*80)
        
        all_compliant = all(s['compliant'] for s in results['slos'].values())
        
        if all_compliant:
            print("✅ Tous les SLO sont respectés")
        else:
            violated = [name for name, data in results['slos'].items() if not data['compliant']]
            print(f"❌ SLO violés: {', '.join(violated)}")
        
        print("="*80)


# ========== EXEMPLE D'UTILISATION ==========

def example_monitoring():
    """Exemple complet de monitoring"""
    
    # Setup
    collector = MetricsCollector()
    monitor = PerformanceMonitor(collector)
    
    # Simuler un flow ETL
    with monitor.start_span('etl_flow', {'table': 'produit'}) as flow_span:
        
        # Extract
        with flow_span.child_span('extract') as extract_span:
            time.sleep(0.5)  # Simuler extraction
            collector.counter('rows_extracted', 10000, {'table': 'produit'})
            collector.gauge('extract_throughput', 20000, {'unit': 'rows/s'})
        
        # Transform
        with flow_span.child_span('transform') as transform_span:
            time.sleep(0.3)
            collector.counter('rows_transformed', 10000, {'table': 'produit'})
        
        # Load
        with flow_span.child_span('load') as load_span:
            time.sleep(0.7)
            collector.counter('rows_loaded', 9999, {'table': 'produit'})
            collector.counter('rows_rejected', 1, {'table': 'produit', 'reason': 'invalid_pk'})
    
    # Afficher résumé
    summary = collector.get_summary()
    print(json.dumps(summary, indent=2))
    
    return collector