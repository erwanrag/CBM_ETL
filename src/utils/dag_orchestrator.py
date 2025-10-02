"""
Orchestrateur DAG avec gestion de dépendances
Remplace l'orchestrateur simple par un système intelligent
"""
import sys
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Set, Optional
from dataclasses import dataclass, field
from collections import defaultdict, deque
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent.parent / ".env")

from src.flows.load_flow_simple import load_flow_simple
from src.utils.connections import get_sqlserver_connection
from src.utils.alerting import Alerter
from src.utils.monitoring import MetricsCollector, PerformanceMonitor

@dataclass
class TableNode:
    """Nœud représentant une table dans le DAG"""
    name: str
    priority: str  # 'critical', 'high', 'normal'
    dependencies: List[str] = field(default_factory=list)
    status: str = 'pending'  # 'pending', 'running', 'success', 'failed', 'skipped'
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    error: Optional[str] = None
    retry_count: int = 0
    max_retries: int = 2
    
    @property
    def duration(self) -> Optional[float]:
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None
    
    def can_run(self, completed_tables: Set[str]) -> bool:
        """Vérifie si toutes les dépendances sont complètes"""
        return all(dep in completed_tables for dep in self.dependencies)


class DAGOrchestrator:
    """
    Orchestrateur DAG intelligent avec:
    - Résolution automatique des dépendances
    - Parallélisation sur plusieurs niveaux
    - Retry intelligent
    - Skip des tables en échec avec propagation
    """
    
    def __init__(self, mode: str = "incremental", stop_on_critical: bool = True):
        self.mode = mode
        self.stop_on_critical = stop_on_critical
        self.nodes: Dict[str, TableNode] = {}
        self.alerter = Alerter()
        self.collector = MetricsCollector()
        self.monitor = PerformanceMonitor(self.collector)
    
    def load_config(self):
        """Charge la configuration depuis SQL Server"""
        conn = get_sqlserver_connection()
        
        # Charger tables avec métadonnées
        df = pd.read_sql("""
            SELECT 
                TableName,
                IsDimension,
                IsFact,
                Notes,
                CASE 
                    WHEN Notes LIKE '%critical%' OR Notes LIKE '%critique%' THEN 'critical'
                    WHEN IsDimension = 1 THEN 'high'
                    WHEN IsFact = 1 THEN 'high'
                    ELSE 'normal'
                END AS Priority
            FROM config.ETL_Tables
            WHERE IsActive = 1  -- Ajouter cette colonne si nécessaire
            ORDER BY Priority, TableName
        """, conn)
        
        # Charger dépendances (si table existe)
        try:
            df_deps = pd.read_sql("""
                SELECT TableName, DependsOn
                FROM config.ETL_Dependencies
                WHERE IsActive = 1
            """, conn)
            
            dependencies = defaultdict(list)
            for _, row in df_deps.iterrows():
                dependencies[row['TableName']].append(row['DependsOn'])
        except Exception:
            # Table dependencies n'existe pas encore
            dependencies = defaultdict(list)
            print("⚠️  Table config.ETL_Dependencies non trouvée - dépendances ignorées")
        
        conn.close()
        
        # Créer les nœuds
        for _, row in df.iterrows():
            table_name = row['TableName']
            self.nodes[table_name] = TableNode(
                name=table_name,
                priority=row['Priority'],
                dependencies=dependencies.get(table_name, [])
            )
        
        print(f"📋 Chargé {len(self.nodes)} tables avec dépendances")
        self._validate_dag()
    
    def _validate_dag(self):
        """Valide le DAG (pas de cycle, dépendances existantes)"""
        # Vérifier que toutes les dépendances existent
        all_tables = set(self.nodes.keys())
        
        for node in self.nodes.values():
            for dep in node.dependencies:
                if dep not in all_tables:
                    print(f"⚠️  {node.name}: dépendance manquante '{dep}'")
                    node.dependencies.remove(dep)
        
        # Détecter cycles
        if self._has_cycle():
            raise ValueError("❌ Cycle détecté dans le DAG de dépendances!")
    
    def _has_cycle(self) -> bool:
        """Détecte les cycles avec DFS"""
        visited = set()
        rec_stack = set()
        
        def visit(node_name: str) -> bool:
            visited.add(node_name)
            rec_stack.add(node_name)
            
            for dep in self.nodes[node_name].dependencies:
                if dep not in visited:
                    if visit(dep):
                        return True
                elif dep in rec_stack:
                    return True
            
            rec_stack.remove(node_name)
            return False
        
        for node_name in self.nodes:
            if node_name not in visited:
                if visit(node_name):
                    return True
        
        return False
    
    def get_execution_levels(self) -> List[List[str]]:
        """
        Calcule les niveaux d'exécution (topological sort)
        Retourne liste de listes : chaque sous-liste peut s'exécuter en parallèle
        """
        in_degree = {name: len(node.dependencies) for name, node in self.nodes.items()}
        levels = []
        
        while in_degree:
            # Trouver tous les nœuds sans dépendances restantes
            current_level = [name for name, degree in in_degree.items() if degree == 0]
            
            if not current_level:
                raise ValueError("Impossible de résoudre dépendances (cycle détecté)")
            
            levels.append(current_level)
            
            # Retirer ce niveau et mettre à jour les degrés
            for name in current_level:
                del in_degree[name]
                
                # Réduire le degré des tables qui dépendaient de celle-ci
                for other_name, node in self.nodes.items():
                    if other_name in in_degree and name in node.dependencies:
                        in_degree[other_name] -= 1
        
        return levels
    
    def execute_node(self, node: TableNode) -> bool:
        """Exécute le chargement d'une table avec retry"""
        node.status = 'running'
        node.start_time = datetime.now()
        
        print(f"\n{'='*80}")
        print(f"🔄 {node.name} (priorité: {node.priority.upper()})")
        
        if node.dependencies:
            print(f"   Dépendances: {', '.join(node.dependencies)}")
        
        print(f"{'='*80}")
        
        while node.retry_count <= node.max_retries:
            try:
                with self.monitor.start_span('table_load', {'table': node.name}) as span:
                    load_flow_simple(node.name, mode=self.mode)
                
                node.status = 'success'
                node.end_time = datetime.now()
                
                self.collector.counter('table_success', 1, {'table': node.name})
                self.collector.timing(
                    'table_duration', 
                    node.duration, 
                    {'table': node.name, 'status': 'success'}
                )
                
                print(f"✅ {node.name} terminé ({node.duration:.1f}s)")
                return True
                
            except Exception as e:
                node.retry_count += 1
                error_msg = str(e)
                
                if node.retry_count > node.max_retries:
                    node.status = 'failed'
                    node.end_time = datetime.now()
                    node.error = error_msg[:500]
                    
                    self.collector.counter('table_failed', 1, {'table': node.name})
                    
                    print(f"❌ {node.name} échoué après {node.max_retries} tentatives")
                    print(f"   Erreur: {error_msg[:200]}")
                    
                    # Alerte immédiate si critique
                    if node.priority == 'critical':
                        self.alerter.alert_etl_failure(node.name, error_msg)
                    
                    return False
                else:
                    print(f"⚠️  Tentative {node.retry_count}/{node.max_retries} échouée - retry...")
                    import time
                    time.sleep(2 ** node.retry_count)  # Backoff exponentiel
        
        return False
    
    def execute(self) -> Dict[str, Any]:
        """Exécute le DAG complet"""
        start_global = datetime.now()
        
        print("\n" + "="*80)
        print(f"🚀 DAG ORCHESTRATOR (mode: {self.mode})")
        print(f"   Démarrage: {start_global.strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*80)
        
        # Charger config
        self.load_config()
        
        # Calculer ordre d'exécution
        levels = self.get_execution_levels()
        
        print(f"\n📊 PLAN D'EXÉCUTION")
        print("-"*80)
        print(f"Tables total : {len(self.nodes)}")
        print(f"Niveaux parallélisation : {len(levels)}")
        
        for i, level in enumerate(levels, 1):
            priority_counts = defaultdict(int)
            for table in level:
                priority_counts[self.nodes[table].priority] += 1
            
            print(f"\nNiveau {i}: {len(level)} table(s) - {dict(priority_counts)}")
            print(f"  {', '.join(level[:10])}" + (" ..." if len(level) > 10 else ""))
        
        print("-"*80)
        
        # Exécution niveau par niveau
        completed = set()
        failed = set()
        skipped = set()
        
        for level_num, level_tables in enumerate(levels, 1):
            print(f"\n{'='*80}")
            print(f"📍 NIVEAU {level_num}/{len(levels)}")
            print(f"{'='*80}")
            
            # Dans chaque niveau, trier par priorité
            sorted_tables = sorted(
                level_tables,
                key=lambda t: {'critical': 0, 'high': 1, 'normal': 2}[self.nodes[t].priority]
            )
            
            for i, table_name in enumerate(sorted_tables, 1):
                node = self.nodes[table_name]
                
                print(f"\n[{i}/{len(sorted_tables)}] ", end="")
                
                # Vérifier si on doit skip (dépendance échouée)
                failed_deps = [dep for dep in node.dependencies if dep in failed]
                
                if failed_deps:
                    node.status = 'skipped'
                    skipped.add(table_name)
                    print(f"⏭️  {table_name} SKIPPED (dépendances échouées: {', '.join(failed_deps)})")
                    continue
                
                # Exécuter
                success = self.execute_node(node)
                
                if success:
                    completed.add(table_name)
                else:
                    failed.add(table_name)
                    
                    # Arrêter si critique et flag activé
                    if node.priority == 'critical' and self.stop_on_critical:
                        print(f"\n🛑 ARRÊT: Table critique {table_name} échouée")
                        
                        # Marquer toutes les tables restantes comme skipped
                        for other_name, other_node in self.nodes.items():
                            if other_node.status == 'pending':
                                other_node.status = 'skipped'
                                skipped.add(other_name)
                        
                        break
            
            # Si arrêt demandé, sortir
            if any(n.priority == 'critical' and n.status == 'failed' for n in self.nodes.values()) and self.stop_on_critical:
                break
        
        # Rapport final
        duration_global = (datetime.now() - start_global).total_seconds()
        
        results = {
            'start_time': start_global,
            'end_time': datetime.now(),
            'duration_seconds': duration_global,
            'total_tables': len(self.nodes),
            'completed': len(completed),
            'failed': len(failed),
            'skipped': len(skipped),
            'success_rate': len(completed) / len(self.nodes) if self.nodes else 0,
            'nodes': {name: {
                'status': node.status,
                'duration': node.duration,
                'retry_count': node.retry_count,
                'error': node.error
            } for name, node in self.nodes.items()}
        }
        
        self._print_final_report(results)
        self._save_report(results)
        
        return results
    
    def _print_final_report(self, results: Dict):
        """Affiche rapport final"""
        print("\n" + "="*80)
        print("📊 RAPPORT FINAL")
        print("="*80)
        
        print(f"\n✅ Complétées : {results['completed']}/{results['total_tables']}")
        print(f"❌ Échecs     : {results['failed']}/{results['total_tables']}")
        print(f"⏭️  Skippées   : {results['skipped']}/{results['total_tables']}")
        print(f"📈 Taux succès: {results['success_rate']:.1%}")
        print(f"⏱️  Durée      : {results['duration_seconds']/60:.1f} min")
        
        # Détail par priorité
        print(f"\n📊 PAR PRIORITÉ")
        print("-"*80)
        
        for priority in ['critical', 'high', 'normal']:
            priority_nodes = [n for n in self.nodes.values() if n.priority == priority]
            if not priority_nodes:
                continue
            
            success = sum(1 for n in priority_nodes if n.status == 'success')
            failed = sum(1 for n in priority_nodes if n.status == 'failed')
            
            print(f"{priority.upper():10} : {success}/{len(priority_nodes)} réussies, {failed} échecs")
        
        # Top 5 plus lentes
        print(f"\n⏱️  TOP 5 TABLES LES PLUS LENTES")
        print("-"*80)
        
        completed_nodes = [n for n in self.nodes.values() if n.duration is not None]
        slowest = sorted(completed_nodes, key=lambda n: n.duration, reverse=True)[:5]
        
        for node in slowest:
            print(f"  {node.name:30} {node.duration:>8.1f}s")
        
        # Échecs détaillés
        if results['failed'] > 0:
            print(f"\n❌ TABLES EN ÉCHEC")
            print("-"*80)
            
            for node in self.nodes.values():
                if node.status == 'failed':
                    print(f"\n{node.name} ({node.priority.upper()}):")
                    print(f"  Tentatives: {node.retry_count}")
                    print(f"  Erreur: {node.error[:200] if node.error else 'N/A'}")
        
        print("\n" + "="*80)
    
    def _save_report(self, results: Dict):
        """Sauvegarde rapport détaillé"""
        output_dir = Path(__file__).parent.parent / "dag_reports"
        output_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Rapport texte
        report_file = output_dir / f"dag_{timestamp}.txt"
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(f"RAPPORT DAG ORCHESTRATOR\n")
            f.write(f"Mode: {self.mode}\n")
            f.write(f"Démarrage: {results['start_time']}\n")
            f.write(f"Durée: {results['duration_seconds']/60:.1f} min\n")
            f.write("="*80 + "\n\n")
            
            # Par niveau
            levels = self.get_execution_levels()
            for i, level in enumerate(levels, 1):
                f.write(f"\nNIVEAU {i}\n")
                f.write("-"*80 + "\n")
                
                for table in level:
                    node = self.nodes[table]
                    status_icon = {
                        'success': '✅',
                        'failed': '❌',
                        'skipped': '⏭️',
                        'pending': '⏸️'
                    }[node.status]
                    
                    duration_str = f"{node.duration:.1f}s" if node.duration else "N/A"
                    f.write(f"{status_icon} {table:30} {node.priority:10} {duration_str:>10}\n")
                    
                    if node.error:
                        f.write(f"   Erreur: {node.error}\n")
        
        print(f"\n📄 Rapport sauvegardé: {report_file}")
        
        # CSV pour analyse
        csv_file = output_dir / f"dag_{timestamp}.csv"
        df = pd.DataFrame([
            {
                'table': name,
                'priority': node.priority,
                'status': node.status,
                'duration_seconds': node.duration,
                'retry_count': node.retry_count,
                'dependencies': ','.join(node.dependencies),
                'error': node.error
            }
            for name, node in self.nodes.items()
        ])
        df.to_csv(csv_file, index=False, encoding='utf-8-sig')
        print(f"📊 CSV exporté: {csv_file}")


# ========== SCRIPT DE MIGRATION ==========

def create_dependencies_table():
    """Crée la table config.ETL_Dependencies"""
    from src.utils.connections import get_sqlserver_connection
    
    conn = get_sqlserver_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES 
                      WHERE TABLE_SCHEMA = 'config' 
                      AND TABLE_NAME = 'ETL_Dependencies')
        BEGIN
            CREATE TABLE config.ETL_Dependencies (
                DependencyId INT IDENTITY(1,1) PRIMARY KEY,
                TableName NVARCHAR(100) NOT NULL,
                DependsOn NVARCHAR(100) NOT NULL,
                DependencyType NVARCHAR(20) DEFAULT 'hard',  -- 'hard' ou 'soft'
                IsActive BIT DEFAULT 1,
                Notes NVARCHAR(500),
                CONSTRAINT UQ_Dependency UNIQUE (TableName, DependsOn),
                CONSTRAINT FK_Dependency_Table 
                    FOREIGN KEY (TableName) REFERENCES config.ETL_Tables(TableName),
                CONSTRAINT FK_Dependency_DependsOn 
                    FOREIGN KEY (DependsOn) REFERENCES config.ETL_Tables(TableName)
            );
            
            -- Exemple de dépendances (à adapter)
            INSERT INTO config.ETL_Dependencies (TableName, DependsOn, Notes)
            VALUES 
                ('lignecli', 'client', 'Les lignes de commande dépendent des clients'),
                ('lignecli', 'produit', 'Les lignes de commande dépendent des produits');
            
            PRINT '✅ Table config.ETL_Dependencies créée';
        END
        ELSE
        BEGIN
            PRINT '⚠️  Table config.ETL_Dependencies existe déjà';
        END
    """)
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print("✅ Migration terminée")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='DAG Orchestrator avec dépendances')
    parser.add_argument(
        '--mode',
        choices=['full', 'incremental'],
        default='incremental',
        help='Mode de chargement ETL'
    )
    parser.add_argument(
        '--continue-on-error',
        action='store_true',
        help='Continue même si table critique échoue'
    )
    parser.add_argument(
        '--setup',
        action='store_true',
        help='Créer table de dépendances'
    )
    
    args = parser.parse_args()
    
    if args.setup:
        create_dependencies_table()
        sys.exit(0)
    
    orchestrator = DAGOrchestrator(
        mode=args.mode,
        stop_on_critical=not args.continue_on_error
    )
    
    results = orchestrator.execute()
    
    success = results['failed'] == 0
    sys.exit(0 if success else 1)