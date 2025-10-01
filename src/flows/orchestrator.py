import sys
import os
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent.parent / \".env\")

import pandas as pd
from src.flows.load_flow_simple import load_flow_simple
from src.utils.connections import get_sqlserver_connection
from src.utils.alerting import Alerter

def get_tables_by_priority():
    """
    Récupère les tables groupées par priorité
    Returns: dict avec keys 'critical', 'high', 'normal'
    """
    conn = get_sqlserver_connection()
    
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
        ORDER BY Priority, TableName
    """, conn)
    
    conn.close()
    
    groups = {
        'critical': df[df['Priority'] == 'critical']['TableName'].tolist(),
        'high': df[df['Priority'] == 'high']['TableName'].tolist(),
        'normal': df[df['Priority'] == 'normal']['TableName'].tolist()
    }
    
    return groups

def orchestrate_etl(mode="incremental", stop_on_critical_failure=True):
    """
    Orchestrateur principal - lance tous les ETL par ordre de priorité
    
    Args:
        mode: 'full' ou 'incremental'
        stop_on_critical_failure: Si True, arrête tout si une table critique échoue
    """
    
    start_global = datetime.now()
    alerter = Alerter()
    
    print("="*80)
    print(f"🚀 ORCHESTRATEUR ETL (mode: {mode})")
    print(f"   Démarrage : {start_global.strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)
    
    groups = get_tables_by_priority()
    
    # Afficher le plan
    print("\n📋 PLAN D'EXÉCUTION")
    print("-"*80)
    print(f"Critical : {len(groups['critical'])} tables")
    print(f"High     : {len(groups['high'])} tables")
    print(f"Normal   : {len(groups['normal'])} tables")
    print(f"TOTAL    : {sum(len(g) for g in groups.values())} tables")
    print("-"*80)
    
    results = {}
    
    # Traiter par groupe de priorité
    for priority_level in ['critical', 'high', 'normal']:
        tables = groups[priority_level]
        
        if not tables:
            continue
        
        print(f"\n{'='*80}")
        print(f"🎯 PRIORITÉ : {priority_level.upper()} ({len(tables)} tables)")
        print(f"{'='*80}")
        
        for i, table in enumerate(tables, 1):
            print(f"\n[{i}/{len(tables)}] {table}")
            print("-"*80)
            
            try:
                start = datetime.now()
                load_flow_simple(table, mode=mode)
                duration = (datetime.now() - start).total_seconds()
                
                results[table] = {
                    'priority': priority_level,
                    'status': 'success',
                    'duration': duration,
                    'error': None
                }
                
                print(f"✅ {table} terminé ({duration:.1f}s)")
                
            except Exception as e:
                error_msg = str(e)
                duration = (datetime.now() - start).total_seconds()
                
                results[table] = {
                    'priority': priority_level,
                    'status': 'failed',
                    'duration': duration,
                    'error': error_msg[:500]
                }
                
                print(f"❌ {table} échoué : {error_msg[:200]}")
                
                # Alerte immédiate si critique
                if priority_level == 'critical':
                    alerter.alert_etl_failure(table, error_msg)
                
                # Si table critique échoue, arrêter ?
                if priority_level == 'critical' and stop_on_critical_failure:
                    print("\n"+"="*80)
                    print("🛑 ARRÊT : Table critique échouée")
                    print("="*80)
                    generate_report(results, start_global)
                    return False
    
    # Rapport final
    duration_global = (datetime.now() - start_global).total_seconds()
    
    print("\n" + "="*80)
    print("📊 ORCHESTRATION TERMINÉE")
    print("="*80)
    
    success_count = sum(1 for r in results.values() if r['status'] == 'success')
    failed_count = len(results) - success_count
    
    print(f"✅ Réussite : {success_count}/{len(results)}")
    print(f"❌ Échec    : {failed_count}/{len(results)}")
    print(f"⏱️  Durée    : {duration_global/60:.1f} min")
    
    # Détail par priorité
    for priority in ['critical', 'high', 'normal']:
        priority_results = {k: v for k, v in results.items() if v['priority'] == priority}
        if priority_results:
            p_success = sum(1 for r in priority_results.values() if r['status'] == 'success')
            print(f"\n{priority.upper()}: {p_success}/{len(priority_results)} réussies")
    
    print("="*80)
    
    # Sauvegarder rapport
    generate_report(results, start_global)
    
    return failed_count == 0

def generate_report(results, start_time):
    """Génère un rapport détaillé de l'orchestration"""
    
    output_dir = Path(__file__).parent.parent / "orchestration_reports"
    output_dir.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_file = output_dir / f"orchestration_{timestamp}.txt"
    
    duration_total = (datetime.now() - start_time).total_seconds()
    
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write(f"RAPPORT ORCHESTRATION ETL\n")
        f.write(f"Démarrage : {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Durée : {duration_total/60:.1f} min\n")
        f.write("="*80 + "\n\n")
        
        # Par priorité
        for priority in ['critical', 'high', 'normal']:
            priority_results = {k: v for k, v in results.items() if v['priority'] == priority}
            if not priority_results:
                continue
            
            f.write(f"\n{priority.upper()}\n")
            f.write("-"*80 + "\n")
            
            for table, result in sorted(priority_results.items()):
                status_icon = '✅' if result['status'] == 'success' else '❌'
                f.write(f"{status_icon} {table:30} {result['duration']:>6.1f}s\n")
                if result['error']:
                    f.write(f"   Erreur : {result['error'][:200]}\n")
        
        # Échecs détaillés
        failures = {k: v for k, v in results.items() if v['status'] == 'failed'}
        if failures:
            f.write("\n" + "="*80 + "\n")
            f.write("ÉCHECS DÉTAILLÉS\n")
            f.write("-"*80 + "\n")
            for table, result in failures.items():
                f.write(f"\n{table}:\n")
                f.write(f"  {result['error']}\n")
    
    print(f"\n📄 Rapport sauvegardé : {report_file}")
    
    # CSV pour analyse
    csv_file = output_dir / f"orchestration_{timestamp}.csv"
    df = pd.DataFrame([
        {
            'table': table,
            'priority': r['priority'],
            'status': r['status'],
            'duration_seconds': r['duration'],
            'error': r['error']
        }
        for table, r in results.items()
    ])
    df.to_csv(csv_file, index=False, encoding='utf-8-sig')
    print(f"📊 CSV exporté : {csv_file}")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Orchestrateur ETL multi-tables')
    parser.add_argument(
        '--mode',
        choices=['full', 'incremental'],
        default='incremental',
        help='Mode de chargement ETL'
    )
    parser.add_argument(
        '--continue-on-error',
        action='store_true',
        help='Continue même si une table critique échoue'
    )
    
    args = parser.parse_args()
    
    success = orchestrate_etl(
        mode=args.mode,
        stop_on_critical_failure=not args.continue_on_error
    )
    
    sys.exit(0 if success else 1)