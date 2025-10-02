"""
Profiling batch : Profile toutes les tables obsolètes
"""
import sys
import os
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent.parent / ".env")

import pandas as pd
from src.flows.profiling_flow import profiling_flow
from src.utils.connections import get_sqlserver_connection
from src.utils.alerting import Alerter

def get_tables_needing_profiling(days_threshold: int = 14):
    """
    Récupère tables nécessitant profiling
    
    Returns:
        list: Liste de (table_name, days_since_profiling)
    """
    conn = get_sqlserver_connection()
    
    df = pd.read_sql(f"""
        SELECT 
            TableName,
            LastProfilingTs,
            CASE 
                WHEN LastProfilingTs IS NULL THEN 999
                ELSE DATEDIFF(day, LastProfilingTs, GETDATE())
            END AS DaysSinceProfiling
        FROM config.ETL_Tables
        WHERE (
            LastProfilingTs IS NULL 
            OR DATEDIFF(day, LastProfilingTs, GETDATE()) >= {days_threshold}
        )
        ORDER BY DaysSinceProfiling DESC
    """, conn)
    
    conn.close()
    
    return [(row['TableName'], row['DaysSinceProfiling']) for _, row in df.iterrows()]


def batch_profiling(
    days_threshold: int = 14,
    auto_apply: bool = True,
    max_tables: int = None
):
    """
    Profile toutes les tables obsolètes
    
    Args:
        days_threshold: Seuil jours pour considérer profiling obsolète
        auto_apply: Appliquer automatiquement exclusions
        max_tables: Limite nombre de tables (pour tests)
    """
    alerter = Alerter()
    start_global = datetime.now()
    
    print("="*80)
    print(f"🔍 BATCH PROFILING")
    print(f"   Seuil : {days_threshold} jours")
    print(f"   Auto-apply : {auto_apply}")
    print("="*80)
    
    # Récupérer tables
    tables = get_tables_needing_profiling(days_threshold)
    
    if max_tables:
        tables = tables[:max_tables]
        print(f"⚠️  Limite : {max_tables} premières tables")
    
    if not tables:
        print("\n✅ Aucune table nécessitant profiling")
        return
    
    print(f"\n📋 {len(tables)} table(s) à profiler")
    print("-"*80)
    
    for table, days_since in tables:
        if days_since == 999:
            print(f"  {table:30} Jamais profilé")
        else:
            print(f"  {table:30} {days_since}j")
    
    print("-"*80)
    
    # Profiler chaque table
    results = {}
    
    for i, (table_name, days_since) in enumerate(tables, 1):
        print(f"\n{'='*80}")
        print(f"[{i}/{len(tables)}] Profiling : {table_name}")
        print(f"{'='*80}")
        
        try:
            result = profiling_flow(
                table_name=table_name,
                force=False,
                days_threshold=days_threshold,
                auto_apply=auto_apply,
                min_empty_pct=90.0
            )
            
            results[table_name] = {
                'status': result['status'],
                'excluded': result.get('excluded_columns', 0) if result['status'] == 'success' else 0,
                'duration': result.get('duration', 0),
                'error': None
            }
            
            if result['status'] == 'success':
                print(f"✅ {table_name} : {result['excluded_columns']} colonnes exclues")
            else:
                print(f"⏭️  {table_name} : {result['status']}")
                
        except Exception as e:
            error_msg = str(e)
            results[table_name] = {
                'status': 'failed',
                'excluded': 0,
                'duration': 0,
                'error': error_msg[:200]
            }
            
            print(f"❌ {table_name} : {error_msg[:200]}")
    
    # Rapport final
    duration_global = (datetime.now() - start_global).total_seconds()
    
    print("\n" + "="*80)
    print("📊 RAPPORT BATCH PROFILING")
    print("="*80)
    
    success = sum(1 for r in results.values() if r['status'] == 'success')
    skipped = sum(1 for r in results.values() if r['status'] == 'skipped')
    failed = sum(1 for r in results.values() if r['status'] == 'failed')
    
    total_excluded = sum(r['excluded'] for r in results.values())
    
    print(f"✅ Succès   : {success}/{len(tables)}")
    print(f"⏭️  Skippés  : {skipped}/{len(tables)}")
    print(f"❌ Échecs   : {failed}/{len(tables)}")
    print(f"📊 Total colonnes exclues : {total_excluded}")
    print(f"⏱️  Durée totale : {duration_global/60:.1f} min")
    
    # Détail par table
    print(f"\n📋 DÉTAIL PAR TABLE")
    print("-"*80)
    
    for table, result in sorted(results.items()):
        status_icon = {
            'success': '✅',
            'skipped': '⏭️',
            'failed': '❌'
        }[result['status']]
        
        excluded_str = f"{result['excluded']} excl." if result['excluded'] > 0 else ""
        duration_str = f"{result['duration']:.1f}s" if result['duration'] > 0 else ""
        
        print(f"{status_icon} {table:30} {excluded_str:15} {duration_str:10}")
        
        if result['error']:
            print(f"   Erreur: {result['error']}")
    
    print("="*80)
    
    # Sauvegarder rapport
    output_dir = Path(__file__).parent.parent / "profiling_reports"
    output_dir.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_file = output_dir / f"batch_profiling_{timestamp}.csv"
    
    df_results = pd.DataFrame([
        {
            'table': table,
            'status': r['status'],
            'excluded_columns': r['excluded'],
            'duration_seconds': r['duration'],
            'error': r['error']
        }
        for table, r in results.items()
    ])
    
    df_results.to_csv(report_file, index=False, encoding='utf-8-sig')
    print(f"\n📄 Rapport CSV : {report_file}")
    
    # Alerte Teams si échecs
    if failed > 0:
        alerter.send_alert(
            subject=f"Batch Profiling : {failed} échec(s)",
            message=f"{failed}/{len(tables)} table(s) ont échoué lors du profiling batch",
            severity='warning',
            details=[f"{t}: {r['error'][:100]}" for t, r in results.items() if r['status'] == 'failed']
        )
    
    return results


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Profiling batch toutes tables obsolètes')
    parser.add_argument('--days', type=int, default=14, help='Seuil jours profiling obsolète')
    parser.add_argument('--no-auto-apply', action='store_true', help='Ne pas appliquer exclusions automatiquement')
    parser.add_argument('--max-tables', type=int, help='Limite nombre tables (tests)')
    
    args = parser.parse_args()
    
    batch_profiling(
        days_threshold=args.days,
        auto_apply=not args.no_auto_apply,
        max_tables=args.max_tables
    )