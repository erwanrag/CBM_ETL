import sys
import os
from pathlib import Path
from datetime import datetime
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent.parent / ".env")

from src.flows.load_flow_simple import load_flow_simple
from src.utils.connections import get_sqlserver_connection

def get_tables_to_validate(filter_mode="all"):
    """
    Récupère les tables à valider depuis config.ETL_Tables
    
    Args:
        filter_mode: 
            - "all" : Toutes les tables
            - "dimensions" : Seulement IsDimension=1
            - "facts" : Seulement IsFact=1
            - "priority" : Tables avec Notes contenant 'priority' ou 'critique'
    """
    conn = get_sqlserver_connection()
    
    base_query = """
    SELECT 
        TableName,
        DestinationTable,
        IsDimension,
        IsFact,
        HasTimestamps,
        Notes,
        LastSuccessTs
    FROM config.ETL_Tables
    WHERE 1=1
    """
    
    if filter_mode == "dimensions":
        base_query += " AND IsDimension = 1"
    elif filter_mode == "facts":
        base_query += " AND IsFact = 1"
    elif filter_mode == "priority":
        base_query += " AND (Notes LIKE '%priority%' OR Notes LIKE '%critique%')"
    
    base_query += " ORDER BY TableName"
    
    df = pd.read_sql(base_query, conn)
    conn.close()
    
    print(f"\n📋 Tables à valider (mode: {filter_mode})")
    print("-"*80)
    print(f"Total : {len(df)} tables")
    
    if len(df) > 0:
        dims = df[df['IsDimension'] == 1]
        facts = df[df['IsFact'] == 1]
        print(f"  - Dimensions : {len(dims)}")
        print(f"  - Facts : {len(facts)}")
        print(f"  - Autres : {len(df) - len(dims) - len(facts)}")
    
    return df['TableName'].tolist()

def validate_all_tables(filter_mode="all", mode="full", max_tables=None):
    """
    Valide que l'ETL fonctionne sur toutes les tables
    
    Args:
        filter_mode: Type de tables à valider (all/dimensions/facts/priority)
        mode: Mode ETL (full/incremental)
        max_tables: Limite le nombre de tables (pour tests rapides)
    """
    
    tables = get_tables_to_validate(filter_mode)
    
    if max_tables:
        tables = tables[:max_tables]
        print(f"⚠️  Limite à {max_tables} tables pour test rapide")
    
    results = {}
    start_global = datetime.now()
    
    print("\n" + "="*80)
    print(f"🚀 VALIDATION ETL - {len(tables)} tables (mode: {mode})")
    print("="*80)
    
    for i, table in enumerate(tables, 1):
        print(f"\n[{i}/{len(tables)}] Validation : {table}")
        print("-"*80)
        
        try:
            start = datetime.now()
            load_flow_simple(table, mode=mode)
            duration = (datetime.now() - start).total_seconds()
            
            results[table] = {
                'status': '✅ SUCCESS',
                'duration': duration,
                'duration_str': f"{duration:.1f}s",
                'error': None
            }
            
        except Exception as e:
            error_msg = str(e)
            results[table] = {
                'status': '❌ FAILED',
                'duration': (datetime.now() - start).total_seconds(),
                'duration_str': '-',
                'error': error_msg[:200]  # Limiter la taille
            }
            print(f"\n❌ ERREUR : {error_msg[:200]}")
    
    # Rapport final
    duration_global = (datetime.now() - start_global).total_seconds()
    
    print("\n" + "="*80)
    print("📊 RAPPORT FINAL")
    print("="*80)
    
    success_count = sum(1 for r in results.values() if '✅' in r['status'])
    failed_count = len(results) - success_count
    
    # Tri par statut puis par nom
    sorted_results = sorted(results.items(), key=lambda x: (x[1]['status'], x[0]))
    
    for table, result in sorted_results:
        status = result['status']
        detail = result['duration_str']
        print(f"{status:12} {table:30} {detail:>10}")
    
    print("-"*80)
    print(f"✅ Réussite : {success_count}/{len(tables)}")
    print(f"❌ Échec    : {failed_count}/{len(tables)}")
    print(f"⏱️  Durée    : {duration_global/60:.1f} min")
    
    # Statistiques de performance
    if success_count > 0:
        success_durations = [r['duration'] for r in results.values() if '✅' in r['status']]
        avg_duration = sum(success_durations) / len(success_durations)
        max_duration = max(success_durations)
        
        print(f"\n📈 Performance (tables réussies)")
        print(f"  - Moyenne : {avg_duration:.1f}s")
        print(f"  - Maximum : {max_duration:.1f}s")
        
        # Table la plus lente
        slowest = max(
            [(t, r['duration']) for t, r in results.items() if '✅' in r['status']], 
            key=lambda x: x[1]
        )
        print(f"  - Plus lente : {slowest[0]} ({slowest[1]:.1f}s)")
    
    print("="*80)
    
    # Sauvegarder résultats détaillés
    output_dir = Path(__file__).parent.parent / "validation_reports"
    output_dir.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_file = output_dir / f"validation_{filter_mode}_{timestamp}.txt"
    
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write(f"Validation ETL - {datetime.now()}\n")
        f.write(f"Mode : {mode} | Filtre : {filter_mode}\n")
        f.write("="*80 + "\n\n")
        
        f.write("RÉSULTATS\n")
        f.write("-"*80 + "\n")
        for table, result in sorted_results:
            f.write(f"{result['status']} {table:30} {result['duration_str']:>10}\n")
            if result['error']:
                f.write(f"    Erreur : {result['error']}\n")
        
        f.write("\n" + "="*80 + "\n")
        f.write(f"Réussite : {success_count}/{len(tables)}\n")
        f.write(f"Durée totale : {duration_global/60:.1f} min\n")
        
        # Liste des échecs pour debug
        if failed_count > 0:
            f.write("\n" + "="*80 + "\n")
            f.write("TABLES EN ÉCHEC (à investiguer)\n")
            f.write("-"*80 + "\n")
            for table, result in results.items():
                if '❌' in result['status']:
                    f.write(f"\n{table}:\n")
                    f.write(f"  {result['error']}\n")
    
    print(f"\n📄 Rapport sauvegardé : {report_file}")
    
    # Sauvegarder aussi en CSV pour analyse Excel
    csv_file = output_dir / f"validation_{filter_mode}_{timestamp}.csv"
    df_results = pd.DataFrame([
        {
            'table': table,
            'status': 'success' if '✅' in r['status'] else 'failed',
            'duration_seconds': r['duration'],
            'error': r['error']
        }
        for table, r in results.items()
    ])
    df_results.to_csv(csv_file, index=False, encoding='utf-8-sig')
    print(f"📊 CSV exporté : {csv_file}")
    
    return success_count == len(tables)

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Valide l\'ETL sur plusieurs tables')
    parser.add_argument(
        '--filter', 
        choices=['all', 'dimensions', 'facts', 'priority'],
        default='all',
        help='Type de tables à valider'
    )
    parser.add_argument(
        '--mode',
        choices=['full', 'incremental'],
        default='full',
        help='Mode de chargement ETL'
    )
    parser.add_argument(
        '--max',
        type=int,
        help='Nombre maximum de tables à tester (pour tests rapides)'
    )
    
    args = parser.parse_args()
    
    print("="*80)
    print("🔍 VALIDATION ETL - Configuration")
    print("="*80)
    print(f"Filtre : {args.filter}")
    print(f"Mode ETL : {args.mode}")
    if args.max:
        print(f"Limite : {args.max} premières tables")
    print("="*80)
    
    success = validate_all_tables(
        filter_mode=args.filter,
        mode=args.mode,
        max_tables=args.max
    )
    
    sys.exit(0 if success else 1)