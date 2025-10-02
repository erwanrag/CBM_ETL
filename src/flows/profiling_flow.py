"""
Profiling intelligent des colonnes ETL
- Analyse TOUTES les données en staging (pas d'échantillon)
- Vérification date dernier profiling
- Force possible
- Intégration optionnelle dans load_flow
"""
import sys
import os
from pathlib import Path
from datetime import datetime, timedelta

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent.parent / ".env")

import pandas as pd
import pyodbc
from prefect import flow, task
from src.utils.connections import get_sqlserver_connection
from src.etl_logger import ETLLogger

SQLSERVER_CONN = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    f"SERVER={os.getenv('SQL_SERVER')};"
    f"DATABASE={os.getenv('SQL_DATABASE')};"
    "Trusted_Connection=yes;"
    "Connection Timeout=300;"
    "Command Timeout=600;"
)


@task
def check_profiling_needed(table_name: str, force: bool = False, days_threshold: int = 14):
    """
    Vérifie si profiling nécessaire
    
    Args:
        table_name: Nom de la table
        force: Si True, profiling même si récent
        days_threshold: Nombre de jours avant re-profiling
    
    Returns:
        tuple: (bool: profiling_needed, datetime: last_profiling_ts)
    """
    conn = get_sqlserver_connection()
    
    # Récupérer date dernier profiling
    df = pd.read_sql("""
        SELECT LastProfilingTs
        FROM config.ETL_Tables
        WHERE TableName = ?
    """, conn, params=[table_name])
    
    conn.close()
    
    if df.empty:
        raise ValueError(f"Table {table_name} introuvable dans ETL_Tables")
    
    last_profiling = df.iloc[0]['LastProfilingTs']
    
    # Force : toujours faire profiling
    if force:
        print(f"🔄 Profiling FORCÉ pour {table_name}")
        return True, last_profiling
    
    # Jamais profilé : profiling nécessaire
    if pd.isna(last_profiling):
        print(f"ℹ️  {table_name} : Jamais profilé → Profiling nécessaire")
        return True, None
    
    # Vérifier ancienneté
    days_since = (datetime.now() - last_profiling).days
    
    if days_since >= days_threshold:
        print(f"⚠️  {table_name} : Dernier profiling il y a {days_since}j (seuil: {days_threshold}j) → Profiling nécessaire")
        return True, last_profiling
    else:
        print(f"✅ {table_name} : Profiling récent ({days_since}j) → Skip")
        return False, last_profiling


@task
def profile_staging_table(table_name: str, min_empty_pct: float = 90.0, min_distinct_ratio: float = 0.01):
    """
    Profile TOUTES les données en staging (pas d'échantillon)
    
    Args:
        table_name: Nom de la table
        min_empty_pct: Seuil % vide pour exclusion (défaut 90%)
        min_distinct_ratio: Ratio minimum distinct/total pour colonnes non-statiques (1% par défaut)
    
    Returns:
        pd.DataFrame: Résultat profiling avec recommandations
    """
    conn = get_sqlserver_connection()
    cursor = conn.cursor()
    
    print(f"\n{'='*80}")
    print(f"📊 PROFILING : stg.{table_name}")
    print(f"{'='*80}")
    
    # 1. Compter lignes totales
    cursor.execute(f"SELECT COUNT(*) FROM stg.{table_name}")
    total_rows = cursor.fetchone()[0]
    
    if total_rows == 0:
        print(f"⚠️  Table stg.{table_name} vide → Profiling impossible")
        conn.close()
        return pd.DataFrame()
    
    print(f"📈 Total lignes : {total_rows:,}")
    
    # 2. Récupérer structure colonnes
    cursor.execute(f"""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'stg' AND TABLE_NAME = ?
          AND COLUMN_NAME NOT IN ('hashdiff', 'ts_source', 'load_ts')
        ORDER BY ORDINAL_POSITION
    """, (table_name,))
    
    columns = cursor.fetchall()
    
    if not columns:
        print(f"⚠️  Aucune colonne trouvée pour stg.{table_name}")
        conn.close()
        return pd.DataFrame()
    
    print(f"📊 Colonnes à profiler : {len(columns)}")
    
    # 3. Profiler chaque colonne
    results = []
    
    for i, (col_name, col_type) in enumerate(columns, 1):
        print(f"\r[{i}/{len(columns)}] Profiling : {col_name:30}", end="", flush=True)
        
        try:
            # Compter NULL
            cursor.execute(f"""
                SELECT COUNT(*) 
                FROM stg.{table_name}
                WHERE [{col_name}] IS NULL
            """)
            null_count = cursor.fetchone()[0]
            
            # Compter valeurs vides (pour VARCHAR)
            empty_count = 0
            if col_type in ['varchar', 'nvarchar', 'char', 'nchar']:
                cursor.execute(f"""
                    SELECT COUNT(*) 
                    FROM stg.{table_name}
                    WHERE LTRIM(RTRIM([{col_name}])) = ''
                """)
                empty_count = cursor.fetchone()[0]
            
            # Compter valeurs distinctes
            cursor.execute(f"""
                SELECT COUNT(DISTINCT [{col_name}])
                FROM stg.{table_name}
                WHERE [{col_name}] IS NOT NULL
            """)
            distinct_count = cursor.fetchone()[0]
            
            # Calculer métriques
            empty_total = null_count + empty_count
            empty_pct = (empty_total / total_rows * 100) if total_rows > 0 else 0
            distinct_ratio = (distinct_count / total_rows) if total_rows > 0 else 0
            
            # Déterminer si colonne à exclure
            is_static = distinct_count <= 1  # ≤1 valeur unique
            is_fully_empty = empty_pct >= 99.9  # Quasi 100% vide
            is_mostly_empty_and_static = empty_pct >= min_empty_pct and distinct_count <= 2
            
            # RÈGLE CRITIQUE : Si >1% rempli ET valeurs variées → GARDER !
            has_meaningful_data = empty_pct < 99.0 and distinct_count > 2
            
            # Exclure SEULEMENT si :
            # 1. Statique (≤1 valeur) OU
            # 2. Quasi 100% vide (≥99.9%) OU  
            # 3. >90% vide ET ≤2 valeurs (ex: flag binaire rarement utilisé)
            recommend_exclude = (is_static or is_fully_empty or is_mostly_empty_and_static) and not has_meaningful_data
            
            # Raison exclusion
            reasons = []
            if is_static:
                reasons.append("Statique (≤1 valeur)")
            elif is_fully_empty:
                reasons.append(f"{empty_pct:.1f}% vide")
            elif is_mostly_empty_and_static:
                reasons.append(f"{empty_pct:.1f}% vide + ≤2 valeurs")
            
            if has_meaningful_data and recommend_exclude:
                reasons.append("⚠️ CONSERVÉE (>1% données variées)")
                recommend_exclude = False
            
            reason = " | ".join(reasons) if reasons else "Colonne active"
            
            results.append({
                'ColumnName': col_name,
                'DataType': col_type,
                'TotalRows': total_rows,
                'NullCount': null_count,
                'EmptyCount': empty_count,
                'EmptyTotal': empty_total,
                'EmptyPct': round(empty_pct, 2),
                'DistinctCount': distinct_count,
                'DistinctRatio': round(distinct_ratio, 4),
                'RecommendExclude': recommend_exclude,
                'Reason': reason
            })
            
        except Exception as e:
            print(f"\n⚠️  Erreur profiling {col_name} : {e}")
            results.append({
                'ColumnName': col_name,
                'DataType': col_type,
                'TotalRows': total_rows,
                'NullCount': None,
                'EmptyCount': None,
                'EmptyTotal': None,
                'EmptyPct': None,
                'DistinctCount': None,
                'DistinctRatio': None,
                'RecommendExclude': False,
                'Reason': f"Erreur: {str(e)[:100]}"
            })
    
    print()  # Nouvelle ligne après progression
    
    conn.close()
    
    return pd.DataFrame(results)


@task
def save_profiling_report(table_name: str, profiling_df: pd.DataFrame):
    """Sauvegarde rapport profiling en CSV"""
    output_dir = Path(__file__).parent.parent / "profiling_reports"
    output_dir.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_file = output_dir / f"profiling_{table_name}_{timestamp}.csv"
    
    profiling_df.to_csv(report_file, index=False, encoding='utf-8-sig')
    
    print(f"\n📄 Rapport sauvegardé : {report_file}")
    
    return str(report_file)


@task
def apply_exclusions(table_name: str, profiling_df: pd.DataFrame, auto_apply: bool = False):
    """
    Applique les exclusions recommandées dans config.ETL_Columns
    
    Args:
        table_name: Nom de la table
        profiling_df: DataFrame avec recommandations
        auto_apply: Si True, applique automatiquement. Sinon, demande confirmation.
    
    Returns:
        int: Nombre de colonnes exclues
    """
    to_exclude = profiling_df[profiling_df['RecommendExclude'] == True]
    
    if len(to_exclude) == 0:
        print("\n✅ Aucune colonne à exclure")
        return 0
    
    print(f"\n{'='*80}")
    print(f"📋 RECOMMANDATIONS D'EXCLUSION : {len(to_exclude)} colonne(s)")
    print(f"{'='*80}")
    
    for _, row in to_exclude.iterrows():
        print(f"❌ {row['ColumnName']:30} {row['Reason']}")
    
    # Confirmation si non auto
    if not auto_apply:
        print(f"\n⚠️  Appliquer ces exclusions dans config.ETL_Columns ?")
        response = input("   [O]ui / [N]on : ").strip().upper()
        
        if response != 'O':
            print("⏭️  Exclusions NON appliquées")
            return 0
    
    # Appliquer exclusions
    conn = get_sqlserver_connection()
    cursor = conn.cursor()
    
    excluded_count = 0
    
    for _, row in to_exclude.iterrows():
        col_name = row['ColumnName']
        reason = row['Reason']
        
        cursor.execute("""
            UPDATE config.ETL_Columns
            SET IsExcluded = 1,
                Notes = ?
            WHERE TableName = ? AND ColumnName = ?
        """, (f"Auto-profiling: {reason}", table_name, col_name))
        
        excluded_count += cursor.rowcount
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"\n✅ {excluded_count} colonne(s) exclue(s) dans config.ETL_Columns")
    
    return excluded_count


@task
def update_profiling_timestamp(table_name: str):
    """Met à jour LastProfilingTs dans ETL_Tables"""
    conn = get_sqlserver_connection()
    cursor = conn.cursor()
    
    now = datetime.now()
    
    cursor.execute("""
        UPDATE config.ETL_Tables
        SET LastProfilingTs = ?
        WHERE TableName = ?
    """, (now, table_name))
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"✅ LastProfilingTs mis à jour : {now.strftime('%Y-%m-%d %H:%M:%S')}")


@flow(name="Profiling Flow V2")
def profiling_flow(
    table_name: str,
    force: bool = False,
    days_threshold: int = 14,
    auto_apply: bool = False,
    min_empty_pct: float = 90.0
):
    """
    Flow de profiling intelligent
    
    Args:
        table_name: Nom de la table à profiler
        force: Forcer profiling même si récent
        days_threshold: Seuil jours avant re-profiling (14 par défaut)
        auto_apply: Appliquer automatiquement exclusions sans confirmation
        min_empty_pct: Seuil % vide pour exclusion (90% par défaut)
    
    Returns:
        dict: Résultat profiling
    """
    logger = ETLLogger(SQLSERVER_CONN)
    start_time = datetime.now()
    
    print(f"\n{'='*80}")
    print(f"🔍 PROFILING FLOW V2 : {table_name}")
    print(f"   Force : {force}")
    print(f"   Seuil : {days_threshold} jours")
    print(f"   Auto-apply : {auto_apply}")
    print(f"{'='*80}")
    
    try:
        logger.log_step(table_name, "profiling_start", "started")
        
        # 1. Vérifier si profiling nécessaire
        profiling_needed, last_profiling = check_profiling_needed(
            table_name, force, days_threshold
        )
        
        if not profiling_needed:
            print(f"\n✅ Profiling non nécessaire pour {table_name}")
            logger.log_step(table_name, "profiling_skipped", "success")
            return {
                'status': 'skipped',
                'reason': 'Profiling récent',
                'last_profiling': last_profiling
            }
        
        # 2. Profiler colonnes depuis staging
        profiling_df = profile_staging_table(table_name, min_empty_pct=min_empty_pct)
        
        if profiling_df.empty:
            print(f"⚠️  Profiling impossible (table vide ou erreur)")
            logger.log_step(table_name, "profiling_failed", "failed", error="Table vide")
            return {'status': 'failed', 'reason': 'Table vide'}
        
        # 3. Sauvegarder rapport
        report_file = save_profiling_report(table_name, profiling_df)
        
        # 4. Afficher résumé
        total_cols = len(profiling_df)
        to_exclude = len(profiling_df[profiling_df['RecommendExclude'] == True])
        active = total_cols - to_exclude
        
        print(f"\n{'='*80}")
        print(f"📊 RÉSUMÉ PROFILING")
        print(f"{'='*80}")
        print(f"Total colonnes    : {total_cols}")
        print(f"Colonnes actives  : {active} ({active/total_cols*100:.1f}%)")
        print(f"À exclure         : {to_exclude} ({to_exclude/total_cols*100:.1f}%)")
        print(f"{'='*80}")
        
        # 5. Appliquer exclusions
        excluded_count = apply_exclusions(table_name, profiling_df, auto_apply)
        
        # 6. Mettre à jour timestamp
        update_profiling_timestamp(table_name)
        
        # 7. Log succès
        duration = (datetime.now() - start_time).total_seconds()
        logger.log_step(
            table_name, 
            "profiling_complete", 
            "success",
            duration=duration
        )
        
        print(f"\n✅ Profiling terminé ({duration:.1f}s)")
        
        return {
            'status': 'success',
            'total_columns': total_cols,
            'active_columns': active,
            'excluded_columns': excluded_count,
            'report_file': report_file,
            'duration': duration
        }
        
    except Exception as e:
        logger.log_step(table_name, "profiling_failed", "failed", error=str(e))
        print(f"\n❌ Erreur profiling : {e}")
        raise


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Profiling intelligent colonnes ETL')
    parser.add_argument('table_name', help='Nom de la table à profiler')
    parser.add_argument('--force', action='store_true', help='Forcer profiling même si récent')
    parser.add_argument('--days', type=int, default=14, help='Seuil jours avant re-profiling (défaut: 14)')
    parser.add_argument('--auto-apply', action='store_true', help='Appliquer exclusions automatiquement')
    parser.add_argument('--min-empty-pct', type=float, default=90.0, help='Seuil %% vide pour exclusion (défaut: 90)')
    
    args = parser.parse_args()
    
    profiling_flow(
        table_name=args.table_name,
        force=args.force,
        days_threshold=args.days,
        auto_apply=args.auto_apply,
        min_empty_pct=args.min_empty_pct
    )