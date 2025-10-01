import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")

import pandas as pd
from utils.connections import get_sqlserver_connection
from utils.alerting import Alerter  # AJOUTER cette ligne

def check_etl_health():
    """Vérification santé quotidienne de l'ETL"""
    
    alerter = Alerter()  # AJOUTER cette ligne
    conn = get_sqlserver_connection()
    
    print("="*80)
    print("🏥 ETL HEALTH CHECK")
    print("="*80)
    
    # 1. SLA Status
    print("\n📅 SLA Status")
    print("-"*80)
    df_sla = pd.read_sql("""
        SELECT TableName, HoursSinceSuccess, SLAStatus
        FROM etl.vw_SLABreach
        ORDER BY HoursSinceSuccess DESC
    """, conn)
    
    critical = df_sla[df_sla['SLAStatus'] == 'CRITICAL']
    warning = df_sla[df_sla['SLAStatus'] == 'WARNING']
    
    if len(critical) > 0:
        print(f"🔴 CRITICAL : {len(critical)} table(s)")
        for _, row in critical.iterrows():
            print(f"   {row['TableName']:20} {row['HoursSinceSuccess']}h sans succès")
    
    if len(warning) > 0:
        print(f"⚠️  WARNING : {len(warning)} table(s)")
        for _, row in warning.iterrows():
            print(f"   {row['TableName']:20} {row['HoursSinceSuccess']}h sans succès")
    
    if len(critical) == 0 and len(warning) == 0:
        print("✅ Toutes les tables sont à jour")
    
    # 2. Échecs récents (24h)
    print("\n❌ Échecs (dernières 24h)")
    print("-"*80)
    df_failures = pd.read_sql("""
        SELECT TableName, LogTs, ErrorMessage
        FROM etl.ETL_Log
        WHERE Status = 'failed'
          AND LogTs >= DATEADD(hour, -24, GETDATE())
          AND StepName = 'flow_complete'
        ORDER BY LogTs DESC
    """, conn)
    
    if len(df_failures) > 0:
        print(f"🔴 {len(df_failures)} échec(s) détecté(s)")
        for _, row in df_failures.iterrows():
            print(f"   {row['TableName']:20} {row['LogTs']}")
            print(f"      {row['ErrorMessage'][:100]}")
    else:
        print("✅ Aucun échec dans les dernières 24h")
    
    # 3. Performance moyenne
    print("\n⚡ Performance (7 derniers jours)")
    print("-"*80)
    df_perf = pd.read_sql("""
        SELECT TOP 5
            TableName, 
            TotalRuns,
            SuccessCount,
            FailCount,
            AvgDuration
        FROM etl.vw_TablePerformance
        ORDER BY AvgDuration DESC
    """, conn)
    
    print("Tables les plus lentes :")
    for _, row in df_perf.iterrows():
        success_rate = (row['SuccessCount'] / row['TotalRuns'] * 100) if row['TotalRuns'] > 0 else 0
        print(f"   {row['TableName']:20} {row['AvgDuration']:>6.1f}s  ({success_rate:.0f}% succès)")
    
    # 4. Volume traité (24h)
    print("\n📊 Volume (dernières 24h)")
    print("-"*80)
    df_volume = pd.read_sql("""
        SELECT 
            SUM(RowsProcessed) as TotalRows,
            COUNT(DISTINCT TableName) as TablesProcessed,
            COUNT(*) as TotalRuns
        FROM etl.ETL_Log
        WHERE Status = 'success'
          AND StepName = 'flow_complete'
          AND LogTs >= DATEADD(hour, -24, GETDATE())
    """, conn)
    
    if not df_volume.empty:
        row = df_volume.iloc[0]
        print(f"   Lignes traitées : {row['TotalRows']:>10,}")
        print(f"   Tables traitées : {row['TablesProcessed']:>10}")
        print(f"   Runs exécutés   : {row['TotalRuns']:>10}")
    
    # === NOUVEAU : ALERTING AUTOMATIQUE ===
    
    # Alerte SLA Breach
    if len(critical) > 0 or len(warning) > 0:
        tables_breach = []
        for _, row in pd.concat([critical, warning]).iterrows():
            tables_breach.append({
                'TableName': row['TableName'],
                'HoursSinceSuccess': row['HoursSinceSuccess']
            })
        alerter.alert_sla_breach(tables_breach)
    
    # Alerte échecs multiples (seuil : >5 échecs)
    if len(df_failures) > 5:
        alerter.send_alert(
            subject=f"Échecs multiples : {len(df_failures)} échecs",
            message="Plusieurs tables ont échoué dans les dernières 24h.",
            severity='critical',
            details=[f"{row['TableName']} ({row['LogTs']})" for _, row in df_failures.head(10).iterrows()]
        )
    
    # Résumé quotidien (si tout OK)
    if len(critical) == 0 and len(df_failures) == 0 and not df_volume.empty:
        stats = {
            'success_rate': 100.0,
            'tables_processed': int(df_volume.iloc[0]['TablesProcessed']),
            'rows_loaded': int(df_volume.iloc[0]['TotalRows']),
            'failures': 0,
            'duration_min': 0
        }
        alerter.alert_daily_summary(stats)
    
    conn.close()
    
    print("\n" + "="*80)
    
    return len(critical) == 0 and len(df_failures) == 0

if __name__ == "__main__":
    healthy = check_etl_health()
    sys.exit(0 if healthy else 1)