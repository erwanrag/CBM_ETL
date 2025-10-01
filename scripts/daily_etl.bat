@echo off
REM ETL quotidien automatique

call D:\prefect_env\Scripts\activate.bat
cd /d D:\SQLServer\CBM_ETL\ETL

echo %date% %time% - Debut ETL >> logs\daily_etl.log

REM Orchestrateur
python flows/orchestrator.py --mode incremental >> logs\daily_etl.log 2>&1

REM Health check post-ETL
python scripts/daily_health_check.py >> logs\daily_etl.log 2>&1

REM Nettoyage cache >7 jours
python scripts/cleanup_cache.py --days 7 >> logs\daily_etl.log 2>&1

echo %date% %time% - Fin ETL >> logs\daily_etl.log