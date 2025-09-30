@echo off
echo ========================================
echo  Activation environnement CBM_ETL
echo ========================================
echo.

REM Activer l'environnement virtuel Prefect centralisé
call D:\prefect_env\Scripts\activate.bat

REM Naviguer vers le dossier ETL
cd /d D:\SQLServer\CBM_ETL\ETL

echo.
echo ✅ Environnement activé : D:\prefect_env
echo 📁 Dossier ETL : %CD%
echo.
echo Commandes disponibles :
echo   - python flows/load_flow.py ^<table^> [full^|incremental]
echo   - prefect server start (dans une autre fenêtre)
echo   - python flows/orchestrator.py (Phase 2)
echo.

cmd /k