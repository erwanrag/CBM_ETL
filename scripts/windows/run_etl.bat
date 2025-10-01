@echo off
echo ========================================
echo  Lancement ETL CBM
echo ========================================
echo.

if "%1"=="" (
    echo ❌ Usage: run_etl.bat ^<table_name^> [full^|incremental]
    echo.
    echo Exemples:
    echo   run_etl.bat produit full
    echo   run_etl.bat client incremental
    echo.
    pause
    exit /b 1
)

REM Activer l'environnement virtuel centralisé
call D:\prefect_env\Scripts\activate.bat

REM Naviguer vers le dossier ETL
cd /d D:\SQLServer\CBM_ETL\ETL

REM Définir le mode (défaut: incremental)
set MODE=%2
if "%MODE%"=="" set MODE=incremental

echo.
echo 🚀 Lancement ETL : %1 (mode: %MODE%)
echo    Environnement : D:\prefect_env
echo.

REM Exécuter le flow
python flows/load_flow.py %1 %MODE%

echo.
echo ========================================
echo  ETL terminé
echo ========================================
pause