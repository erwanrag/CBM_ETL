@echo off
echo ========================================
echo  Profiling Colonnes ETL
echo ========================================
echo.

if "%1"=="" (
    echo Usage: run_profiling.bat ^<table_name^> [--force] [--auto-apply]
    echo.
    echo Exemples:
    echo   run_profiling.bat produit
    echo   run_profiling.bat client --force
    echo   run_profiling.bat lignecli --force --auto-apply
    echo.
    pause
    exit /b 1
)

REM Activer environnement
call D:\prefect_env\Scripts\activate.bat

REM Naviguer vers dossier ETL
cd /d D:\SQLServer\CBM_ETL\ETL

echo.
echo 🔍 Profiling : %1
echo    Options : %2 %3 %4
echo.

REM Exécuter profiling
python src\flows\profiling_flow.py %1 %2 %3 %4

echo.
echo ========================================
echo  Profiling terminé
echo ========================================
pause