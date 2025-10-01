@echo off
echo ========================================
echo  Nettoyage cache Parquet
echo ========================================
echo.

REM Activer l'environnement virtuel
call D:\prefect_env\Scripts\activate.bat

REM Naviguer vers le dossier ETL
cd /d D:\SQLServer\CBM_ETL\ETL

REM Supprimer les fichiers Parquet
if exist cache\parquet\*.parquet (
    echo 🗑️  Suppression des fichiers cache...
    del /q cache\parquet\*.parquet
    echo ✅ Cache Parquet nettoyé
) else (
    echo ℹ️  Aucun fichier cache trouvé
)

echo.
pause