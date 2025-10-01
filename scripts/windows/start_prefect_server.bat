@echo off
echo ========================================
echo  Démarrage Prefect Server
echo ========================================
echo.

REM Activer l'environnement virtuel centralisé
call D:\prefect_env\Scripts\activate.bat

REM Naviguer vers le dossier ETL pour logs/config
cd /d D:\SQLServer\CBM_ETL\ETL

echo.
echo 🚀 Démarrage du serveur Prefect...
echo    Interface UI : http://127.0.0.1:4200
echo    Environnement : D:\prefect_env
echo.
echo ⚠️  Laisser cette fenêtre ouverte pendant l'utilisation
echo.

REM Démarrer le serveur
prefect server start