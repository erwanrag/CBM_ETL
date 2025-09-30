@echo off
echo ========================================
echo  Workspace ETL CBM
echo ========================================
echo.

REM Démarrer le serveur Prefect dans une nouvelle fenêtre
start "Prefect Server" cmd /c "call D:\SQLServer\CBM_ETL\ETL\start_prefect_server.bat"

REM Attendre que le serveur démarre
echo ⏳ Démarrage du serveur Prefect...
timeout /t 5 /nobreak >nul

REM Ouvrir l'UI Prefect
start http://127.0.0.1:4200

REM Activer la console ETL
call D:\prefect_env\Scripts\activate.bat
cd /d D:\SQLServer\CBM_ETL\ETL

echo.
echo ✅ Workspace prêt !
echo    - Serveur Prefect : fenêtre séparée
echo    - UI Prefect : http://127.0.0.1:4200
echo    - Console ETL : cette fenêtre
echo    - Environnement : D:\prefect_env
echo.
echo Commandes disponibles :
echo   python flows/load_flow.py ^<table^> [full^|incremental]
echo   python flows/profiling_flow.py ^<table^>
echo.

cmd /k