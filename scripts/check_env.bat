@echo off
call D:\prefect_env\Scripts\activate.bat
cd /d D:\SQLServer\CBM_ETL\ETL

echo ========================================
echo  Vérification environnement
echo ========================================
echo.
echo Python version:
python --version
echo.
echo Prefect version:
prefect version
echo.
echo Packages installés:
pip list | findstr /i "prefect pandas pyodbc sqlalchemy pyarrow"
echo.
pause