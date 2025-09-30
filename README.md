README.md minimaliste
markdown# CBM_ETL - Progress → SQL Server ETL Pipeline

Pipeline ETL professionnel pour migration Progress OpenEdge vers SQL Server avec architecture Staging/ODS.

## Stack
- Python 3.10+
- Prefect 2.x (orchestration)
- pyodbc (connectivité)
- pandas (transformation)

## Structure
CBM_ETL/
├── .env.template          # Template credentials
├── etl_logger.py          # Logging centralisé
├── utils/                 # Utilitaires réutilisables
├── tasks/                 # Tasks Prefect modulaires
└── flows/                 # Flows orchestration

## Installation
```bash
python -m venv prefect_env
prefect_env\Scripts\activate
pip install -r requirements.txt
cp .env.template .env      # Configurer vos credentials
Usage
bash# Single table
python flows/load_flow.py produit incremental

# Orchestration (à venir)
python flows/orchestrator.py
Architecture

Staging (stg): Chargement brut avec colonnes techniques
ODS (ods): Tables dimensionnelles avec historisation
Config: Métadonnées ETL et paramétrage


## .env.template (à commiter)
```bash
# Progress OpenEdge
PROGRESS_DSN=your_dsn_name
PROGRESS_USER=your_user
PROGRESS_PWD=your_password

# SQL Server
SQL_SERVER=localhost
SQL_DATABASE=CBM_ETL
SQL_USER=
SQL_PASSWORD=

# Prefect (optionnel)
PREFECT_API_URL=