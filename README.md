# CBM_ETL - Progress → SQL Server Pipeline

Pipeline ETL production-ready pour migration Progress OpenEdge vers SQL Server.

## Architecture

### Stack technique
- Python 3.10+ | Prefect 2.x | pyodbc | pandas | pyarrow
- Progress OpenEdge (source) via ODBC
- SQL Server 2019+ (destination)

### Structure données
Progress (PUB schema)
↓ Extract (avec alias SQL-safe)
Parquet Cache (compression snappy)
↓ Transform (hashdiff + timestamps)
stg.* (staging avec colonnes techniques)
↓ MERGE (upsert basé sur PK + hashdiff)
ods.* (tables finales historisées)

## Installation rapide

# 1. Cloner et configurer environnement
python -m venv prefect_env
prefect_env\Scripts\activate
pip install -r requirements.txt

# 2. Configuration credentials
cp .env.template .env
# Éditer .env avec vos credentials Progress + SQL Server

# 3. Vérifier environnement
python scripts/preflight_check.py

# 4. Tester une table
python flows/load_flow_simple.py produit full
Utilisation
Chargement table unique
bash# Mode incremental (delta depuis LastSuccessTs)
python flows/load_flow_simple.py produit incremental

# Mode full (rechargement complet)
python flows/load_flow_simple.py client full
Orchestration multi-tables
bash# Toutes les tables par ordre de priorité
python flows/orchestrator.py --mode incremental

# Continuer même si table critique échoue
python flows/orchestrator.py --mode incremental --continue-on-error
Validation et monitoring
bash# Valider toutes les tables
python flows/validate_tables.py --filter all --mode incremental

# Health check quotidien
python scripts/daily_health_check.py

# Vérifier config avant ETL
python scripts/preflight_check.py produit
Configuration métadonnées
Ajouter une nouvelle table

Synchroniser métadonnées Progress :

sqlEXEC etl.SyncConfigColumns;

Configurer dans config.ETL_Tables :

sqlINSERT INTO config.ETL_Tables (
    TableName, DestinationTable, PrimaryKeyCols,
    HasTimestamps, DateModifCol, IsDimension
) VALUES (
    'nouvelle_table', 'ods.nouvelle_table', 'id',
    1, 'dat_mod', 1
);

Exclure colonnes inutiles dans config.ETL_Columns :

sqlUPDATE config.ETL_Columns
SET IsExcluded = 1
WHERE TableName = 'nouvelle_table' 
  AND ColumnName IN ('col_inutile1', 'col_inutile2');

Tester :

bashpython flows/load_flow_simple.py nouvelle_table full
Gestion des colonnes à tirets
Progress autorise les tirets dans les noms (gencod-v), SQL Server non.
Solution automatique :

config.ETL_Columns.ColumnName : Nom original (gencod-v)
config.ETL_Columns.SqlName : Nom normalisé (gencod_v)
config.ETL_Columns.SourceExpression : Alias extraction ("gencod-v" AS gencod_v)

Toutes les tables/colonnes sont créées automatiquement avec les noms SQL-safe.
Monitoring production
Vues SQL disponibles
sql-- Derniers runs par table
SELECT * FROM etl.vw_LastRuns;

-- Performance 7 derniers jours
SELECT * FROM etl.vw_TablePerformance;

-- Tables en retard (SLA breach)
SELECT * FROM etl.vw_SLABreach WHERE SLAStatus <> 'OK';
Logs structurés
Tous les runs sont tracés dans etl.ETL_Log avec :

RunId UUID unique par exécution
Durée par étape (extract/transform/load/merge)
Nombre de lignes traitées
Messages d'erreur détaillés

Architecture fichiers
CBM_ETL/
├── .env                    # Credentials (gitignored)
├── etl_logger.py           # Logging centralisé
├── requirements.txt
├── cache/parquet/          # Cache Parquet intermédiaire
├── utils/
│   ├── connections.py      # Pool connexions
│   ├── parquet_cache.py    # Gestion cache
│   ├── data_cleaning.py    # Transformations
│   └── type_mapping.py     # Mapping types Progress→SQL
├── tasks/                  # Tasks Prefect
├── tasks_simple/           # Version sans orchestration
├── flows/
│   ├── load_flow.py        # Flow Prefect principal
│   ├── load_flow_simple.py # Version simple
│   ├── orchestrator.py     # Orchestration multi-tables
│   └── validate_tables.py  # Validation batch
└── scripts/
    ├── preflight_check.py      # Vérif pré-ETL
    ├── daily_health_check.py   # Monitoring santé
    └── cleanup_cache.py        # Nettoyage automatique
Performances observées

Petites tables (<100 lignes) : <2s
Tables moyennes (2k lignes, 400+ colonnes) : 10-15s
Grosses tables (40k lignes, 350 colonnes) : 70-120s

Throughput typique : 500-700 lignes/seconde
Troubleshooting
Erreur : "Table n'existe pas dans ETL_Tables"
bashpython scripts/preflight_check.py <table_name>
Cache Parquet corrompu
bashpython scripts/cleanup_cache.py --days 0  # Force cleanup
Logs ETL
sql-- Dernières erreurs
SELECT TOP 20 * FROM etl.ETL_Log 
WHERE Status = 'failed' 
ORDER BY LogTs DESC;

-- Détail d'un run spécifique
SELECT * FROM etl.ETL_Log 
WHERE RunId = 'XXX-XXX-XXX'
ORDER BY LogTs;

---

### 2. Créer un guide d'exploitation (30min)

Crée `OPERATIONS.md` pour ton équipe :

# Guide d'exploitation ETL

## Démarrage rapide

### Activer environnement

D:\SQLServer\CBM_ETL\ETL\scripts\activate_etl.bat
Lancer ETL manuel
bash# Table unique
python flows/load_flow_simple.py produit incremental

# Toutes les tables
python flows/orchestrator.py --mode incremental
Vérifications quotidiennes
1. Health check (5 min)
bashpython scripts/daily_health_check.py
Alertes à surveiller :

Tables en retard (SLA breach)
Échecs récents (24h)
Performance dégradée

2. Logs SQL (2 min)
sql-- Tables non chargées aujourd'hui
SELECT TableName, LastSuccess
FROM etl.vw_LastRuns
WHERE CAST(LastSuccess AS DATE) < CAST(GETDATE() AS DATE);
Incidents fréquents
ETL échoue sur une table
1. Vérifier la cause
sqlSELECT TOP 1 ErrorMessage 
FROM etl.ETL_Log 
WHERE TableName = 'XXX' AND Status = 'failed'
ORDER BY LogTs DESC;
2. Actions selon erreur
ErreurCauseSolutionTimeout ODBCProgress surchargéRelancer plus tardIntegrityErrorDuplicate PKVérifier config PKString truncationVARCHAR trop courtAgrandir colonne SQLinvalid literal intType mal mappéVérifier meta.ProginovColumns
3. Relancer manuellement
bashpython flows/load_flow_simple.py <table> full
Cache Parquet plein
bash# Nettoyer fichiers >7 jours
python scripts/cleanup_cache.py --days 7

# Forcer nettoyage total
python scripts/cleanup_cache.py --days 0
Nouvelle table à ajouter
Voir section "Configuration métadonnées" dans README.md
Maintenance
Hebdomadaire

Vérifier etl.vw_TablePerformance pour dégradation
Purger anciens logs (>30 jours)

Mensuelle

Analyser volumétrie cache Parquet
Réviser SLA par table si besoin

