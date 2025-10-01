# CBM_ETL - Progress → SQL Server Pipeline

Pipeline ETL production-ready pour migration Progress OpenEdge vers SQL Server.

## Architecture

### Stack technique
- **Python 3.10+** | **Prefect 2.x** | **pyodbc** | **pandas** | **pyarrow**
- **Source** : Progress OpenEdge via ODBC
- **Destination** : SQL Server 2019+
- **Orchestration** : Prefect avec UI web
- **Alerting** : Microsoft Teams via Power Automate

### Flux de données

```
Progress (PUB schema)
    ↓ Extract (ODBC + alias SQL-safe)
Parquet Cache (compression snappy)
    ↓ Transform (hashdiff + timestamps techniques)
stg.* (staging temporaire)
    ↓ MERGE (upsert sur PK + hashdiff)
ods.* (tables finales historisées)
```

## Installation rapide

```bash
# 1. Cloner et configurer environnement
python -m venv prefect_env
prefect_env\Scripts\activate
pip install -r requirements.txt

# 2. Installer package en mode développement
pip install -e .

# 3. Configuration credentials
copy .env.template .env
# Éditer .env avec vos credentials Progress + SQL Server + Teams webhook

# 4. Vérifier environnement
python scripts\python\preflight_check.py

# 5. Démarrer serveur Prefect (dans une fenêtre séparée)
prefect server start

# 6. Tester une table
python src\flows\load_flow_simple.py produit full
```

## Utilisation

### Chargement table unique

```bash
# Mode incremental (delta depuis LastSuccessTs)
python src\flows\load_flow_simple.py produit incremental

# Mode full (rechargement complet)
python src\flows\load_flow_simple.py client full
```

### Orchestration multi-tables

```bash
# Toutes les tables par ordre de priorité (critical → high → normal)
python src\flows\orchestrator.py --mode incremental

# Continuer même si table critique échoue
python src\flows\orchestrator.py --mode incremental --continue-on-error
```

### Validation et monitoring

```bash
# Valider toutes les tables
python src\flows\validate_tables.py --filter all --mode full

# Health check quotidien (avec alerting automatique)
python scripts\python\daily_health_check.py

# Vérifier config avant ETL
python scripts\python\preflight_check.py client
```

### Scripts batch Windows

```bash
# Lancer ETL avec serveur Prefect
scripts\windows\run_etl.bat client full

# Workspace complet (serveur + UI + console)
scripts\windows\start_etl_workspace.bat

# ETL quotidien automatique
scripts\windows\daily_etl.bat
```

## Configuration métadonnées

### Ajouter une nouvelle table

1. **Synchroniser métadonnées Progress**

```sql
EXEC etl.SyncConfigColumns;
```

2. **Configurer dans config.ETL_Tables**

```sql
INSERT INTO config.ETL_Tables (
    TableName, DestinationTable, PrimaryKeyCols,
    HasTimestamps, DateModifCol, DateModifPrecision,
    IsDimension, IsFact, LookbackInterval
) VALUES (
    'nouvelle_table',           -- Nom table Progress
    'ods.nouvelle_table',       -- Table destination SQL Server
    'id',                       -- Clé primaire (ou multi : 'id1,id2')
    1,                          -- HasTimestamps (0 ou 1)
    'dat_mod',                  -- Colonne de modification
    'datetime',                 -- Précision : 'date' ou 'datetime'
    1,                          -- IsDimension (0 ou 1)
    0,                          -- IsFact (0 ou 1)
    '1d'                        -- Lookback : '0d', '1d', '12h', etc.
);
```

3. **Exclure colonnes inutiles** (optionnel)

```sql
UPDATE config.ETL_Columns
SET IsExcluded = 1,
    Notes = 'Colonne obsolète'
WHERE TableName = 'nouvelle_table' 
  AND ColumnName IN ('col_inutile1', 'col_inutile2');
```

4. **Tester**

```bash
python scripts\python\preflight_check.py nouvelle_table
python src\flows\load_flow_simple.py nouvelle_table full
```

### Gestion des colonnes à tirets

Progress autorise les tirets dans les noms (`gencod-v`), SQL Server non.

**Solution automatique :**
- `config.ETL_Columns.ColumnName` : Nom original (`gencod-v`)
- `config.ETL_Columns.SqlName` : Nom normalisé (`gencod_v`)
- `config.ETL_Columns.SourceExpression` : Alias extraction (`"gencod-v" AS gencod_v`)

Toutes les tables/colonnes sont créées automatiquement avec les noms SQL-safe.

## Monitoring production

### Vues SQL disponibles

```sql
-- Derniers runs par table
SELECT * FROM etl.vw_LastRuns;

-- Performance 7 derniers jours
SELECT * FROM etl.vw_TablePerformance;

-- Tables en retard (SLA breach)
SELECT * FROM etl.vw_SLABreach WHERE SLAStatus <> 'OK';
```

### Logs structurés

Tous les runs sont tracés dans `etl.ETL_Log` avec :
- **RunId** : UUID unique par exécution
- **Durée par étape** : extract, transform, load, merge
- **Nombre de lignes** traitées
- **Messages d'erreur** détaillés

### Alerting Teams

Alertes automatiques envoyées via webhook Teams :
- **SLA Breach** : Tables non chargées dans les délais
- **Échecs multiples** : >5 échecs sur 24h
- **Résumé quotidien** : Stats de la journée si tout OK

## Structure du projet

```
CBM_ETL/
├── .env                    # Credentials (gitignored)
├── .env.template           # Template configuration
├── README.md               # Ce fichier
├── requirements.txt        # Dépendances Python
├── setup.py                # Installation package
│
├── src/                    # Code source
│   ├── etl_logger.py      # Logging centralisé
│   ├── flows/             # Flows Prefect
│   │   ├── load_flow.py           # Flow avec orchestration
│   │   ├── load_flow_simple.py    # Flow standalone
│   │   ├── orchestrator.py        # Multi-tables
│   │   ├── validate_tables.py     # Validation batch
│   │   └── profiling_flow.py      # Profiling colonnes
│   ├── tasks/             # Tasks Prefect
│   │   ├── config_tasks.py
│   │   ├── extract_tasks.py
│   │   ├── transform_tasks.py
│   │   ├── staging_tasks.py
│   │   └── ods_tasks.py
│   └── utils/             # Utilitaires
│       ├── connections.py         # Pool connexions
│       ├── parquet_cache.py       # Gestion cache
│       ├── data_cleaning.py       # Transformations
│       ├── type_mapping.py        # Mapping types
│       └── alerting.py            # Alerting Teams
│
├── scripts/
│   ├── python/            # Scripts maintenance
│   │   ├── preflight_check.py
│   │   ├── daily_health_check.py
│   │   ├── cleanup_cache.py
│   │   └── test_alerting.py
│   └── windows/           # Batch Windows
│       ├── run_etl.bat
│       ├── daily_etl.bat
│       └── start_prefect_server.bat
│
├── data/
│   ├── cache/parquet/     # Cache Parquet temporaire
│   └── logs/              # Logs centralisés
│
├── reports/               # Rapports générés
│   ├── validation/
│   ├── orchestration/
│   └── profiling/
│
├── tests/                 # Tests isolés
├── docs/                  # Documentation
└── config/                # Configuration future (JSON)
```

## Performances observées

| Type de table | Volumétrie | Temps moyen |
|--------------|-----------|-------------|
| Petites tables | < 100 lignes | < 2s |
| Tables moyennes | 2k-15k lignes, 200-400 colonnes | 10-30s |
| Grosses tables | 40k+ lignes, 300+ colonnes | 70-150s |

**Throughput typique** : 500-700 lignes/seconde

**Tables validées** (5) : client, fournis, hisprixa, lignecli, produit  
**Volume 24h** : ~520k lignes traitées

## Troubleshooting

### Erreur : "Table n'existe pas dans ETL_Tables"

```bash
python scripts\python\preflight_check.py <table_name>
```

Si config manquante, exécuter :
```sql
EXEC etl.SyncConfigColumns;
-- Puis ajouter config dans config.ETL_Tables
```

### Cache Parquet corrompu

```bash
# Nettoyer fichiers >7 jours
python scripts\python\cleanup_cache.py --days 7

# Force cleanup total
python scripts\python\cleanup_cache.py --days 0
```

### Logs ETL

```sql
-- Dernières erreurs
SELECT TOP 20 * FROM etl.ETL_Log 
WHERE Status = 'failed' 
ORDER BY LogTs DESC;

-- Détail d'un run spécifique
SELECT * FROM etl.ETL_Log 
WHERE RunId = 'XXX-XXX-XXX'
ORDER BY LogTs;
```

### Serveur Prefect ne démarre pas

```bash
# Vérifier port 4200 disponible
netstat -ano | findstr :4200

# Réinitialiser config
prefect config unset PREFECT_API_URL
prefect server start
```

## Documentation

- **[OPERATIONS.md](docs/OPERATIONS.md)** : Guide d'exploitation quotidien
- **[ARCHITECTURE.md](docs/ARCHITECTURE.md)** : Architecture technique détaillée
- **[Prefect UI](http://127.0.0.1:4200)** : Dashboard orchestration (serveur local)

## Support

Pour questions ou incidents :
1. Vérifier logs dans `etl.ETL_Log`
2. Consulter alertes Teams
3. Lancer health check : `python scripts\python\daily_health_check.py`
4. Contacter équipe Data Engineering