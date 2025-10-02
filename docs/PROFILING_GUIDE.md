# Guide Profiling Colonnes ETL

## Vue d'ensemble

Le **profiling intelligent** analyse **TOUTES les données** en staging pour identifier les colonnes inutiles :
- ❌ Colonnes à >90% vides
- ❌ Colonnes statiques (≤1 valeur distincte)
- ❌ Colonnes à faible cardinalité (<1% distinct)

### Avantages

✅ **Performances** : Réduction taille tables ODS  
✅ **Automatique** : Vérification tous les 14 jours  
✅ **Intelligent** : Analyse sur données COMPLÈTES (pas échantillon)  
✅ **Traçabilité** : Historique profiling dans `config.ETL_Tables`

---

## Installation

### 1. Migration SQL

```bash
sqlcmd -S localhost -d CBM_ETL -E -i scripts\sql\migration_add_profiling_ts.sql
```

Ou exécuter directement dans SSMS :
```sql
ALTER TABLE config.ETL_Tables ADD LastProfilingTs DATETIME2 NULL;
```

### 2. Vérification

```sql
-- Tables nécessitant profiling
SELECT * FROM etl.vw_ProfilingNeeded WHERE ProfilingStatus <> 'Profiling récent';
```

---

## Utilisation

### Mode 1 : Profiling manuel d'une table

```bash
# Profiling simple (demande confirmation avant exclusions)
python src\flows\profiling_flow_v2.py produit

# Profiling forcé (même si récent)
python src\flows\profiling_flow_v2.py produit --force

# Profiling avec auto-apply (sans confirmation)
python src\flows\profiling_flow_v2.py client --auto-apply

# Personnaliser seuil % vide
python src\flows\profiling_flow_v2.py lignecli --min-empty-pct 85
```

**Ou via batch Windows :**
```batch
scripts\windows\run_profiling.bat produit
scripts\windows\run_profiling.bat client --force --auto-apply
```

---

### Mode 2 : Profiling intégré dans ETL

Le profiling peut s'exécuter automatiquement après le chargement staging :

```bash
# ETL classique (sans profiling)
python src\flows\load_flow_simple.py produit full

# ETL avec profiling automatique
python src\flows\load_flow_simple.py produit full --enable-profiling

# ETL avec profiling et seuil personnalisé
python src\flows\load_flow_simple.py client incremental --enable-profiling --profiling-days 7
```

**Comportement :**
1. Extract → Staging FULL
2. **Profiling si LastProfilingTs > 14j**
3. Exclusions appliquées automatiquement
4. Transform avec colonnes mises à jour
5. Merge ODS

---

### Mode 3 : Batch profiling (toutes les tables obsolètes)

Profile toutes les tables non profilées depuis >14 jours :

```bash
# Profiling batch standard
python scripts\python\batch_profiling.py

# Personnaliser seuil
python scripts\python\batch_profiling.py --days 30

# Sans auto-apply (demande confirmation)
python scripts\python\batch_profiling.py --no-auto-apply

# Limiter nombre de tables (tests)
python scripts\python\batch_profiling.py --max-tables 5
```

**Sortie :**
- Rapport console avec résumé
- CSV détaillé dans `profiling_reports/batch_profiling_YYYYMMDD_HHMMSS.csv`
- Alerte Teams si échecs

---

## Critères d'exclusion

Une colonne est **recommandée pour exclusion** si :

| Critère | Seuil | Exemple |
|---------|-------|---------|
| **% vide** | ≥90% | 95% NULL ou chaînes vides |
| **Statique** | ≤1 valeur | Toutes les lignes = 'ACTIF' |
| **Faible cardinalité** | <1% distinct ET <10 valeurs | 3 valeurs sur 10k lignes |

**Exemples réels :**
```
❌ col_obsolete     : 98.5% vide
❌ flag_constant    : Statique (1 valeur : 'Y')
❌ type_fixe        : Faible cardinalité (2 valeurs)
```

---

## Rapports générés

### 1. Rapport CSV complet

**Emplacement :** `profiling_reports/profiling_<table>_<timestamp>.csv`

**Colonnes :**
- `ColumnName` : Nom colonne
- `DataType` : Type SQL
- `TotalRows` : Nombre lignes analysées
- `NullCount` : Nombre NULL
- `EmptyCount` : Nombre chaînes vides
- `EmptyPct` : % vide total
- `DistinctCount` : Valeurs distinctes
- `DistinctRatio` : Ratio distinct/total
- `RecommendExclude` : True/False
- `Reason` : Raison exclusion

**Exemple :**
```csv
ColumnName,DataType,TotalRows,EmptyPct,DistinctCount,RecommendExclude,Reason
cod_pro,nvarchar,15000,0.0,15000,False,Colonne active
lib_obsolete,nvarchar,15000,95.2,12,True,95.2% vide
flag_actif,bit,15000,0.0,1,True,Statique (≤1 valeur)
```

---

### 2. Mise à jour `config.ETL_Columns`

Les exclusions sont appliquées automatiquement (si `--auto-apply`) :

```sql
UPDATE config.ETL_Columns
SET IsExcluded = 1,
    Notes = 'Auto-profiling: 95.2% vide | Statique'
WHERE TableName = 'produit' AND ColumnName = 'lib_obsolete';
```

---

### 3. Mise à jour `config.ETL_Tables`

```sql
UPDATE config.ETL_Tables
SET LastProfilingTs = '2025-10-02 14:30:00'
WHERE TableName = 'produit';
```

---

## Monitoring

### Vérifier tables nécessitant profiling

```sql
-- Vue synthétique
SELECT 
    TableName,
    LastProfilingTs,
    DATEDIFF(day, LastProfilingTs, GETDATE()) AS DaysAgo,
    CASE 
        WHEN LastProfilingTs IS NULL THEN 'Jamais profilé'
        WHEN DATEDIFF(day, LastProfilingTs, GETDATE()) >= 14 THEN 'Obsolète'
        ELSE 'Récent'
    END AS Status
FROM config.ETL_Tables
ORDER BY DaysAgo DESC;
```

### Dashboard recommandé

**Grafana / Power BI :**
- 📊 Nombre tables jamais profilées
- ⏰ Nombre tables profiling obsolète (>14j)
- 📈 Évolution colonnes exclues dans le temps
- 🎯 Taux compression tables ODS (avant/après)

---

## Cas d'usage

### Scénario 1 : Nouvelle table ajoutée

```bash
# 1. Ajouter config dans ETL_Tables
INSERT INTO config.ETL_Tables (TableName, ...) VALUES ('nouvelle_table', ...);

# 2. Charger en FULL pour remplir staging
python src\flows\load_flow_simple.py nouvelle_table full

# 3. Profiler
python src\flows\profiling_flow_v2.py nouvelle_table --auto-apply

# 4. Re-charger avec colonnes optimisées
python src\flows\load_flow_simple.py nouvelle_table full
```

---

### Scénario 2 : Re-profiling mensuel

**Tâche planifiée Windows (mensuelle) :**

```batch
REM batch_profiling_monthly.bat
python scripts\python\batch_profiling.py --days 30 --auto-apply
```

**Scheduler Windows :**
- Déclencheur : 1er jour du mois, 2h du matin
- Action : `D:\SQLServer\CBM_ETL\ETL\scripts\windows\batch_profiling_monthly.bat`

---

### Scénario 3 : Profiling intégré ETL quotidien

**Modifier `scripts\windows\daily_etl.bat` :**

```batch
REM Orchestrateur avec profiling auto
python src\flows\orchestrator.py --mode incremental --enable-profiling
```

Ou modifier `orchestrator.py` pour activer profiling sur tables FULL uniquement.

---

## FAQ

### Q : Quelle est la différence avec l'ancien `profiling_flow.py` ?

| Critère | Ancien | Nouveau (v2) |
|---------|--------|--------------|
| Données analysées | Échantillon 100k lignes | **TOUTES** les données staging |
| Vérification date | ❌ Non | ✅ Oui (14j par défaut) |
| Mise à jour `LoadTs` | ❌ Non | ✅ Oui |
| Intégration ETL | ❌ Non | ✅ Optionnelle |
| Force profiling | ❌ Non | ✅ Oui (`--force`) |
| Auto-apply | ❌ Non | ✅ Oui (`--auto-apply`) |

---

### Q : Le profiling ralentit-il l'ETL ?

**Non, car :**
- Profiling **optionnel** (activé manuellement)
- Exécuté **uniquement si >14j** (pas à chaque run)
- Analyse staging déjà chargé (pas de requête Progress)
- Durée : ~10-30s pour table moyenne (15k lignes, 300 cols)

---

### Q : Que se passe-t-il si j'exclus une colonne par erreur ?

Réactiver manuellement :

```sql
UPDATE config.ETL_Columns
SET IsExcluded = 0,
    Notes = 'Réactivation manuelle'
WHERE TableName = 'produit' AND ColumnName = 'col_importante';
```

Puis re-charger en FULL pour inclure la colonne dans ODS.

---

### Q : Comment ajuster les seuils d'exclusion ?

**Modifier seuil % vide (défaut 90%) :**
```bash
python src\flows\profiling_flow_v2.py produit --min-empty-pct 95
```

**Modifier code pour cardinalité :**
Éditer `src/flows/profiling_flow_v2.py` :
```python
min_distinct_ratio: float = 0.01  # 1% par défaut
```

---

### Q : Profiling sur table Progress directement (sans staging) ?

**Non recommandé** pour 2 raisons :
1. Impact performance Progress (lecture complète table)
2. Risque timeout ODBC sur grosses tables

**Solution actuelle** : Profiling sur staging après chargement FULL.

---

## Bonnes pratiques

✅ **DO :**
- Profiler nouvelles tables après 1er chargement FULL
- Re-profiler tous les 1-3 mois (données évolutives)
- Vérifier rapports CSV avant d'appliquer exclusions critiques
- Utiliser `--auto-apply` en batch, confirmation manuelle en dev

❌ **DON'T :**
- Profiler tables sans staging (requiert FULL load avant)
- Ignorer colonnes PK dans exclusions (vérif automatique)
- Profiler trop souvent (seuil <7j inutile, données stables)

---

## Support

**Logs profiling :**
```sql
SELECT * FROM etl.ETL_Log 
WHERE StepName LIKE 'profiling%' 
ORDER BY LogTs DESC;
```

**Rapports :**
- Console : Affichage temps réel
- CSV : `profiling_reports/profiling_<table>_<timestamp>.csv`
- SQL : `config.ETL_Columns.Notes` (raison exclusion)

**Contact :** Équipe Data Engineering