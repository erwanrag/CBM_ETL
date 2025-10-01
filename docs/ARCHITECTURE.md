**Logique CDC :**
- Si PK existe + hashdiff différent → UPDATE (ligne modifiée)
- Si PK existe + hashdiff identique → Rien (ligne inchangée)
- Si PK n'existe pas → INSERT (nouvelle ligne)

### Étape 6 : Update LastSuccessTs

```sql
UPDATE config.ETL_Tables
SET LastSuccessTs = GETDATE()
WHERE TableName = 'produit';
```

## Décisions de design

### Pourquoi Parquet intermédiaire ?

**Avantages :**
1. **Découplage** : Extract peut réussir même si SQL Server indisponible
2. **Performance** : Compression snappy réduit I/O (ratio ~4:1)
3. **Rejouabilité** : Retransformer sans re-extraire Progress
4. **Debug** : Inspecter données brutes facilement

**Inconvénient :**
- Espace disque (10-50 MB par table, nettoyé automatiquement >7j)

### Pourquoi hashdiff au lieu de timestamps ?

**Problème timestamps :**
- Certaines tables n'ont pas de colonne modification
- Timestamps peuvent être incorrects (modif manuel)
- Ne détecte pas changements sans changement de timestamp

**Solution hashdiff :**
- Détecte **tout** changement de données
- Fonctionne même sans colonne timestamp
- Coût : 40 bytes par ligne (négligeable)

### Pourquoi staging intermédiaire ?

**Avantages :**
1. **Atomicité** : MERGE en une transaction
2. **Performance** : Index sur PK pour MERGE rapide
3. **Rollback** : Si MERGE échoue, ODS intact
4. **Validation** : Vérifier données avant merge

**Alternative rejetée :**
- INSERT direct dans ODS : Risque corruption si échec partiel

### Gestion colonnes à tirets

**Problème :**
- Progress : `SELECT "gencod-v" FROM produit` ✅
- SQL Server : `CREATE TABLE ... [gencod-v] INT` ❌

**Solution :**
1. **Extract** : Alias SQL-safe (`"gencod-v" AS gencod_v`)
2. **Storage** : Tout en underscores dans Parquet/SQL
3. **Config** : Mapping ColumnName ↔ SqlName ↔ SourceExpression

## Performance

### Optimisations appliquées

**Extract :**
- Requêtes incrémentales (WHERE sur timestamp)
- Sélection colonnes explicite (pas de SELECT *)
- Projection poussée dans Progress

**Transform :**
- Parquet avec compression snappy
- Types optimisés (Int8/Int16/Int32 au lieu de Int64)

**Load :**
- `fast_executemany` pyodbc
- Batch 1000 lignes
- Index sur PK staging pour MERGE

**MERGE :**
- Index clustered sur PK ODS
- Comparaison hashdiff évite UPDATE inutiles

### Bottlenecks identifiés

1. **Progress ODBC** : Limite principale (débit ~700 lignes/s)
2. **Réseau** : Si Progress distant, latence impact
3. **MERGE** : Sur grosses tables (>100k lignes), peut être lent

**Solutions :**
- Fenêtres incrémentales courtes (12h-24h)
- Parallélisation possible (si plusieurs tables indépendantes)
- Partition tables ODS si >1M lignes (futur)

## Sécurité

### Credentials

- **Stockage** : `.env` (gitignored)
- **Accès** : Service account dédiés (ODBCREADER Progress, ETL_User SQL)
- **Rotation** : Manuelle (tous les 6 mois)

### Permissions SQL Server

```sql
-- Utilisateur ETL minimum
CREATE USER ETL_User WITHOUT LOGIN;
GRANT SELECT ON SCHEMA::meta TO ETL_User;
GRANT SELECT ON SCHEMA::config TO ETL_User;
GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA::stg TO ETL_User;
GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA::ods TO ETL_User;
GRANT INSERT ON SCHEMA::etl TO ETL_User;
```

### Audit

- Tous les runs loggés dans `etl.ETL_Log`
- Alertes critiques dans `etl.ETL_Alerts`
- Logs conservés 30 jours

## Évolutions futures

### Court terme (Q1 2025)

- [ ] Ajouter 15-20 tables Progress restantes
- [ ] Dashboard Grafana sur vues monitoring
- [ ] Email backup si Teams webhook indisponible

### Moyen terme (Q2 2025)

- [ ] Parallélisation flows (Dask ou Prefect workers)
- [ ] Historisation complète (SCD Type 2) pour tables dimension
- [ ] Tests automatisés (pytest + fixtures SQL)

### Long terme (Q3-Q4 2025)

- [ ] Migration vers Databricks/Spark si volumétrie x10
- [ ] Data quality checks (Great Expectations)
- [ ] Lineage tracking (OpenLineage)

## Références

- **Prefect docs** : https://docs.prefect.io
- **pandas pyarrow** : https://arrow.apache.org/docs/python
- **SQL Server MERGE** : https://learn.microsoft.com/sql/t-sql/statements/merge-transact-sql