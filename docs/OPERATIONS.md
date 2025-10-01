## Commandes utiles

```bash
# Lister tables configurées
SELECT TableName, DestinationTable, LastSuccessTs 
FROM config.ETL_Tables 
ORDER BY TableName;

# Statistiques dernières 24h
SELECT 
    COUNT(DISTINCT TableName) AS TablesProcessed,
    SUM(RowsProcessed) AS TotalRows,
    AVG(DurationSeconds) AS AvgDuration
FROM etl.ETL_Log
WHERE Status = 'success'
  AND StepName = 'flow_complete'
  AND LogTs >= DATEADD(hour, -24, GETDATE());

# Top 10 tables les plus volumineuses
SELECT TOP 10
    t.name AS TableName,
    SUM(p.rows) AS RowCount,
    SUM(a.total_pages) * 8 / 1024 AS SizeMB
FROM sys.tables t
INNER JOIN sys.partitions p ON t.object_id = p.object_id
INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
WHERE t.schema_id = SCHEMA_ID('ods')
  AND p.index_id IN (0,1)
GROUP BY t.name
ORDER BY SizeMB DESC;
```

## Contact & Support

**Équipe Data Engineering**
- Email : data-engineering@cbm.fr
- Teams : Canal #etl-support
- Astreinte : +33 X XX XX XX XX

**Escalade incidents**
1. **Niveau 1** (0-2h) : Relancer ETL, vérifier logs
2. **Niveau 2** (2-4h) : Contacter DBA Progress/SQL Server
3. **Niveau 3** (>4h) : Escalade manager + analyse approfondie