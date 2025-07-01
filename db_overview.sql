-- Database overview report (suitable for registered server groups)

SELECT
    db.database_id                                  AS ID,
    DB_NAME(db.database_id)                         AS DatabaseName,
    ''                                              AS SystemName,
    ''                                              AS SystemOwner,
    ''                                              AS SystemContact,
    ''                                              AS SystemDepartment,
    ''                                              AS SystemDependencies,
    ''                                              AS ApplicationHosts,
    ''                                              AS AccessMonitorNotes,
    ''                                              AS AccessMonitorLogins,
    ''                                              AS CNAME,
    db.compatibility_level                          AS DB_Compatibility_Level,

    -- Size calculations (in MB)
    (CAST(mfrows.RowSize AS FLOAT) * 8) / 1024      AS DatabaseDataSizeMB,
    (CAST(mflog.LogSize AS FLOAT) * 8) / 1024       AS DatabaseLogSizeMB,
    ((mfrows.RowSize + mflog.LogSize) * 8) / 1024   AS TotalDatabaseSizeMB,

    db.recovery_model_desc                          AS RecoveryModel,
    'PROD'                                          AS Environment, -- Consider logic for environment detection
    ''                                              AS SLA,
    ''                                              AS ServiceWindow,
    SERVERPROPERTY('ServerName')                    AS ServerName,

    -- SQL Server version parsing
    CASE
        WHEN @@VERSION LIKE '%2000%8.%'         THEN '2000'
        WHEN @@VERSION LIKE '%2005%9.%'         THEN '2005'
        WHEN @@VERSION LIKE '%2008%10.0%'       THEN '2008'
        WHEN @@VERSION LIKE '%2008 R2%10.5%'    THEN '2008 R2'
        WHEN @@VERSION LIKE '%2012%11.%'        THEN '2012'
        WHEN @@VERSION LIKE '%2014%12.%'        THEN '2014'
        WHEN @@VERSION LIKE '%2016%13.%'        THEN '2016'
        WHEN @@VERSION LIKE '%2017%14.%'        THEN '2017'
        WHEN @@VERSION LIKE '%2019%15.%'        THEN '2019'
        WHEN @@VERSION LIKE '%2022%16.%'        THEN '2022'
        WHEN @@VERSION LIKE '%2025%17.%'        THEN '2025'
    END                                            AS SQLVersion,

    SERVERPROPERTY('Edition')                      AS Edition,
    DATABASEPROPERTYEX(name, 'Collation')          AS Collation,

    -- Migration metadata placeholders
    ''                                              AS NewServer,
    ''                                              AS NewServerVersion,
    ''                                              AS NewCNAME,
    ''                                              AS MigrationReadyDMA,
    ''                                              AS AGName,
    ''                                              AS RelatedSqlObjects,
    ''                                              AS Comments,
    ''                                              AS MigrationStatus,

    -- Isolation and other database settings
    db.snapshot_isolation_state_desc               AS SnapshotIsolation,
    db.is_read_committed_snapshot_on               AS RCSI_Enabled,
    db.state_desc                                   AS State,
    db.is_encrypted                                 AS IsEncrypted,
    db.is_broker_enabled                            AS ServiceBrokerEnabled,
    db.delayed_durability_desc                      AS DelayedDurability

FROM sys.databases db
    LEFT JOIN (
    SELECT database_id, SUM(size) AS RowSize
    FROM sys.master_files
    WHERE type = 0
    GROUP BY database_id
) mfrows ON mfrows.database_id = db.database_id

    LEFT JOIN (
    SELECT database_id, SUM(size) AS LogSize
    FROM sys.master_files
    WHERE type = 1
    GROUP BY database_id
) mflog ON mflog.database_id = db.database_id

WHERE db.name NOT IN ('master', 'model', 'tempdb', 'msdb');
