/*
    File:        sql-server-handoff-checks-2012-2016.sql
    Author:      Misty Collins
    Purpose:     Read-only server and database handoff review for SQL Server 2012, 2014, and 2016
    Notes:       Run one section at a time. Review results before moving on.
                 Intended for DBA onboarding, server audits, and operational baselining.
    Safety:      This script is diagnostic only. It does not change configuration.
*/

SET NOCOUNT ON;

SELECT
    'Run this script section-by-section. Review each result set before continuing.' AS guidance;
RETURN;
GO


/*==============================================================
  SECTION 1 - Instance identity and version
==============================================================*/
SELECT
    @@SERVERNAME                          AS server_name,
    SERVERPROPERTY('MachineName')         AS machine_name,
    SERVERPROPERTY('ServerName')          AS sql_server_name,
    SERVERPROPERTY('InstanceName')        AS instance_name,
    SERVERPROPERTY('Edition')             AS edition,
    SERVERPROPERTY('ProductVersion')      AS product_version,
    SERVERPROPERTY('ProductLevel')        AS product_level,
    SERVERPROPERTY('EngineEdition')       AS engine_edition,
    SERVERPROPERTY('IsClustered')         AS is_clustered,
    SERVERPROPERTY('Collation')           AS server_collation;
GO


/*==============================================================
  SECTION 2 - Databases and basic state
==============================================================*/
SELECT
    d.name,
    d.database_id,
    d.state_desc,
    d.user_access_desc,
    d.recovery_model_desc,
    d.compatibility_level,
    d.containment_desc,
    d.page_verify_option_desc,
    d.is_auto_close_on,
    d.is_auto_shrink_on,
    d.is_read_only,
    d.create_date
FROM sys.databases AS d
ORDER BY d.name;
GO


/*==============================================================
  SECTION 3 - Full backup recency
==============================================================*/
SELECT
    d.name AS database_name,
    MAX(CASE WHEN b.type = 'D' THEN b.backup_finish_date END) AS last_full_backup,
    MAX(CASE WHEN b.type = 'I' THEN b.backup_finish_date END) AS last_diff_backup,
    MAX(CASE WHEN b.type = 'L' THEN b.backup_finish_date END) AS last_log_backup
FROM sys.databases AS d
LEFT JOIN msdb.dbo.backupset AS b
    ON b.database_name = d.name
WHERE d.database_id > 4
GROUP BY d.name
ORDER BY d.name;
GO


/*==============================================================
  SECTION 4 - Databases in FULL/BULK_LOGGED with no log backups found
==============================================================*/
SELECT
    d.name AS database_name,
    d.recovery_model_desc,
    MAX(b.backup_finish_date) AS last_log_backup
FROM sys.databases AS d
LEFT JOIN msdb.dbo.backupset AS b
    ON b.database_name = d.name
   AND b.type = 'L'
WHERE d.database_id > 4
  AND d.recovery_model_desc IN ('FULL', 'BULK_LOGGED')
GROUP BY d.name, d.recovery_model_desc
HAVING MAX(b.backup_finish_date) IS NULL
ORDER BY d.name;
GO


/*==============================================================
  SECTION 5 - Backup destination history
==============================================================*/
SELECT TOP (100)
    bs.database_name,
    bs.backup_start_date,
    bs.backup_finish_date,
    CASE bs.type
        WHEN 'D' THEN 'Full'
        WHEN 'I' THEN 'Differential'
        WHEN 'L' THEN 'Log'
        ELSE bs.type
    END AS backup_type,
    bmf.physical_device_name
FROM msdb.dbo.backupset AS bs
INNER JOIN msdb.dbo.backupmediafamily AS bmf
    ON bs.media_set_id = bmf.media_set_id
ORDER BY bs.backup_finish_date DESC;
GO


/*==============================================================
  SECTION 6 - Oldest backup history still in msdb
==============================================================*/
SELECT TOP (1)
    backup_set_id,
    backup_start_date,
    backup_finish_date,
    database_name,
    type
FROM msdb.dbo.backupset
ORDER BY backup_start_date ASC;
GO


/*==============================================================
  SECTION 7 - Database file layout and growth settings
==============================================================*/
SELECT
    DB_NAME(mf.database_id) AS database_name,
    mf.file_id,
    mf.type_desc,
    mf.name AS logical_name,
    mf.physical_name,
    CAST(mf.size / 128.0 AS DECIMAL(18,2)) AS current_size_mb,
    CASE
        WHEN mf.max_size = -1 THEN 'UNLIMITED'
        ELSE CAST(CAST(mf.max_size / 128.0 AS DECIMAL(18,2)) AS VARCHAR(50))
    END AS max_size_mb,
    CASE
        WHEN mf.is_percent_growth = 1 THEN CAST(mf.growth AS VARCHAR(20)) + '%'
        ELSE CAST(CAST(mf.growth / 128.0 AS DECIMAL(18,2)) AS VARCHAR(50)) + ' MB'
    END AS autogrowth_setting
FROM sys.master_files AS mf
ORDER BY DB_NAME(mf.database_id), mf.type_desc, mf.file_id;
GO


/*==============================================================
  SECTION 8 - Files on C: drive
==============================================================*/
SELECT
    DB_NAME(database_id) AS database_name,
    name AS logical_name,
    type_desc,
    physical_name
FROM sys.master_files
WHERE UPPER(physical_name) LIKE 'C:\%'
ORDER BY DB_NAME(database_id), type_desc, logical_name;
GO


/*==============================================================
  SECTION 9 - Suspect database settings
  Focus areas: AUTO_CLOSE, AUTO_SHRINK, PAGE_VERIFY
==============================================================*/
SELECT
    name,
    state_desc,
    recovery_model_desc,
    page_verify_option_desc,
    is_auto_close_on,
    is_auto_shrink_on
FROM sys.databases
WHERE database_id > 4
  AND (
        is_auto_close_on = 1
        OR is_auto_shrink_on = 1
        OR page_verify_option_desc <> 'CHECKSUM'
      )
ORDER BY name;
GO


/*==============================================================
  SECTION 10 - Last known CHECKDB date
  Note: Uses DBCC DBINFO WITH TABLERESULTS and requires elevated permissions.
==============================================================*/
IF OBJECT_ID('tempdb..#dbccinfo') IS NOT NULL DROP TABLE #dbccinfo;
IF OBJECT_ID('tempdb..#lastgood') IS NOT NULL DROP TABLE #lastgood;

CREATE TABLE #dbccinfo
(
    ParentObject NVARCHAR(255),
    [Object]     NVARCHAR(255),
    Field        NVARCHAR(255),
    [Value]      NVARCHAR(255)
);

CREATE TABLE #lastgood
(
    database_name SYSNAME,
    last_known_good DATETIME NULL
);

DECLARE @db SYSNAME;
DECLARE @sql NVARCHAR(MAX);

DECLARE db_cursor CURSOR LOCAL FAST_FORWARD FOR
SELECT name
FROM sys.databases
WHERE state_desc = 'ONLINE';

OPEN db_cursor;
FETCH NEXT FROM db_cursor INTO @db;

WHILE @@FETCH_STATUS = 0
BEGIN
    DELETE FROM #dbccinfo;

    SET @sql = N'USE ' + QUOTENAME(@db) + N';
                 INSERT INTO #dbccinfo
                 EXEC (''DBCC DBINFO WITH TABLERESULTS'');';

    BEGIN TRY
        EXEC sys.sp_executesql @sql;

        INSERT INTO #lastgood (database_name, last_known_good)
        SELECT
            @db,
            TRY_CONVERT(DATETIME, [Value])
        FROM #dbccinfo
        WHERE Field = 'dbi_dbccLastKnownGood';
    END TRY
    BEGIN CATCH
        INSERT INTO #lastgood (database_name, last_known_good)
        VALUES (@db, NULL);
    END CATCH;

    FETCH NEXT FROM db_cursor INTO @db;
END

CLOSE db_cursor;
DEALLOCATE db_cursor;

SELECT
    database_name,
    last_known_good
FROM #lastgood
ORDER BY
    CASE WHEN last_known_good IS NULL THEN 0 ELSE 1 END,
    last_known_good,
    database_name;
GO


/*==============================================================
  SECTION 11 - SQL Agent jobs and recent outcome
==============================================================*/
;WITH job_history AS
(
    SELECT
        h.job_id,
        h.run_status,
        h.run_date,
        h.run_time,
        h.run_duration,
        ROW_NUMBER() OVER
        (
            PARTITION BY h.job_id
            ORDER BY h.instance_id DESC
        ) AS rn
    FROM msdb.dbo.sysjobhistory AS h
    WHERE h.step_id = 0
)
SELECT
    j.name AS job_name,
    j.enabled,
    SUSER_SNAME(j.owner_sid) AS owner_name,
    CASE h.run_status
        WHEN 0 THEN 'Failed'
        WHEN 1 THEN 'Succeeded'
        WHEN 2 THEN 'Retry'
        WHEN 3 THEN 'Canceled'
        WHEN 4 THEN 'In Progress'
        ELSE 'Unknown'
    END AS last_run_status,
    msdb.dbo.agent_datetime(h.run_date, h.run_time) AS last_run_datetime,
    h.run_duration
FROM msdb.dbo.sysjobs AS j
LEFT JOIN job_history AS h
    ON j.job_id = h.job_id
   AND h.rn = 1
ORDER BY
    CASE WHEN j.enabled = 0 THEN 1 ELSE 0 END,
    j.name;
GO


/*==============================================================
  SECTION 12 - Recently failing jobs
==============================================================*/
;WITH job_history AS
(
    SELECT
        h.job_id,
        h.run_status,
        h.run_date,
        h.run_time,
        ROW_NUMBER() OVER
        (
            PARTITION BY h.job_id
            ORDER BY h.instance_id DESC
        ) AS rn
    FROM msdb.dbo.sysjobhistory AS h
    WHERE h.step_id = 0
)
SELECT
    j.name AS job_name,
    SUSER_SNAME(j.owner_sid) AS owner_name,
    msdb.dbo.agent_datetime(h.run_date, h.run_time) AS last_run_datetime,
    h.run_status
FROM msdb.dbo.sysjobs AS j
INNER JOIN job_history AS h
    ON j.job_id = h.job_id
   AND h.rn = 1
WHERE h.run_status = 0
ORDER BY last_run_datetime DESC, job_name;
GO


/*==============================================================
  SECTION 13 - Server role membership (high privilege review)
==============================================================*/
SELECT
    rp.name AS server_role,
    mp.name AS member_name,
    mp.type_desc AS member_type,
    mp.is_disabled
FROM sys.server_role_members AS srm
INNER JOIN sys.server_principals AS rp
    ON srm.role_principal_id = rp.principal_id
INNER JOIN sys.server_principals AS mp
    ON srm.member_principal_id = mp.principal_id
WHERE rp.name IN ('sysadmin', 'securityadmin', 'serveradmin')
ORDER BY rp.name, mp.name;
GO


/*==============================================================
  SECTION 14 - Database Mail profile/account summary
==============================================================*/
SELECT
    p.name AS profile_name,
    a.name AS account_name,
    a.email_address,
    s.servername,
    s.port,
    s.enable_ssl
FROM msdb.dbo.sysmail_profile AS p
LEFT JOIN msdb.dbo.sysmail_profileaccount AS pa
    ON p.profile_id = pa.profile_id
LEFT JOIN msdb.dbo.sysmail_account AS a
    ON pa.account_id = a.account_id
LEFT JOIN msdb.dbo.sysmail_server AS s
    ON a.account_id = s.account_id
ORDER BY p.name, a.name;
GO


/*==============================================================
  SECTION 15 - SQL Agent alerts
==============================================================*/
SELECT
    name,
    enabled,
    message_id,
    severity,
    delay_between_responses,
    include_event_description
FROM msdb.dbo.sysalerts
ORDER BY name;
GO


/*==============================================================
  SECTION 16 - Non-default sp_configure values
==============================================================*/
SELECT
    name,
    value,
    value_in_use,
    [description]
FROM sys.configurations
WHERE value <> value_in_use
   OR
      (
        name IN
        (
            'max degree of parallelism',
            'cost threshold for parallelism',
            'backup compression default',
            'optimize for ad hoc workloads',
            'clr enabled',
            'xp_cmdshell',
            'remote admin connections',
            'Database Mail XPs'
        )
      )
ORDER BY name;
GO


/*==============================================================
  SECTION 17 - Objects found in master and model
==============================================================*/
SELECT
    'master tables' AS check_area,
    name,
    create_date
FROM master.sys.tables
WHERE is_ms_shipped = 0

UNION ALL

SELECT
    'master procedures',
    name,
    create_date
FROM master.sys.procedures
WHERE is_ms_shipped = 0

UNION ALL

SELECT
    'model tables',
    name,
    create_date
FROM model.sys.tables
WHERE is_ms_shipped = 0

UNION ALL

SELECT
    'model procedures',
    name,
    create_date
FROM model.sys.procedures
WHERE is_ms_shipped = 0
ORDER BY check_area, name;
GO


/*==============================================================
  SECTION 18 - Trigger inventory across user databases
==============================================================*/
IF OBJECT_ID('tempdb..#triggers') IS NOT NULL DROP TABLE #triggers;

CREATE TABLE #triggers
(
    database_name SYSNAME,
    trigger_name SYSNAME,
    parent_object SYSNAME NULL,
    is_disabled BIT,
    is_instead_of_trigger BIT,
    create_date DATETIME,
    modify_date DATETIME
);

DECLARE @trigger_sql NVARCHAR(MAX);
DECLARE @trigger_db SYSNAME;

DECLARE trigger_cursor CURSOR LOCAL FAST_FORWARD FOR
SELECT name
FROM sys.databases
WHERE database_id > 4
  AND state_desc = 'ONLINE';

OPEN trigger_cursor;
FETCH NEXT FROM trigger_cursor INTO @trigger_db;

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @trigger_sql = N'
    USE ' + QUOTENAME(@trigger_db) + N';
    INSERT INTO #triggers
    (
        database_name,
        trigger_name,
        parent_object,
        is_disabled,
        is_instead_of_trigger,
        create_date,
        modify_date
    )
    SELECT
        DB_NAME(),
        t.name,
        OBJECT_NAME(t.parent_id),
        t.is_disabled,
        t.is_instead_of_trigger,
        t.create_date,
        t.modify_date
    FROM sys.triggers AS t
    WHERE t.parent_class_desc = ''OBJECT_OR_COLUMN'';';

    BEGIN TRY
        EXEC sys.sp_executesql @trigger_sql;
    END TRY
    BEGIN CATCH
        -- skip inaccessible/problem databases
    END CATCH;

    FETCH NEXT FROM trigger_cursor INTO @trigger_db;
END

CLOSE trigger_cursor;
DEALLOCATE trigger_cursor;

SELECT *
FROM #triggers
ORDER BY database_name, parent_object, trigger_name;
GO


/*==============================================================
  SECTION 19 - Backup throughput trend
==============================================================*/
SELECT
    bs.database_name,
    YEAR(bs.backup_finish_date)  AS backup_year,
    MONTH(bs.backup_finish_date) AS backup_month,
    COUNT(*) AS full_backup_count,
    CAST(AVG(
        CASE
            WHEN DATEDIFF(SECOND, bs.backup_start_date, bs.backup_finish_date) > 0
            THEN (bs.backup_size / 1048576.0) / DATEDIFF(SECOND, bs.backup_start_date, bs.backup_finish_date)
        END
    ) AS DECIMAL(18,2)) AS avg_mb_per_sec
FROM msdb.dbo.backupset AS bs
WHERE bs.type = 'D'
  AND bs.backup_size > 0
GROUP BY
    bs.database_name,
    YEAR(bs.backup_finish_date),
    MONTH(bs.backup_finish_date)
ORDER BY
    bs.database_name,
    backup_year DESC,
    backup_month DESC;
GO


/*==============================================================
  SECTION 20 - Buffer pool usage by database
  Warning: can be expensive on large-memory systems.
==============================================================*/
SELECT
    CASE
        WHEN database_id = 32767 THEN 'ResourceDB'
        ELSE DB_NAME(database_id)
    END AS database_name,
    COUNT(*) AS cached_pages,
    CAST(COUNT(*) * 8.0 / 1024 AS DECIMAL(18,2)) AS cached_mb
FROM sys.dm_os_buffer_descriptors
GROUP BY database_id
ORDER BY cached_pages DESC;
GO


/*==============================================================
  SECTION 21 - Fragmented indexes worth review
==============================================================*/
IF OBJECT_ID('tempdb..#frag') IS NOT NULL DROP TABLE #frag;
CREATE TABLE #frag
(
    database_name SYSNAME,
    schema_name SYSNAME,
    object_name SYSNAME,
    index_name SYSNAME,
    index_type_desc NVARCHAR(60),
    avg_fragmentation_in_percent FLOAT,
    page_count BIGINT
);

DECLARE @frag_db SYSNAME;
DECLARE @frag_sql NVARCHAR(MAX);

DECLARE frag_cursor CURSOR LOCAL FAST_FORWARD FOR
SELECT name
FROM sys.databases
WHERE database_id > 4
  AND state_desc = 'ONLINE'
  AND is_read_only = 0;

OPEN frag_cursor;
FETCH NEXT FROM frag_cursor INTO @frag_db;

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @frag_sql = N'
    USE ' + QUOTENAME(@frag_db) + N';
    INSERT INTO #frag
    (
        database_name,
        schema_name,
        object_name,
        index_name,
        index_type_desc,
        avg_fragmentation_in_percent,
        page_count
    )
    SELECT
        DB_NAME(),
        s.name,
        o.name,
        i.name,
        ips.index_type_desc,
        ips.avg_fragmentation_in_percent,
        ips.page_count
    FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, ''LIMITED'') AS ips
    INNER JOIN sys.indexes AS i
        ON ips.object_id = i.object_id
       AND ips.index_id = i.index_id
    INNER JOIN sys.objects AS o
        ON ips.object_id = o.object_id
    INNER JOIN sys.schemas AS s
        ON o.schema_id = s.schema_id
    WHERE ips.index_id > 0
      AND ips.page_count >= 1000
      AND ips.avg_fragmentation_in_percent >= 30
      AND o.type = ''U'';';

    BEGIN TRY
        EXEC sys.sp_executesql @frag_sql;
    END TRY
    BEGIN CATCH
        -- skip inaccessible/problem databases
    END CATCH;

    FETCH NEXT FROM frag_cursor INTO @frag_db;
END

CLOSE frag_cursor;
DEALLOCATE frag_cursor;

SELECT *
FROM #frag
ORDER BY avg_fragmentation_in_percent DESC, page_count DESC;
GO
