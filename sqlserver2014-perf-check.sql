/*
==============================================================================
  Script:        sqlserver2014-perf-check.sql
  Author:        Misty Collins
  Compatible with:SQL Server 2014 (and later)
  Run as: sysadmin or with VIEW SERVER STATE permission
==============================================================================
*/

PRINT '============================================================';
PRINT ' SQL SERVER 2014 PERFORMANCE HEALTH CHECK';
PRINT ' Run Date: ' + CONVERT(VARCHAR, GETDATE(), 120);
PRINT '============================================================';

-- ============================================================
-- 1. INSTANCE OVERVIEW
-- ============================================================
PRINT '';
PRINT '--- [1] INSTANCE OVERVIEW ---';

SELECT
    @@SERVERNAME                                        AS ServerName,
    @@VERSION                                           AS SQLVersion,
    SERVERPROPERTY('Edition')                           AS Edition,
    SERVERPROPERTY('ProductLevel')                      AS ServicePack,
    cpu_count                                           AS LogicalCPUs,
    hyperthread_ratio,
    cpu_count / hyperthread_ratio                       AS PhysicalCores,
    physical_memory_kb / 1024                           AS PhysicalMemory_MB,
    sqlserver_start_time                                AS LastRestart,
    DATEDIFF(HOUR, sqlserver_start_time, GETDATE())     AS UptimeHours
FROM sys.dm_os_sys_info;


-- ============================================================
-- 2. MEMORY PRESSURE
-- ============================================================
PRINT '';
PRINT '--- [2] MEMORY PRESSURE ---';

-- Buffer Pool Usage
SELECT
    physical_memory_in_use_kb / 1024    AS MemoryUsed_MB,
    page_fault_count,
    memory_utilization_percentage
FROM sys.dm_os_process_memory;

-- Page Life Expectancy (Target: >300s, ideally >1000s per 4GB RAM)
SELECT
    object_name,
    counter_name,
    cntr_value                          AS PageLifeExpectancy_Seconds
FROM sys.dm_os_performance_counters
WHERE counter_name = 'Page life expectancy'
  AND object_name LIKE '%Buffer Manager%';

-- Memory Grants Pending (should be 0 or very low)
SELECT
    counter_name,
    cntr_value
FROM sys.dm_os_performance_counters
WHERE counter_name IN ('Memory Grants Pending', 'Memory Grants Outstanding')
  AND object_name LIKE '%Memory Manager%';

-- Top 10 Memory-Consuming Queries (cached plans)
SELECT TOP 10
    qs.total_grant_kb / 1024            AS TotalGrant_MB,
    qs.execution_count,
    qs.total_grant_kb / qs.execution_count / 1024 AS AvgGrant_MB,
    SUBSTRING(qt.text, (qs.statement_start_offset/2)+1,
        ((CASE qs.statement_end_offset WHEN -1 THEN DATALENGTH(qt.text)
          ELSE qs.statement_end_offset END - qs.statement_start_offset)/2)+1)
                                        AS QueryText,
    DB_NAME(qt.dbid)                    AS DatabaseName
FROM sys.dm_exec_query_stats qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) qt
ORDER BY qs.total_grant_kb DESC;


-- ============================================================
-- 3. CPU PRESSURE
-- ============================================================
PRINT '';
PRINT '--- [3] CPU PRESSURE ---';

-- Signal Wait % (high signal wait = CPU pressure; target <25%)
SELECT
    CAST(100.0 * SUM(signal_wait_time_ms) / NULLIF(SUM(wait_time_ms), 0) AS DECIMAL(5,2))
                                        AS SignalWaitPct,
    CAST(100.0 * SUM(wait_time_ms - signal_wait_time_ms) / NULLIF(SUM(wait_time_ms), 0) AS DECIMAL(5,2))
                                        AS ResourceWaitPct
FROM sys.dm_os_wait_stats
WHERE wait_type NOT IN (
    'SLEEP_TASK','BROKER_TO_FLUSH','BROKER_TASK_STOP','CLR_AUTO_EVENT',
    'DISPATCHER_QUEUE_SEMAPHORE','FT_IFTS_SCHEDULER_IDLE_WAIT',
    'HADR_FILESTREAM_IOMGR_IOCOMPLETION','HADR_WORK_QUEUE',
    'HADR_CLUSAPI_CALL','HADR_TIMER_TASK','HADR_TRANSPORT_DBREC_UNLOAD',
    'ONDEMAND_TASK_QUEUE','REQUEST_FOR_DEADLOCK_SEARCH','RESOURCE_QUEUE',
    'SERVER_IDLE_CHECK','SLEEP_DBSTARTUP','SLEEP_DCOMSTARTUP',
    'SLEEP_MASTERDBREADY','SLEEP_MASTERMDREADY','SLEEP_MASTERUPGRADED',
    'SLEEP_MSDBSTARTUP','SLEEP_TEMPDBSTARTUP','SNI_HTTP_ACCEPT',
    'SP_SERVER_DIAGNOSTICS_SLEEP','SQLTRACE_BUFFER_FLUSH','WAITFOR',
    'WAIT_XTP_OFFLINE_CKPT_NEW_LOG','XE_DISPATCHER_WAIT','XE_TIMER_EVENT',
    'LOGMGR_QUEUE','CHECKPOINT_QUEUE','DBMIRROR_EVENTS_QUEUE',
    'SQLTRACE_INCREMENTAL_FLUSH_SLEEP','BROKER_EVENTHANDLER',
    'BROKER_TRANSMITTER','SLEEP_DBRECLAIMID'
);

-- Top 10 CPU-consuming queries
SELECT TOP 10
    qs.total_worker_time / 1000         AS TotalCPU_ms,
    qs.execution_count,
    qs.total_worker_time / qs.execution_count / 1000 AS AvgCPU_ms,
    qs.total_elapsed_time / qs.execution_count / 1000 AS AvgDuration_ms,
    DB_NAME(qt.dbid)                    AS DatabaseName,
    SUBSTRING(qt.text, (qs.statement_start_offset/2)+1,
        ((CASE qs.statement_end_offset WHEN -1 THEN DATALENGTH(qt.text)
          ELSE qs.statement_end_offset END - qs.statement_start_offset)/2)+1)
                                        AS QueryText
FROM sys.dm_exec_query_stats qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) qt
ORDER BY qs.total_worker_time DESC;


-- ============================================================
-- 4. TOP WAIT STATISTICS
-- ============================================================
PRINT '';
PRINT '--- [4] TOP WAIT STATISTICS (since last restart) ---';

SELECT TOP 15
    wait_type,
    wait_time_ms / 1000                 AS WaitTime_Sec,
    signal_wait_time_ms / 1000          AS SignalWait_Sec,
    waiting_tasks_count                 AS WaitingTasks,
    CAST(100.0 * wait_time_ms / SUM(wait_time_ms) OVER ()
                                AS DECIMAL(5,2)) AS WaitPct
FROM sys.dm_os_wait_stats
WHERE wait_type NOT IN (
    'SLEEP_TASK','BROKER_TO_FLUSH','BROKER_TASK_STOP','CLR_AUTO_EVENT',
    'DISPATCHER_QUEUE_SEMAPHORE','FT_IFTS_SCHEDULER_IDLE_WAIT',
    'HADR_FILESTREAM_IOMGR_IOCOMPLETION','HADR_WORK_QUEUE',
    'HADR_CLUSAPI_CALL','HADR_TIMER_TASK','HADR_TRANSPORT_DBREC_UNLOAD',
    'ONDEMAND_TASK_QUEUE','REQUEST_FOR_DEADLOCK_SEARCH','RESOURCE_QUEUE',
    'SERVER_IDLE_CHECK','SLEEP_DBSTARTUP','SLEEP_DCOMSTARTUP',
    'SLEEP_MASTERDBREADY','SLEEP_MASTERMDREADY','SLEEP_MASTERUPGRADED',
    'SLEEP_MSDBSTARTUP','SLEEP_TEMPDBSTARTUP','SNI_HTTP_ACCEPT',
    'SP_SERVER_DIAGNOSTICS_SLEEP','SQLTRACE_BUFFER_FLUSH','WAITFOR',
    'WAIT_XTP_OFFLINE_CKPT_NEW_LOG','XE_DISPATCHER_WAIT','XE_TIMER_EVENT',
    'LOGMGR_QUEUE','CHECKPOINT_QUEUE','DBMIRROR_EVENTS_QUEUE',
    'SQLTRACE_INCREMENTAL_FLUSH_SLEEP','BROKER_EVENTHANDLER',
    'BROKER_TRANSMITTER','SLEEP_DBRECLAIMID'
)
  AND wait_time_ms > 0
ORDER BY wait_time_ms DESC;


-- ============================================================
-- 5. I/O PERFORMANCE
-- ============================================================
PRINT '';
PRINT '--- [5] I/O PERFORMANCE ---';

-- Per-file I/O latency (target: <20ms reads, <30ms writes; >50ms = problem)
SELECT
    DB_NAME(vfs.database_id)            AS DatabaseName,
    mf.physical_name                    AS FilePath,
    mf.type_desc                        AS FileType,
    vfs.io_stall_read_ms,
    vfs.num_of_reads,
    CASE WHEN vfs.num_of_reads = 0 THEN 0
         ELSE vfs.io_stall_read_ms / vfs.num_of_reads END
                                        AS AvgReadLatency_ms,
    vfs.io_stall_write_ms,
    vfs.num_of_writes,
    CASE WHEN vfs.num_of_writes = 0 THEN 0
         ELSE vfs.io_stall_write_ms / vfs.num_of_writes END
                                        AS AvgWriteLatency_ms,
    vfs.io_stall                        AS TotalIOStall_ms
FROM sys.dm_io_virtual_file_stats(NULL, NULL) vfs
JOIN sys.master_files mf
    ON vfs.database_id = mf.database_id AND vfs.file_id = mf.file_id
ORDER BY vfs.io_stall DESC;


-- ============================================================
-- 6. INDEX HEALTH
-- ============================================================
PRINT '';
PRINT '--- [6] INDEX HEALTH ---';

-- Missing indexes (potential quick wins)
SELECT TOP 20
    DB_NAME(mid.database_id)            AS DatabaseName,
    OBJECT_NAME(mid.object_id, mid.database_id) AS TableName,
    migs.avg_total_user_cost * migs.avg_user_impact * (migs.user_seeks + migs.user_scans)
                                        AS ImprovementScore,
    migs.user_seeks,
    migs.user_scans,
    migs.avg_user_impact                AS EstimatedImpactPct,
    mid.equality_columns,
    mid.inequality_columns,
    mid.included_columns,
    'CREATE INDEX [IX_' + OBJECT_NAME(mid.object_id, mid.database_id) + '_Missing_'
        + CAST(mid.index_handle AS VARCHAR) + '] ON '
        + mid.statement + ' (' + ISNULL(mid.equality_columns,'')
        + CASE WHEN mid.inequality_columns IS NOT NULL
               THEN CASE WHEN mid.equality_columns IS NOT NULL THEN ',' ELSE '' END
                    + mid.inequality_columns ELSE '' END + ')'
        + ISNULL(' INCLUDE (' + mid.included_columns + ')','')
                                        AS CreateIndexStatement
FROM sys.dm_db_missing_index_groups mig
JOIN sys.dm_db_missing_index_group_stats migs ON mig.index_group_handle = migs.group_handle
JOIN sys.dm_db_missing_index_details mid ON mig.index_handle = mid.index_handle
ORDER BY ImprovementScore DESC;

-- Unused indexes (wasting write overhead)
SELECT TOP 20
    DB_NAME()                           AS DatabaseName,
    OBJECT_NAME(i.object_id)           AS TableName,
    i.name                             AS IndexName,
    i.type_desc                        AS IndexType,
    ius.user_seeks,
    ius.user_scans,
    ius.user_lookups,
    ius.user_updates,
    ius.last_user_seek,
    ius.last_user_scan
FROM sys.indexes i
JOIN sys.dm_db_index_usage_stats ius
    ON i.object_id = ius.object_id AND i.index_id = ius.index_id
    AND ius.database_id = DB_ID()
WHERE i.type > 0                        -- exclude heaps
  AND i.is_primary_key = 0
  AND i.is_unique = 0
  AND (ius.user_seeks + ius.user_scans + ius.user_lookups) = 0
  AND ius.user_updates > 0
ORDER BY ius.user_updates DESC;

-- Highly fragmented indexes (>30% = rebuild; 10-30% = reorganize)
SELECT
    DB_NAME()                           AS DatabaseName,
    OBJECT_NAME(ips.object_id)         AS TableName,
    i.name                             AS IndexName,
    ips.index_type_desc,
    ROUND(ips.avg_fragmentation_in_percent, 2) AS FragmentationPct,
    ips.page_count
FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') ips
JOIN sys.indexes i ON ips.object_id = i.object_id AND ips.index_id = i.index_id
WHERE ips.avg_fragmentation_in_percent > 10
  AND ips.page_count > 100
ORDER BY ips.avg_fragmentation_in_percent DESC;


-- ============================================================
-- 7. BLOCKING & DEADLOCKS
-- ============================================================
PRINT '';
PRINT '--- [7] CURRENT BLOCKING ---';

SELECT
    r.session_id,
    r.blocking_session_id,
    r.wait_type,
    r.wait_time / 1000                  AS WaitTime_Sec,
    r.status,
    DB_NAME(r.database_id)             AS DatabaseName,
    SUBSTRING(qt.text, (r.statement_start_offset/2)+1,
        ((CASE r.statement_end_offset WHEN -1 THEN DATALENGTH(qt.text)
          ELSE r.statement_end_offset END - r.statement_start_offset)/2)+1)
                                        AS CurrentStatement,
    s.login_name,
    s.host_name,
    s.program_name
FROM sys.dm_exec_requests r
JOIN sys.dm_exec_sessions s ON r.session_id = s.session_id
CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) qt
WHERE r.blocking_session_id > 0;


-- ============================================================
-- 8. TEMPDB USAGE
-- ============================================================
PRINT '';
PRINT '--- [8] TEMPDB USAGE ---';

-- TempDB file sizes and space used
SELECT
    name,
    physical_name,
    size * 8 / 1024                     AS SizeMB,
    fileproperty(name, 'SpaceUsed') * 8 / 1024 AS UsedMB,
    (size - fileproperty(name, 'SpaceUsed')) * 8 / 1024 AS FreeMB
FROM tempdb.sys.database_files;

-- Sessions using most tempdb space
SELECT TOP 10
    ss.session_id,
    ss.login_name,
    ss.host_name,
    ss.program_name,
    tsu.internal_objects_alloc_page_count * 8 / 1024  AS InternalObjects_MB,
    tsu.user_objects_alloc_page_count * 8 / 1024      AS UserObjects_MB,
    tsu.task_internal_objects_alloc_page_count * 8 / 1024 AS TaskInternal_MB
FROM sys.dm_exec_sessions ss
JOIN (
    SELECT
        session_id,
        SUM(internal_objects_alloc_page_count) AS internal_objects_alloc_page_count,
        SUM(user_objects_alloc_page_count)     AS user_objects_alloc_page_count,
        SUM(task_internal_objects_alloc_page_count) AS task_internal_objects_alloc_page_count
    FROM sys.dm_db_task_space_usage
    GROUP BY session_id
) tsu ON ss.session_id = tsu.session_id
WHERE (tsu.internal_objects_alloc_page_count + tsu.user_objects_alloc_page_count) > 0
ORDER BY (tsu.internal_objects_alloc_page_count + tsu.user_objects_alloc_page_count) DESC;


-- ============================================================
-- 9. TOP SLOW QUERIES (by elapsed time)
-- ============================================================
PRINT '';
PRINT '--- [9] TOP SLOW QUERIES (by total elapsed time) ---';

SELECT TOP 15
    qs.execution_count,
    qs.total_elapsed_time / 1000        AS TotalElapsed_ms,
    qs.total_elapsed_time / qs.execution_count / 1000 AS AvgElapsed_ms,
    qs.total_logical_reads              AS TotalLogicalReads,
    qs.total_logical_reads / qs.execution_count AS AvgLogicalReads,
    qs.total_worker_time / 1000         AS TotalCPU_ms,
    qs.total_physical_reads,
    DB_NAME(qt.dbid)                    AS DatabaseName,
    SUBSTRING(qt.text, (qs.statement_start_offset/2)+1,
        ((CASE qs.statement_end_offset WHEN -1 THEN DATALENGTH(qt.text)
          ELSE qs.statement_end_offset END - qs.statement_start_offset)/2)+1)
                                        AS QueryText,
    qp.query_plan                       AS ExecutionPlan
FROM sys.dm_exec_query_stats qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) qt
CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) qp
ORDER BY qs.total_elapsed_time DESC;


-- ============================================================
-- 10. DATABASE FILE AUTOGROWTH EVENTS (last 7 days)
-- ============================================================
PRINT '';
PRINT '--- [10] AUTOGROWTH EVENTS (last 7 days) ---';
-- Requires default trace to be enabled (usually on by default)

DECLARE @tracefile NVARCHAR(500) =
    (SELECT path FROM sys.traces WHERE is_default = 1);

SELECT
    DatabaseName,
    Filename,
    CASE EventClass
        WHEN 92 THEN 'Data File Autogrow'
        WHEN 93 THEN 'Log File Autogrow'
    END                                 AS EventType,
    Duration / 1000                     AS Duration_ms,
    StartTime,
    EndTime,
    IntegerData * 8 / 1024              AS GrowthMB
FROM sys.fn_trace_gettable(@tracefile, DEFAULT)
WHERE EventClass IN (92, 93)
  AND StartTime >= DATEADD(DAY, -7, GETDATE())
ORDER BY StartTime DESC;


-- ============================================================
-- 11. CONFIGURATION CHECK
-- ============================================================
PRINT '';
PRINT '--- [11] KEY CONFIGURATION SETTINGS ---';

SELECT
    name,
    value_in_use                        AS CurrentValue,
    description
FROM sys.configurations
WHERE name IN (
    'max degree of parallelism',
    'cost threshold for parallelism',
    'max server memory (MB)',
    'min server memory (MB)',
    'optimize for ad hoc workloads',
    'backup compression default',
    'fill factor (%)',
    'remote admin connections',
    'priority boost'
)
ORDER BY name;

-- Check Max Memory vs Physical RAM
SELECT
    (SELECT value_in_use FROM sys.configurations WHERE name = 'max server memory (MB)')
                                        AS MaxServerMemory_MB,
    physical_memory_kb / 1024           AS PhysicalRAM_MB,
    CAST(100.0 *
        (SELECT value_in_use FROM sys.configurations WHERE name = 'max server memory (MB)')
        / (physical_memory_kb / 1024) AS DECIMAL(5,1))
                                        AS MaxMemoryAsPctOfRAM
FROM sys.dm_os_sys_info;


-- ============================================================
-- SUMMARY NOTES
-- ============================================================
PRINT '';
PRINT '============================================================';
PRINT ' INTERPRETATION GUIDE';
PRINT '============================================================';
PRINT ' Page Life Expectancy  : Target >300s (ideally >1000s/4GB RAM)';
PRINT ' Signal Wait %         : >25% suggests CPU pressure';
PRINT ' Read I/O Latency      : >20ms = concern, >50ms = problem';
PRINT ' Write I/O Latency     : >30ms = concern, >50ms = problem';
PRINT ' Index Fragmentation   : 10-30% = reorganize, >30% = rebuild';
PRINT ' Missing Index Score   : Higher = more potential benefit';
PRINT ' TempDB files          : Should match # of logical CPUs (up to 8)';
PRINT ' MAXDOP                : Usually set to # physical cores / 2';
PRINT ' Cost Threshold        : Default 5 is too low; 25-50 recommended';
PRINT '============================================================';
