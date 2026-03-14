/*
==============================================================================
  Script:        replication-health-check.sql
  Author:        Misty Collins
  Compatible with: SQL Server 2014 (and later)
  Covers: Transactional, Merge, and Snapshot Replication
  Run as: sysadmin or replmonitor role on the distribution database
  Run on: The DISTRIBUTOR instance (or publisher if self-distributing)
==============================================================================
*/

PRINT '============================================================';
PRINT ' SQL SERVER REPLICATION HEALTH CHECK';
PRINT ' Run Date: ' + CONVERT(VARCHAR, GETDATE(), 120);
PRINT ' Server:   ' + @@SERVERNAME;
PRINT '============================================================';


-- ============================================================
-- 1. REPLICATION TOPOLOGY OVERVIEW
-- ============================================================
PRINT '';
PRINT '--- [1] REPLICATION TOPOLOGY OVERVIEW ---';

-- Distributors configured on this server
SELECT
    name                                AS DistributionDatabase,
    create_date,
    log_reuse_wait_desc                 AS LogReuseWait
FROM sys.databases
WHERE is_distributor = 1;

-- All Publications
USE distribution;

SELECT
    srv.srvname                         AS Publisher,
    pub.publisher_db                    AS PublisherDatabase,
    pub.publication                     AS PublicationName,
    CASE pub.publication_type
        WHEN 0 THEN 'Transactional'
        WHEN 1 THEN 'Snapshot'
        WHEN 2 THEN 'Merge'
    END                                 AS PublicationType,
    pub.status                          AS PublicationStatus,
    pub.allow_push,
    pub.allow_pull,
    pub.immediate_sync                  AS SnapshotAlwaysAvailable,
    pub.retention                       AS RetentionPeriodHours
FROM MSpublications pub
JOIN MSdistpublishers dp ON pub.publisher_id = dp.id
JOIN master.sys.servers srv ON dp.name = srv.srvname
ORDER BY srv.srvname, pub.publisher_db, pub.publication;

-- All Subscriptions
SELECT
    srv.srvname                         AS Publisher,
    pub.publisher_db                    AS PublisherDatabase,
    pub.publication                     AS PublicationName,
    sub.subscriber_db                   AS SubscriberDatabase,
    CASE pub.publication_type
        WHEN 0 THEN 'Transactional'
        WHEN 1 THEN 'Snapshot'
        WHEN 2 THEN 'Merge'
    END                                 AS PublicationType,
    sub.subscriber_server               AS SubscriberServer,
    CASE sub.subscription_type
        WHEN 0 THEN 'Push'
        WHEN 1 THEN 'Pull'
        WHEN 2 THEN 'Anonymous'
    END                                 AS SubscriptionType,
    sub.status                          AS SubscriptionStatus
FROM MSsubscriptions sub
JOIN MSpublications pub ON sub.publication_id = pub.publication_id
JOIN MSdistpublishers dp ON pub.publisher_id = dp.id
JOIN master.sys.servers srv ON dp.name = srv.srvname
WHERE sub.subscriber_id >= 0
ORDER BY srv.srvname, pub.publication, sub.subscriber_server;


-- ============================================================
-- 2. DISTRIBUTION AGENT STATUS (Transactional / Snapshot)
-- ============================================================
PRINT '';
PRINT '--- [2] DISTRIBUTION AGENT STATUS ---';

SELECT
    da.name                             AS AgentName,
    pub.publication                     AS Publication,
    da.subscriber_db                    AS SubscriberDB,
    da.subscriber_security_mode,
    dah.runstatus,
    CASE dah.runstatus
        WHEN 1 THEN 'Started'
        WHEN 2 THEN 'Succeeded'
        WHEN 3 THEN 'In Progress'
        WHEN 4 THEN 'Idle'
        WHEN 5 THEN 'Retrying'
        WHEN 6 THEN '*** FAILED ***'
    END                                 AS StatusDescription,
    dah.comments                        AS LastMessage,
    dah.start_time                      AS LastRunStart,
    dah.time                            AS LastRunTime,
    dah.duration                        AS DurationSeconds,
    dah.delivered_commands,
    dah.delivered_transactions,
    dah.delivery_rate                   AS CmdsPerSec
FROM MSdistribution_agents da
JOIN MSpublications pub ON da.publication_id = pub.publication_id
OUTER APPLY (
    SELECT TOP 1 *
    FROM MSdistribution_history
    WHERE agent_id = da.id
    ORDER BY time DESC
) dah
ORDER BY
    CASE dah.runstatus WHEN 6 THEN 0 WHEN 5 THEN 1 ELSE 2 END,
    da.name;


-- ============================================================
-- 3. LOG READER AGENT STATUS
-- ============================================================
PRINT '';
PRINT '--- [3] LOG READER AGENT STATUS ---';

SELECT
    la.name                             AS AgentName,
    la.publisher_db                     AS PublisherDB,
    CASE lah.runstatus
        WHEN 1 THEN 'Started'
        WHEN 2 THEN 'Succeeded'
        WHEN 3 THEN 'In Progress'
        WHEN 4 THEN 'Idle'
        WHEN 5 THEN 'Retrying'
        WHEN 6 THEN '*** FAILED ***'
    END                                 AS StatusDescription,
    lah.comments                        AS LastMessage,
    lah.start_time                      AS LastRunStart,
    lah.time                            AS LastRunTime,
    lah.delivered_commands              AS CommandsRead,
    lah.delivered_transactions          AS TransactionsRead,
    lah.delivery_rate                   AS CmdsPerSec
FROM MSlogreader_agents la
OUTER APPLY (
    SELECT TOP 1 *
    FROM MSlogreader_history
    WHERE agent_id = la.id
    ORDER BY time DESC
) lah
ORDER BY
    CASE lah.runstatus WHEN 6 THEN 0 WHEN 5 THEN 1 ELSE 2 END,
    la.name;


-- ============================================================
-- 4. SNAPSHOT AGENT STATUS
-- ============================================================
PRINT '';
PRINT '--- [4] SNAPSHOT AGENT STATUS ---';

SELECT
    sa.name                             AS AgentName,
    sa.publisher_db                     AS PublisherDB,
    pub.publication                     AS Publication,
    CASE sah.runstatus
        WHEN 1 THEN 'Started'
        WHEN 2 THEN 'Succeeded'
        WHEN 3 THEN 'In Progress'
        WHEN 4 THEN 'Idle'
        WHEN 5 THEN 'Retrying'
        WHEN 6 THEN '*** FAILED ***'
    END                                 AS StatusDescription,
    sah.comments                        AS LastMessage,
    sah.start_time                      AS LastRunStart,
    sah.time                            AS LastRunTime,
    sah.duration                        AS DurationSeconds
FROM MSsnapshot_agents sa
JOIN MSpublications pub ON sa.publication_id = pub.publication_id
OUTER APPLY (
    SELECT TOP 1 *
    FROM MSsnapshot_history
    WHERE agent_id = sa.id
    ORDER BY time DESC
) sah
ORDER BY
    CASE sah.runstatus WHEN 6 THEN 0 WHEN 5 THEN 1 ELSE 2 END,
    sa.name;


-- ============================================================
-- 5. REPLICATION LATENCY (Tracer Tokens)
-- ============================================================
PRINT '';
PRINT '--- [5] RECENT TRACER TOKEN LATENCY ---';
-- Shows latency measured from last tracer tokens posted
-- (Tracer tokens must have been posted previously via SSMS or sp_posttracertoken)

SELECT TOP 20
    pub.publication                     AS Publication,
    tt.publisher_commit                 AS TokenPostedAt,
    DATEDIFF(SECOND, tt.publisher_commit, tth.distributor_commit)
                                        AS PublisherToDistributor_Sec,
    DATEDIFF(SECOND, tth.distributor_commit, tth.subscriber_commit)
                                        AS DistributorToSubscriber_Sec,
    DATEDIFF(SECOND, tt.publisher_commit, tth.subscriber_commit)
                                        AS EndToEndLatency_Sec,
    tth.subscriber_server               AS Subscriber
FROM MStracer_tokens tt
JOIN MSpublications pub ON tt.publication_id = pub.publication_id
JOIN MStracer_history tth ON tt.tracer_id = tth.parent_tracer_id
ORDER BY tt.publisher_commit DESC;


-- ============================================================
-- 6. UNDISTRIBUTED COMMANDS (Replication Lag)
-- ============================================================
PRINT '';
PRINT '--- [6] UNDISTRIBUTED COMMANDS (Pending Delivery) ---';
-- High values here = subscriber is falling behind

SELECT
    da.name                             AS AgentName,
    pub.publication                     AS Publication,
    da.subscriber_db                    AS SubscriberDB,
    -- sp_replcounters gives pending command counts per subscription
    rc.pubname,
    rc.replbeginlsn,
    rc.replnextlsn,
    rc.undelivcmds                      AS PendingCommands,
    rc.undeliv_time_ms / 1000          AS PendingDeliveryTime_Sec
FROM MSdistribution_agents da
JOIN MSpublications pub ON da.publication_id = pub.publication_id
CROSS APPLY (
    SELECT TOP 1
        pubname,
        replbeginlsn,
        replnextlsn,
        undelivcmds,
        undeliv_time_ms
    FROM MSdistribution_status ds
    WHERE ds.agent_id = da.id
) rc
WHERE rc.undelivcmds > 0
ORDER BY rc.undelivcmds DESC;


-- ============================================================
-- 7. MERGE AGENT STATUS (if Merge replication in use)
-- ============================================================
PRINT '';
PRINT '--- [7] MERGE AGENT STATUS ---';

SELECT
    ma.name                             AS AgentName,
    ma.publisher_db                     AS PublisherDB,
    ma.publication                      AS Publication,
    ma.subscriber_db                    AS SubscriberDB,
    CASE mah.runstatus
        WHEN 1 THEN 'Started'
        WHEN 2 THEN 'Succeeded'
        WHEN 3 THEN 'In Progress'
        WHEN 4 THEN 'Idle'
        WHEN 5 THEN 'Retrying'
        WHEN 6 THEN '*** FAILED ***'
    END                                 AS StatusDescription,
    mah.comments                        AS LastMessage,
    mah.start_time                      AS LastRunStart,
    mah.time                            AS LastRunTime,
    mah.upload_inserts,
    mah.upload_updates,
    mah.upload_deletes,
    mah.download_inserts,
    mah.download_updates,
    mah.download_deletes,
    mah.conflicts                       AS ConflictsDetected
FROM MSmerge_agents ma
OUTER APPLY (
    SELECT TOP 1 *
    FROM MSmerge_history
    WHERE agent_id = ma.id
    ORDER BY time DESC
) mah
ORDER BY
    CASE mah.runstatus WHEN 6 THEN 0 WHEN 5 THEN 1 ELSE 2 END,
    ma.name;


-- ============================================================
-- 8. REPLICATION ERRORS (last 24 hours)
-- ============================================================
PRINT '';
PRINT '--- [8] REPLICATION ERRORS (last 24 hours) ---';

-- Distribution Agent errors
SELECT
    'Distribution' AS AgentType,
    da.name         AS AgentName,
    pub.publication AS Publication,
    dah.time        AS ErrorTime,
    dah.runstatus,
    dah.comments    AS ErrorMessage
FROM MSdistribution_history dah
JOIN MSdistribution_agents da ON dah.agent_id = da.id
JOIN MSpublications pub ON da.publication_id = pub.publication_id
WHERE dah.runstatus = 6
  AND dah.time >= DATEADD(HOUR, -24, GETDATE())

UNION ALL

-- Log Reader errors
SELECT
    'LogReader',
    la.name,
    la.publisher_db,
    lah.time,
    lah.runstatus,
    lah.comments
FROM MSlogreader_history lah
JOIN MSlogreader_agents la ON lah.agent_id = la.id
WHERE lah.runstatus = 6
  AND lah.time >= DATEADD(HOUR, -24, GETDATE())

UNION ALL

-- Snapshot Agent errors
SELECT
    'Snapshot',
    sa.name,
    pub.publication,
    sah.time,
    sah.runstatus,
    sah.comments
FROM MSsnapshot_history sah
JOIN MSsnapshot_agents sa ON sah.agent_id = sa.id
JOIN MSpublications pub ON sa.publication_id = pub.publication_id
WHERE sah.runstatus = 6
  AND sah.time >= DATEADD(HOUR, -24, GETDATE())

UNION ALL

-- Merge Agent errors
SELECT
    'Merge',
    ma.name,
    ma.publication,
    mah.time,
    mah.runstatus,
    mah.comments
FROM MSmerge_history mah
JOIN MSmerge_agents ma ON mah.agent_id = ma.id
WHERE mah.runstatus = 6
  AND mah.time >= DATEADD(HOUR, -24, GETDATE())

ORDER BY ErrorTime DESC;


-- ============================================================
-- 9. DISTRIBUTION DATABASE HEALTH
-- ============================================================
PRINT '';
PRINT '--- [9] DISTRIBUTION DATABASE HEALTH ---';

-- Distribution DB size & log usage
SELECT
    name                                AS FileName,
    physical_name,
    type_desc,
    size * 8 / 1024                     AS SizeMB,
    FILEPROPERTY(name, 'SpaceUsed') * 8 / 1024 AS UsedMB,
    (size - FILEPROPERTY(name, 'SpaceUsed')) * 8 / 1024 AS FreeMB
FROM distribution.sys.database_files;

-- Oldest undistributed transaction (key latency indicator)
SELECT
    da.name                             AS AgentName,
    pub.publication                     AS Publication,
    MIN(dt.entry_time)                  AS OldestUndistributedTxn,
    DATEDIFF(MINUTE, MIN(dt.entry_time), GETDATE())
                                        AS AgeMinutes,
    COUNT(*)                            AS UndistributedTxnCount
FROM MSdistribution_agents da
JOIN MSpublications pub ON da.publication_id = pub.publication_id
JOIN MSrepl_transactions dt ON da.publisher_database_id = dt.publisher_database_id
LEFT JOIN MSdistribution_history dah ON dah.agent_id = da.id
WHERE dt.entry_time < GETDATE()
GROUP BY da.name, pub.publication
HAVING MIN(dt.entry_time) < DATEADD(MINUTE, -5, GETDATE())
ORDER BY AgeMinutes DESC;

-- Distribution DB cleanup (retention window)
SELECT
    s.name                              AS ParameterName,
    s.value                             AS Value
FROM distribution.dbo.MSdistribution_agents da
CROSS JOIN (
    SELECT 'max_distretention' AS name, max_distretention AS value
    FROM MSdistpublishers
    UNION ALL
    SELECT 'min_distretention', min_distretention
    FROM MSdistpublishers
) s
WHERE da.id = (SELECT TOP 1 id FROM MSdistribution_agents)
GROUP BY s.name, s.value;


-- ============================================================
-- 10. REPLICATION AGENT JOBS STATUS
-- ============================================================
PRINT '';
PRINT '--- [10] REPLICATION SQL AGENT JOBS ---';

SELECT
    j.name                              AS JobName,
    CASE j.enabled WHEN 1 THEN 'Enabled' ELSE 'DISABLED' END AS JobEnabled,
    CASE jh.run_status
        WHEN 0 THEN '*** FAILED ***'
        WHEN 1 THEN 'Succeeded'
        WHEN 2 THEN 'Retry'
        WHEN 3 THEN 'Cancelled'
        WHEN 4 THEN 'In Progress'
    END                                 AS LastRunStatus,
    msdb.dbo.agent_datetime(jh.run_date, jh.run_time)
                                        AS LastRunDateTime,
    jh.message                          AS LastRunMessage,
    js.next_run_date,
    js.next_run_time
FROM msdb.dbo.sysjobs j
LEFT JOIN msdb.dbo.sysjobhistory jh
    ON j.job_id = jh.job_id
    AND jh.instance_id = (
        SELECT MAX(instance_id)
        FROM msdb.dbo.sysjobhistory
        WHERE job_id = j.job_id AND step_id = 0
    )
LEFT JOIN msdb.dbo.sysjobschedules js ON j.job_id = js.job_id
WHERE j.category_id IN (
    SELECT category_id FROM msdb.dbo.syscategories
    WHERE name IN (
        'REPL-Distribution',
        'REPL-LogReader',
        'REPL-Merge',
        'REPL-Snapshot',
        'REPL-QueueReader',
        'REPL-Checkup',
        'REPL-Alert Response',
        'Replication'
    )
)
ORDER BY
    CASE jh.run_status WHEN 0 THEN 0 ELSE 1 END,
    j.name;


-- ============================================================
-- 11. ARTICLES & SCHEMA VALIDATION
-- ============================================================
PRINT '';
PRINT '--- [11] PUBLISHED ARTICLES ---';

SELECT
    pub.publication                     AS Publication,
    a.article                           AS ArticleName,
    a.source_owner                      AS SchemaName,
    a.source_object                     AS TableName,
    CASE a.type
        WHEN 1  THEN 'Log-based'
        WHEN 3  THEN 'Log-based w/ manual filter'
        WHEN 5  THEN 'Log-based w/ manual view'
        WHEN 7  THEN 'Log-based w/ manual filter & view'
        WHEN 8  THEN 'Stored Procedure'
        WHEN 11 THEN 'Indexed View'
        ELSE CAST(a.type AS VARCHAR)
    END                                 AS ArticleType,
    a.ins_cmd,
    a.upd_cmd,
    a.del_cmd
FROM MSarticles a
JOIN MSpublications pub ON a.publication_id = pub.publication_id
ORDER BY pub.publication, a.article;


-- ============================================================
-- SUMMARY NOTES
-- ============================================================
PRINT '';
PRINT '============================================================';
PRINT ' INTERPRETATION GUIDE';
PRINT '============================================================';
PRINT ' Agent Status 6        : FAILED — investigate immediately';
PRINT ' Agent Status 5        : Retrying — monitor closely';
PRINT ' Pending Commands      : Should trend toward 0; spikes = lag';
PRINT ' End-to-End Latency    : Acceptable varies by SLA; >60s = concern';
PRINT ' Oldest Undistributed  : >5-10min = subscriber falling behind';
PRINT ' Distribution DB Log   : High log reuse wait = cleanup issue';
PRINT ' Disabled Agent Jobs   : Should only be disabled intentionally';
PRINT ' Merge Conflicts       : Any conflicts should be reviewed';
PRINT '------------------------------------------------------------';
PRINT ' TIP: Run sp_posttracertoken @publication = ''YourPub''';
PRINT '      then re-check Section 5 for live latency measurement.';
PRINT '============================================================';
