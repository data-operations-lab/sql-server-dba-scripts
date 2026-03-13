/*
Script:        basic-server-health-check.sql
Author:        Misty Collins
Purpose:       Identify common SQL Server operational risks
Notes:         Read-only diagnostic queries
*/

--------------------------------------------------
-- SQL Server Version
--------------------------------------------------

SELECT
    @@SERVERNAME AS ServerName,
    SERVERPROPERTY('Edition') AS Edition,
    SERVERPROPERTY('ProductVersion') AS Version,
    SERVERPROPERTY('ProductLevel') AS PatchLevel;


--------------------------------------------------
-- Databases not backed up recently
--------------------------------------------------

SELECT
    d.name AS database_name,
    MAX(b.backup_finish_date) AS last_backup
FROM sys.databases d
LEFT JOIN msdb.dbo.backupset b
    ON d.name = b.database_name
    AND b.type = 'D'
WHERE d.database_id > 4
GROUP BY d.name
ORDER BY last_backup;


--------------------------------------------------
-- SQL Agent jobs that failed recently
--------------------------------------------------

SELECT
    j.name AS job_name,
    h.run_date,
    h.run_time,
    h.run_status
FROM msdb.dbo.sysjobhistory h
JOIN msdb.dbo.sysjobs j
    ON h.job_id = j.job_id
WHERE h.run_status = 0
AND h.step_id = 0
ORDER BY h.run_date DESC;


--------------------------------------------------
-- Databases with AUTO_SHRINK enabled
--------------------------------------------------

SELECT
    name,
    is_auto_shrink_on
FROM sys.databases
WHERE is_auto_shrink_on = 1;


--------------------------------------------------
-- Databases with AUTO_CLOSE enabled
--------------------------------------------------

SELECT
    name,
    is_auto_close_on
FROM sys.databases
WHERE is_auto_close_on = 1;
