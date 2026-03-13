/*
Script:        log-transaction-health-check.sql
Author:        Misty Collins
Purpose:       Investigate open transactions and transaction log usage
SQL Versions:  SQL Server 2012+
Notes:         Read-only diagnostic checks
*/

--------------------------------------------------
-- Open transactions in the current database
--------------------------------------------------

DBCC OPENTRAN;
GO


--------------------------------------------------
-- View transaction log contents for a database
-- Replace database name as needed
--------------------------------------------------

DBCC LOG('YourDatabaseName');
GO


--------------------------------------------------
-- Transaction log space usage for all databases
--------------------------------------------------

DBCC SQLPERF(LOGSPACE);
GO
