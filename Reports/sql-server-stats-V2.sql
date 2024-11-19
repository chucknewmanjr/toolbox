IF OBJECT_ID('tempdb..#columns') IS NULL BEGIN; -- drop table #columns
	SELECT TOP 0 DB_ID() AS database_id, * INTO #columns FROM sys.columns WITH (NOLOCK);

	INSERT #columns EXEC sys.sp_MSforeachdb 'USE [?]; SELECT DB_ID() AS database_id, * FROM sys.columns WITH (NOLOCK)';

	DELETE #columns WHERE DB_NAME(database_id) LIKE '%Snapshot%' OR DB_NAME(database_id) IN ('CPDB_shadow', 'msdb', 'DBA') OR OBJECT_SCHEMA_NAME(object_id) = 'sys';
END;

IF OBJECT_ID('tempdb..#dm_db_partition_stats') IS NULL BEGIN; -- drop table #dm_db_partition_stats
	SELECT TOP 0 DB_ID() AS database_id, * INTO #dm_db_partition_stats FROM sys.dm_db_partition_stats WITH (NOLOCK);

	INSERT #dm_db_partition_stats EXEC sys.sp_MSforeachdb 'USE [?]; SELECT DB_ID() AS database_id, * FROM sys.dm_db_partition_stats WITH (NOLOCK);';

	DELETE #dm_db_partition_stats WHERE DB_NAME(database_id) LIKE '%Snapshot%' OR DB_NAME(database_id) IN ('CPDB_shadow', 'msdb', 'DBA') OR OBJECT_SCHEMA_NAME(object_id) = 'sys';
END;

IF OBJECT_ID('tempdb..#objects') IS NULL BEGIN; -- drop table #objects
	SELECT TOP 0 DB_ID() AS database_id, * INTO #objects FROM sys.objects WITH (NOLOCK);

	INSERT #objects EXEC sys.sp_MSforeachdb 'USE [?]; SELECT DB_ID() AS database_id, * FROM sys.objects WITH (NOLOCK);';

	DELETE #objects WHERE DB_NAME(database_id) LIKE '%Snapshot%' OR DB_NAME(database_id) IN ('CPDB_shadow', 'msdb', 'DBA') OR OBJECT_SCHEMA_NAME(object_id) = 'sys';
END;

IF OBJECT_ID('tempdb..#sql_modules') IS NULL BEGIN; -- drop table #sql_modules
	SELECT TOP 0 DB_ID() AS database_id, object_id, LEN(definition) as len_definition INTO #sql_modules FROM sys.sql_modules WITH (NOLOCK);

	INSERT #sql_modules EXEC sys.sp_MSforeachdb 'USE [?]; SELECT DB_ID() AS database_id, object_id, LEN(definition) FROM sys.sql_modules WITH (NOLOCK);';

	DELETE #sql_modules WHERE DB_NAME(database_id) LIKE '%Snapshot%' OR DB_NAME(database_id) IN ('CPDB_shadow', 'msdb', 'DBA') OR OBJECT_SCHEMA_NAME(object_id) = 'sys';
END;

IF OBJECT_ID('tempdb..#foreign_keys') IS NULL BEGIN; -- drop table #foreign_keys
	SELECT TOP 0 DB_ID() AS database_id, * INTO #foreign_keys FROM sys.foreign_keys WITH (NOLOCK);

	INSERT #foreign_keys EXEC sys.sp_MSforeachdb 'USE [?]; SELECT DB_ID() AS database_id, * FROM sys.foreign_keys WITH (NOLOCK);';

	DELETE #foreign_keys WHERE DB_NAME(database_id) LIKE '%Snapshot%' OR DB_NAME(database_id) IN ('CPDB_shadow', 'msdb', 'DBA') OR OBJECT_SCHEMA_NAME(object_id) = 'sys';
END;
GO

DECLARE @PagesPerTB REAL = CAST(POWER(1024, 3) AS REAL) / 8

DECLARE @Stats TABLE (StatID INT IDENTITY, StatName sysname, StatItem sysname, StatValue real);

INSERT @Stats
SELECT TOP 1
	'Total terabytes',
	'',
	ROUND(SUM(used_page_count) / @PagesPerTB, 1)
FROM #dm_db_partition_stats;

INSERT @Stats
SELECT TOP 1
	'Total tables',
	'',
	COUNT(*)
FROM #objects
WHERE type_desc = 'USER_TABLE'

INSERT @Stats
SELECT
	'Number of tables with over 2 billion rows',
	'',
	COUNT(*)
FROM #dm_db_partition_stats
WHERE index_id < 2
	AND row_count > 2000000000;

INSERT @Stats
SELECT TOP 1
	'Total stored procedures',
	'',
	COUNT(*)
FROM #objects
WHERE type_desc = 'SQL_STORED_PROCEDURE';

INSERT @Stats
SELECT TOP 1
	'Total thousands of lines of SQL code',
	'',
	ROUND(SUM(len_definition) / 38000.0, 0) -- roughly 38 characters per line
FROM #sql_modules;

INSERT @Stats
SELECT TOP 1
	'Database with the most lines of code',
	DB_NAME(database_id),
	ROUND(SUM(len_definition) / 38000.0, 0) -- roughly 38 characters per line
FROM #sql_modules
GROUP BY database_id
ORDER BY SUM(len_definition) DESC;

INSERT @Stats
SELECT TOP 1
	'Biggest database in terabytes',
	DB_NAME(database_id),
	ROUND(SUM(used_page_count) / @PagesPerTB, 0)
FROM #dm_db_partition_stats
GROUP BY database_id
ORDER BY SUM(used_page_count) DESC;

INSERT @Stats
SELECT TOP 1
	'Database with the most tables',
	DB_NAME(database_id),
	COUNT(*)
FROM #objects
WHERE type_desc = 'USER_TABLE'
GROUP BY database_id
ORDER BY COUNT(*) DESC;

INSERT @Stats
SELECT TOP 1
	'Tables in CPDB',
	DB_NAME(database_id),
	COUNT(*)
FROM #objects
WHERE type_desc = 'USER_TABLE'
	AND DB_NAME(database_id) = 'CPDB'
GROUP BY database_id
ORDER BY COUNT(*) DESC;

INSERT @Stats
SELECT TOP 1
	'Database with the most stored procedures',
	DB_NAME(database_id),
	COUNT(*)
FROM #objects
WHERE type_desc = 'SQL_STORED_PROCEDURE'
GROUP BY database_id
ORDER BY COUNT(*) DESC;

INSERT @Stats
SELECT TOP 1
	'Biggest table in terabytes',
	DB_NAME(database_id) + '.' + OBJECT_SCHEMA_NAME(object_id, database_id) + '.' + OBJECT_NAME(object_id, database_id),
	ROUND(SUM(used_page_count) / @PagesPerTB, 1)
FROM #dm_db_partition_stats
GROUP BY database_id, object_id
ORDER BY SUM(used_page_count) DESC;

INSERT @Stats
SELECT TOP 1
	'Biggest table by row data in terabytes',
	DB_NAME(database_id) + '.' + OBJECT_SCHEMA_NAME(object_id, database_id) + '.' + OBJECT_NAME(object_id, database_id),
	ROUND(SUM(in_row_data_page_count) / @PagesPerTB, 1)
FROM #dm_db_partition_stats
WHERE index_id < 2
GROUP BY database_id, object_id
ORDER BY SUM(in_row_data_page_count) DESC;

INSERT @Stats
SELECT TOP 1
	'Table with the most rows in billions',
	DB_NAME(database_id) + '.' + OBJECT_SCHEMA_NAME(object_id, database_id) + '.' + OBJECT_NAME(object_id, database_id),
	ROUND(SUM(row_count) / 1e9, 1)
FROM #dm_db_partition_stats
WHERE index_id < 2
GROUP BY database_id, object_id -- group because of partitioning
ORDER BY SUM(row_count) DESC;

INSERT @Stats
SELECT TOP 1
	'Table with the most columns',
	DB_NAME(database_id) + '.' + OBJECT_SCHEMA_NAME(object_id, database_id) + '.' + OBJECT_NAME(object_id, database_id),
	COUNT(*)
FROM #columns
GROUP BY database_id, object_id 
ORDER BY COUNT(*) DESC;

INSERT @Stats
SELECT TOP 1
	'Widest table by average megabytes per row',
	DB_NAME(database_id) + '.' + OBJECT_SCHEMA_NAME(object_id, database_id) + '.' + OBJECT_NAME(object_id, database_id),
	ROUND(SUM(used_page_count) * 8.0 / 1024 / MAX(row_count), 1)
FROM #dm_db_partition_stats
GROUP BY database_id, object_id
HAVING MAX(row_count) > 100
ORDER BY SUM(used_page_count) * 8.0 / 1024 / MAX(row_count) DESC;

INSERT @Stats
SELECT TOP 1
	'Table with the most foreign key constraints',
	DB_NAME(database_id) + '.' + OBJECT_SCHEMA_NAME(parent_object_id, database_id) + '.' + OBJECT_NAME(parent_object_id, database_id),
	COUNT(*)
FROM #objects
WHERE type_desc = 'FOREIGN_KEY_CONSTRAINT'
GROUP BY database_id, parent_object_id
ORDER BY COUNT(*) DESC;

INSERT @Stats
SELECT TOP 2
	'Table with the most referencing tables',
	DB_NAME(database_id) + '.' + OBJECT_SCHEMA_NAME(referenced_object_id, database_id) + '.' + OBJECT_NAME(referenced_object_id, database_id),
	COUNT(*)
FROM #foreign_keys
GROUP BY database_id, referenced_object_id
ORDER BY COUNT(*) DESC;

INSERT @Stats
SELECT TOP 1
	'Longest stored procedure by thousands of lines',
	DB_NAME(database_id) + '.' + OBJECT_SCHEMA_NAME(object_id, database_id) + '.' + OBJECT_NAME(object_id, database_id),
	ROUND(len_definition / 38000.0, 0) -- roughly 38 characters per line
FROM #sql_modules
ORDER BY len_definition DESC;

SELECT * FROM @Stats





