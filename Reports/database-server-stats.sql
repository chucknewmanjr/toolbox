IF OBJECT_ID('tempdb.dbo.#dm_db_partition_stats') IS NULL BEGIN; -- DROP TABLE #dm_db_partition_stats;
	SELECT TOP 0 DB_ID() AS database_id, * INTO #dm_db_partition_stats FROM sys.dm_db_partition_stats;

	INSERT #dm_db_partition_stats EXEC sys.sp_MSforeachdb 'use [?]; SELECT DB_ID() AS dbname, * FROM sys.dm_db_partition_stats;';
END;

-- SELECT top 100 * FROM #dm_db_partition_stats

SELECT COUNT(DISTINCT database_id) AS database_count
FROM #dm_db_partition_stats
WHERE DB_NAME(database_id) NOT LIKE '%snapshot%'
	AND DB_NAME(database_id) NOT IN ('master', 'msdb', 'model', 'tempdb', 'DUMPCONFIGURATION');

SELECT TOP 5 'most rows' AS [label],
	DB_NAME(database_id) AS dbanem,
	OBJECT_SCHEMA_NAME([object_id], database_id) + '.' + OBJECT_NAME([object_id], database_id) AS tablename, 
	MAX(row_count) / 1000000000.0 AS billions_of_rows
FROM #dm_db_partition_stats
WHERE DB_NAME(database_id) NOT LIKE '%snapshot%'
GROUP BY database_id, [object_id]
ORDER BY MAX(row_count) DESC;

SELECT TOP 10 'largest table' AS [label],
	DB_NAME(database_id) AS dbanem,
	OBJECT_SCHEMA_NAME([object_id], database_id) + '.' + OBJECT_NAME([object_id], database_id) AS tablename,
	SUM(used_page_count) * 8.0 / 1024 / 1024 / 1024 AS total_tb
FROM #dm_db_partition_stats
WHERE DB_NAME(database_id) NOT LIKE '%snapshot%'
GROUP BY database_id, [object_id]
ORDER BY SUM(used_page_count) DESC;

IF OBJECT_ID('tempdb.dbo.#sql_modules') IS NULL BEGIN; -- DROP TABLE #sql_modules;
	SELECT TOP 0 DB_ID() AS database_id, object_id, LEN([definition]) AS characters INTO #sql_modules FROM sys.sql_modules;

	INSERT #sql_modules EXEC sys.sp_MSforeachdb 'use [?]; SELECT DB_ID() AS database_id, object_id, LEN([definition]) AS characters FROM sys.sql_modules;';
END;

SELECT SUM(characters) / 47000.0 AS thousands_of_lines_total
FROM #sql_modules
WHERE DB_NAME(database_id) NOT LIKE '%snapshot%'
	AND DB_NAME(database_id) NOT IN ('master', 'msdb', 'model', 'tempdb', 'DUMPCONFIGURATION')
	AND OBJECT_SCHEMA_NAME([object_id], database_id) NOT IN ('sys');

SELECT TOP 5 'longest proc' AS [label],
	DB_NAME(database_id) AS dbanem,
	OBJECT_SCHEMA_NAME([object_id], database_id) + '.' + OBJECT_NAME([object_id], database_id) AS tablename, 
	characters / 47 AS lines
FROM #sql_modules
WHERE DB_NAME(database_id) NOT LIKE '%snapshot%'
	AND DB_NAME(database_id) NOT IN ('master', 'msdb', 'model', 'tempdb', 'DUMPCONFIGURATION')
	AND OBJECT_SCHEMA_NAME([object_id], database_id) NOT IN ('sys')
ORDER BY characters DESC;

IF OBJECT_ID('tempdb.dbo.#objects') IS NULL BEGIN; -- DROP TABLE #objects;
	SELECT TOP 0 DB_ID() AS database_id, * INTO #objects FROM sys.objects;

	INSERT #objects EXEC sys.sp_MSforeachdb 'use [?]; SELECT DB_ID() AS database_id, * FROM sys.objects;';
END;

SELECT TOP 10 [type_desc], COUNT(*) AS Occurs
FROM #objects
WHERE DB_NAME(database_id) NOT LIKE '%snapshot%'
	AND DB_NAME(database_id) NOT IN ('master', 'msdb', 'model', 'tempdb', 'DUMPCONFIGURATION')
	AND OBJECT_SCHEMA_NAME([object_id], database_id) NOT IN ('sys')
GROUP BY [type_desc]
ORDER BY COUNT(*) DESC;

IF OBJECT_ID('tempdb.dbo.#columns') IS NULL BEGIN; -- DROP TABLE #columns;
	SELECT TOP 0 DB_ID() AS database_id, * INTO #columns FROM sys.columns;

	INSERT #columns EXEC sys.sp_MSforeachdb 'use [?]; SELECT DB_ID() AS database_id, * FROM sys.columns;';
END;

SELECT COUNT(*) AS total_columns
FROM #columns
WHERE DB_NAME(database_id) NOT LIKE '%snapshot%'
	AND DB_NAME(database_id) NOT IN ('master', 'msdb', 'model', 'tempdb', 'DUMPCONFIGURATION')
	AND OBJECT_SCHEMA_NAME([object_id], database_id) NOT IN ('sys');

SELECT TOP 5
	DB_NAME(database_id) AS dbaname,
	OBJECT_SCHEMA_NAME([object_id], database_id) + '.' + OBJECT_NAME([object_id], database_id) AS tablename, 
	COUNT(*) AS [columns]
FROM #columns
WHERE DB_NAME(database_id) NOT LIKE '%snapshot%'
	AND DB_NAME(database_id) NOT IN ('master', 'msdb', 'model', 'tempdb', 'DUMPCONFIGURATION')
	AND OBJECT_SCHEMA_NAME([object_id], database_id) NOT IN ('sys')
GROUP BY database_id, [object_id]
ORDER BY COUNT(*) DESC;


