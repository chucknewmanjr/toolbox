IF OBJECT_ID('tempdb..#dm_db_partition_stats') IS NULL BEGIN; -- drop table #dm_db_partition_stats
	SELECT TOP 0 DB_ID() AS database_id, * INTO #dm_db_partition_stats FROM sys.dm_db_partition_stats WITH (NOLOCK);

	INSERT #dm_db_partition_stats EXEC sys.sp_MSforeachdb 'use [?]; SELECT DB_ID() AS database_id, * FROM sys.dm_db_partition_stats WITH (NOLOCK);';
END;
GO

DECLARE @total_reserved_gb INT = (
	SELECT SUM(reserved_page_count) * 8 / 1024 / 1024 AS reserved_gb
	FROM #dm_db_partition_stats
	WHERE DB_NAME(database_id) NOT LIKE '%snapshot%'
);

SELECT TOP 10
	DB_NAME(database_id) AS DBName,
	SUM(reserved_page_count) * 8 / 1024 / 1024 AS reserved_gb,
	FORMAT(SUM(reserved_page_count) * 8 / 1024 / 1024 * 100.0 / @total_reserved_gb, 'N0') AS pct_of_server
FROM #dm_db_partition_stats
WHERE DB_NAME(database_id) NOT LIKE '%snapshot%'
GROUP BY database_id
ORDER BY reserved_gb DESC;

SELECT TOP 50
	DB_NAME(database_id) AS DBName,
	OBJECT_SCHEMA_NAME(object_id, database_id) + '.' + OBJECT_NAME(object_id, database_id) AS TableName,
	SUM(reserved_page_count) * 8 / 1024 / 1024 AS reserved_gb,
	FORMAT(SUM(reserved_page_count) * 8 / 1024 / 1024 * 100.0 / @total_reserved_gb, 'N1') AS pct_of_server
FROM #dm_db_partition_stats
WHERE OBJECT_SCHEMA_NAME(object_id, database_id) NOT IN ('sys')
	AND DB_NAME(database_id) NOT LIKE '%snapshot%'
GROUP BY database_id, OBJECT_ID
ORDER BY reserved_gb DESC;

