IF OBJECT_ID('tempdb..#columns') IS NULL BEGIN;
	SELECT TOP 0 DB_ID() AS database_id, * INTO #columns FROM sys.columns;

	INSERT #columns EXEC sys.sp_MSforeachdb 'use [?]; SELECT DB_ID(), * FROM sys.columns;';
END;

IF OBJECT_ID('tempdb..#tables') IS NULL BEGIN;
	SELECT TOP 0 DB_ID() AS database_id, * INTO #tables FROM sys.tables;

	INSERT #tables EXEC sys.sp_MSforeachdb 'use [?]; SELECT DB_ID(), * FROM sys.tables;';
END;

WITH t1 AS (
	SELECT 
		DB_NAME(c.database_id) AS dbname, 
		OBJECT_SCHEMA_NAME(c.[object_id], c.database_id) AS schemaname, 
		CONCAT(c.[name], '(', COUNT(*), ')') AS columnname, 
		c.[precision], 
		c.scale,
		COUNT(*) AS occurs
	FROM #columns c
	JOIN #tables t on c.[object_id] = t.[object_id]
	WHERE TYPE_NAME(c.user_type_id) IN ('decimal', 'numeric')
		AND DB_NAME(c.database_id) NOT LIKE '%snapshot%'
		AND DB_NAME(c.database_id) NOT LIKE '%Archive'
		AND DB_NAME(c.database_id) NOT IN ('tempdb', 'Utility', 'TBD', 'msdb')
		AND OBJECT_SCHEMA_NAME(c.[object_id], c.database_id) NOT IN ('sys', 'legacy')
		--AND t.create_date > '2023-01-01'
	GROUP BY c.database_id, OBJECT_SCHEMA_NAME(c.[object_id], c.database_id), c.[name], c.[precision], c.scale
)
SELECT 
	dbname, 
	schemaname, 
	[precision], 
	scale, 
	SUM(occurs) AS occurs, 
	LEFT(STRING_AGG(columnname, ', ') WITHIN GROUP (ORDER BY occurs desc), 50) AS Examples
FROM t1
GROUP BY dbname, schemaname, [precision], scale
HAVING SUM(occurs) > 3
ORDER BY occurs DESC;


