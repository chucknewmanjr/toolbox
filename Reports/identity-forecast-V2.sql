IF OBJECT_ID('tempdb..#default_constraints') IS NULL BEGIN; -- drop table #default_constraints
	SELECT TOP 0 DB_ID() AS database_id, * INTO #default_constraints FROM sys.default_constraints WITH (NOLOCK);

	INSERT #default_constraints EXEC sys.sp_MSforeachdb 'USE [?]; SELECT DB_ID() AS database_id, * FROM sys.default_constraints WITH (NOLOCK)';
END;

IF OBJECT_ID('tempdb..#columns') IS NULL BEGIN; -- drop table #columns
	SELECT TOP 0 DB_ID() AS database_id, * INTO #columns FROM sys.columns WITH (NOLOCK);

	INSERT #columns EXEC sys.sp_MSforeachdb 'USE [?]; SELECT DB_ID() AS database_id, * FROM sys.columns WITH (NOLOCK)';
END;

IF OBJECT_ID('tempdb..#identity_columns') IS NULL BEGIN; -- drop table #identity_columns
	SELECT TOP 0 DB_ID() AS database_id, * INTO #identity_columns FROM sys.identity_columns WITH (NOLOCK);

	INSERT #identity_columns EXEC sys.sp_MSforeachdb 'USE [?]; SELECT DB_ID() AS database_id, * FROM sys.identity_columns WITH (NOLOCK)';
END;
GO

WITH Identities AS (
	SELECT 
		DB_NAME(ic.database_id) AS DatabaseName,
		OBJECT_SCHEMA_NAME(ic.object_id, ic.database_id) + '.' + OBJECT_NAME(ic.object_id, ic.database_id) AS TableName,
		[name] AS ColumnName,
		TYPE_NAME(ic.user_type_id) AS DataType,
		ic.last_value,
		ROUND(CONVERT(FLOAT, ic.last_value) * 100.0 / POWER(CAST(2 AS FLOAT), ic.max_length * 8 - 2) / 2, 1) AS pct
	FROM #identity_columns ic
	WHERE ic.last_value IS NOT NULL
)
SELECT *
FROM Identities
WHERE pct > 25
ORDER BY pct DESC;
GO

DROP TABLE IF EXISTS #Identities;
GO

SELECT 
	ROW_NUMBER() OVER (ORDER BY c.database_id) AS IdentityID,
	DB_NAME(c.database_id) + '.' + OBJECT_SCHEMA_NAME(c.[object_id], c.database_id) + '.' + OBJECT_NAME(c.[object_id], c.database_id) AS TableName,
	c.[name] AS DateColumn,
	ic.[name] AS IdentityColumn, 
	CONVERT(BIGINT, ic.last_value) AS last_value,
	map.*
INTO #Identities
FROM #default_constraints dc
JOIN #columns c ON c.database_id = dc.database_id AND c.[object_id] = dc.parent_object_id AND c.column_id = dc.parent_column_id
JOIN #identity_columns ic ON ic.database_id = dc.database_id AND ic.[object_id] = dc.parent_object_id
JOIN (VALUES
	('bigint', 9223372036854775807),
	('int', 2147483647),
	('smallint', 32767),
	('tinyint', 255)
) map (DataType, IDLimit) ON TYPE_NAME(ic.user_type_id) = map.DataType
WHERE dc.[definition] LIKE '%date%'
	AND TYPE_NAME(c.user_type_id) LIKE '%date%'
	AND c.is_nullable = 0
	AND ic.last_value IS NOT NULL
	AND c.[name] NOT IN ('UpdateDate', 'LastModifiedTimestamp')
GO

DROP TABLE IF EXISTS #Stats;
GO

CREATE TABLE #Stats (TableName VARCHAR(200), IDValue BIGINT, CreateDate DATE, IDLimit BIGINT);
GO

DECLARE @Delta BIGINT;
DECLARE @SQL VARCHAR(MAX);
DECLARE @ThisIdentityID INT = (SELECT MAX(IdentityID) FROM #Identities);

WHILE @ThisIdentityID > 0 BEGIN;
	SELECT @SQL = CONCAT(
			'select ''', 
			TableName, ''' as TableName, ', 
			IdentityColumn, ' as IDValue, ', 
			DateColumn, ' as CreateDate, ', 
			IDLimit, ' as IDLimit from ', 
			TableName, ' where ', 
			IdentityColumn, ' IN (', 
			(last_value - 1), ', ',
			(last_value - 100), ', ',
			(last_value - 10000), ', ', -- 10K
			(last_value - 1000000), ', ', -- million
			(last_value - 100000000), ', ', -- 100 million
			(last_value - 10000000000), '); ' -- 10 billion
		)
	FROM #Identities
	WHERE IdentityID = @ThisIdentityID;

	IF @ThisIdentityID % 10 = 0 BEGIN;
		PRINT @ThisIdentityID;

		PRINT @SQL;
	END;

	INSERT #Stats EXEC (@SQL);

	SET @ThisIdentityID -= 1;
END;
GO

WITH [Range] AS (
	SELECT
		s1.TableName,
		s1.IDLimit,
		CAST(s1.IDValue AS REAL) AS MinID,
		CAST(s2.IDValue AS REAL) AS MaxID,
		s1.CreateDate AS MinDate,
		s2.CreateDate AS MaxDate
	FROM #Stats s1
	JOIN #Stats s2 ON s1.TableName = s2.TableName
	WHERE s1.IDValue < s2.IDValue
		AND DATEDIFF(DAY, s1.CreateDate, s2.CreateDate) > 5
), [Data] AS (
	SELECT TOP 400 *, (IDLimit-MaxID) / NULLIF(MaxID-MinID, 0) * DATEDIFF(DAY, MinDate, MaxDate) AS Factor
	FROM [Range]
	ORDER BY Factor
)
SELECT TOP 10 TableName, MIN(DATEADD(DAY, Factor, MaxDate)) AS FromForcast, MAX(DATEADD(DAY, Factor, MaxDate)) AS ToForcast
FROM [Data]
GROUP BY TableName
ORDER BY FromForcast;
GO


