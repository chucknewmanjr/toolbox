/*

	SELECT REPLACE(
			'
				IF OBJECT_ID(''tempdb..#xxx'') IS NOT NULL DROP TABLE #xxx;
				SELECT TOP 0 DB_ID() AS database_id, * INTO #xxx FROM sys.xxx WITH (NOLOCK);
				CREATE CLUSTERED INDEX C_xxx ON #xxx (database_id);
				INSERT #xxx EXEC sys.sp_MSforeachdb ''use ?; SELECT DB_ID() AS database_id, * FROM sys.xxx WITH (NOLOCK);'';
				SELECT TOP 2 * FROM #xxx;
			',
			'xxx',
			'objects'
		) AS [-- SQL code that uses sys.sp_MSforeachdb]

*/

IF OBJECT_ID('tempdb..#sql_dependencies') IS NOT NULL DROP TABLE #sql_dependencies;    
SELECT TOP 0 DB_ID() AS database_id, * INTO #sql_dependencies FROM sys.sql_dependencies WITH (NOLOCK);    
CREATE CLUSTERED INDEX C_sql_dependencies ON #sql_dependencies (database_id);    
INSERT #sql_dependencies EXEC sys.sp_MSforeachdb 'use ?; SELECT DB_ID() AS database_id, * FROM sys.sql_dependencies WITH (NOLOCK);';    
SELECT TOP 2 * FROM #sql_dependencies;    

IF OBJECT_ID('tempdb..#objects') IS NOT NULL DROP TABLE #objects;    
SELECT TOP 0 DB_ID() AS database_id, * INTO #objects FROM sys.objects WITH (NOLOCK);    
CREATE CLUSTERED INDEX C_objects ON #objects (database_id);    
INSERT #objects EXEC sys.sp_MSforeachdb 'use ?; SELECT DB_ID() AS database_id, * FROM sys.objects WITH (NOLOCK);';    
SELECT TOP 2 * FROM #objects;    



