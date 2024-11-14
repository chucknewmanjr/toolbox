-- ====================================================
DECLARE @Saught sysname = 'sp_start_job';
DECLARE @MatchWholeWord BIT = 1;
-- ====================================================

IF OBJECT_ID('tempdb..#sql_modules') IS NULL BEGIN; -- drop table #sql_modules
	SELECT TOP 0 ISNULL(DB_ID(), 0) AS database_id, * INTO #sql_modules FROM sys.sql_modules WITH (NOLOCK);

	ALTER TABLE #sql_modules ADD PRIMARY KEY CLUSTERED (database_id, [object_id]);

	INSERT #sql_modules EXEC sys.sp_MSforeachdb 'USE [?]; SELECT DB_ID() AS database_id, * FROM sys.sql_modules WITH (NOLOCK) where [definition] IS NOT NULL';
END;

IF OBJECT_ID('tempdb..#objects') IS NULL BEGIN; -- drop table #objects
	SELECT TOP 0 ISNULL(DB_ID(), 0) AS database_id, * INTO #objects FROM sys.objects WITH (NOLOCK); -- create temp table

	ALTER TABLE #objects ADD PRIMARY KEY CLUSTERED (database_id, [object_id]);

	INSERT #objects EXEC sys.sp_MSforeachdb 'USE [?]; SELECT DB_ID() AS database_id, * FROM sys.objects WITH (NOLOCK);';
END;

DECLARE @Pattern sysname = REPLACE(REPLACE(@Saught, '[', '[[]'), '_', '[_]');

IF @MatchWholeWord = 1
	SET @Pattern = '[^_@#$a-z0-9]' + @Pattern + '[^_@#$a-z0-9]';

SET @Pattern = '%' + @Pattern + '%';

declare @Routine table (
	Routine_ID INT IDENTITY PRIMARY KEY, 
	DBName sysname, 
	Routine_Name sysname, 
	Contents nvarchar(max), 
	Position int, 
	Modify_Date DATE
);

insert @Routine
SELECT
	DB_NAME(m.database_id),
	OBJECT_SCHEMA_NAME(m.[object_id], m.database_id) + '.' + o.[name],
	m.[definition],
	1,
	o.modify_date
FROM #sql_modules m
JOIN #objects o ON m.database_id = o.database_id and m.[object_id] = o.[object_id]
WHERE [definition] LIKE @Pattern
	AND DB_NAME(m.database_id) NOT LIKE '%snapshot%';

delete @Routine where Routine_Name like 'dbo.sp[_]%diagram%';

update @Routine set Contents = REPLACE(Contents, CHAR(13) + CHAR(10), '  ') -- Replace CRLF with 2 spaces

update @Routine set Contents = REPLACE(Contents, CHAR(10), ' ') -- Sometimes there's only CR

update @Routine set Contents = REPLACE(Contents, CHAR(9), ' ') -- replace tabs

declare @Match table (Routine_ID int, Position int);

SET NOCOUNT ON;

while exists (select * from @Routine where Position <> 0) begin;
	update @Routine set Position = CHARINDEX(@Saught, Contents, Position + LEN(@Saught)) where Position > 0;

	insert @Match select Routine_ID, Position from @Routine where Position > 0;
end;

SET NOCOUNT OFF;

SELECT
	DBName,
	Routine_Name,
	'''' + SUBSTRING(t1.Contents, t2.Position - 100, LEN(@Saught) + 200) as Snip,
	t2.Position as Pos,
	LEN(t1.Contents) as Total,
	CAST(ROUND(t2.Position * 100.0 / LEN(t1.Contents), 0) as int) as PCT,
	Modify_Date as Modified
from @Routine t1 join @Match t2 on t1.Routine_ID = t2.Routine_ID
WHERE PATINDEX(@Pattern, SUBSTRING(t1.Contents, t2.Position - 5, LEN(@Saught) + 10)) <> 0
order by DBName, Routine_Name, Pos;

