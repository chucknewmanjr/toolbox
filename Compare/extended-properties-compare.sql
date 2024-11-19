SET NOCOUNT ON;

DROP TABLE IF EXISTS #extended_properties;
DROP TABLE IF EXISTS #objects;
DROP TABLE IF EXISTS #columns;
DROP TABLE IF EXISTS #parameters;

SELECT TOP 0 DB_ID() AS [dbid], * INTO #extended_properties FROM sys.extended_properties;
SELECT TOP 0 DB_ID() AS [dbid], * INTO #objects FROM sys.objects;
SELECT TOP 0 DB_ID() AS [dbid], * INTO #columns FROM sys.columns;
SELECT TOP 0 DB_ID() AS [dbid], * INTO #parameters FROM sys.parameters;
GO

INSERT #extended_properties EXEC sys.sp_MSforeachdb 'use [?]; SELECT DB_ID(), * FROM sys.extended_properties;';
INSERT #objects EXEC sys.sp_MSforeachdb 'use [?]; SELECT DB_ID(), * FROM sys.objects;';
INSERT #columns EXEC sys.sp_MSforeachdb 'use [?]; SELECT DB_ID(), * FROM sys.columns;';
INSERT #parameters EXEC sys.sp_MSforeachdb 'use [?]; SELECT DB_ID(), * FROM sys.parameters;';

DECLARE @This_Snapshot TABLE (Item sysname PRIMARY KEY, ItemType sysname, [Value] VARCHAR(MAX) NOT NULL);

--Msg 2629, Level 16, State 1, Line 21
--String or binary data would be truncated in object ID '-1176321229'. Truncated value: ''.
INSERT @This_Snapshot
SELECT 
	CONCAT_WS('.', 
		DB_NAME(ep.[dbid]), 
		OBJECT_SCHEMA_NAME(ep.major_id, ep.[dbid]), 
		o.name,
		COALESCE(c.[name], p.[name], NULLIF(CONCAT(ep.minor_id, ' MISSING'), '0 MISSING'))
	), 
	CONCAT_WS('.', 
		o.[type_desc], 
		IIF(c.[name] IS NULL, NULL, 'COLUMN'), 
		IIF(p.[name] IS NULL, NULL, 'PARAMETER')
	), 
	CAST(ep.[value] AS VARCHAR(MAX))
FROM #extended_properties ep
LEFT JOIN #objects o ON ep.[dbid] = o.[dbid] AND ep.major_id = o.[object_id]
LEFT JOIN #columns c ON ep.[dbid] = c.[dbid] AND ep.major_id = c.[object_id] AND ep.minor_id = c.column_id
LEFT JOIN #parameters p ON ep.[dbid] = p.[dbid] AND ep.major_id = p.[object_id] AND ep.minor_id = p.parameter_id
WHERE ep.name = 'MS_Description';

UPDATE @This_Snapshot SET [Value] = TRANSLATE([Value], CHAR(9) + CHAR(10) + CHAR(13), '   ');

DECLARE @ServerName sysname = CAST(SERVERPROPERTY('ServerName') AS sysname);

BEGIN TRY;
	SELECT @ServerName = l.dns_name
	FROM sys.availability_group_listeners l
	JOIN sys.dm_hadr_availability_group_states s ON l.group_id = s.group_id
	WHERE s.primary_replica = @ServerName;
END TRY
BEGIN CATCH;
	PRINT ERROR_MESSAGE();
END CATCH;

insert @This_Snapshot values 
	('! Server', 'SNAPSHOT', @ServerName), 
	('! Local Time', 'SNAPSHOT', FORMAT(SYSDATETIME(), 'yyyy-MMM-d h:mmtt')), 
	('! UTC Time', 'SNAPSHOT', FORMAT(SYSUTCDATETIME(), 'yyyy-MMM-d h:mmtt')), 
	('! Time Zone', 'SNAPSHOT', CAST(DATEDIFF(hour, SYSUTCDATETIME(), SYSDATETIME()) as varchar));

-- ==========================================================
-- ==========================================================
-- ==========================================================

if OBJECT_ID('tempdb.dbo.##Other_Snapshot') is not null begin;
	select
		case
			when s.Item is null then 'only in here'
			when t.Item is null then 'only in other'
			when LEN(s.[Value]) <> LEN(t.[Value]) then 'different lengths'
			when s.[Value] <> t.[Value] then 'different values'
			else 'match'
		end as Result,
		ISNULL(s.Item, t.Item) as Item,
		ISNULL(s.ItemType, t.ItemType) as ItemType,
		ISNULL(s.[Value], '') as ThisValue, 
		ISNULL(t.[Value], '') as OtherValue
	from @This_Snapshot s
	full outer join ##Other_Snapshot t on s.Item = t.Item
	where s.[Value] <> t.[Value]
	order by 2, 3, 4, 1;
end;

declare @Output_Work table (Row_Num int identity, Section int, Line nvarchar(MAX));

INSERT @Output_Work
select 20, ',(''' + Item + ''', ''' + ItemType + ''', ''' + REPLACE([Value], '''', '''''') + ''')'
from @This_Snapshot;

update @Output_Work 
set Line = '/*' + CAST(Row_Num / 1000 as varchar) + '*/ insert ##Other_Snapshot values ' + STUFF(Line, 1, 1, '') 
where Row_Num % 1000 = 1;

declare @SuggestedFilename varchar(MAX) = '-- Suggested file name: server-extended-properties-snapshot-' + @ServerName + '-' + FORMAT(SYSUTCDATETIME(), 'yyyyMMdd') + '.sql';

insert @Output_Work
values
	(5, @SuggestedFilename), 
	(10, 'if object_id(''tempdb..##Other_Snapshot'') is not null drop table ##Other_Snapshot;'), 
	(15, 'create table ##Other_Snapshot (Item sysname PRIMARY KEY, ItemType sysname, [Value] VARCHAR(MAX) NOT NULL);'), 
	(30, 'print ''The ##Other_Snapshot temp table is ready for a comparison. Once you close this script, the table will go away.'';'),
	(35, @SuggestedFilename);

select Line as [--Permissions_Snapshot] from @Output_Work order by Section, Row_Num;
GO
