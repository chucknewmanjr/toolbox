SET NOCOUNT ON;

IF OBJECT_ID('tempdb..#principals') IS NOT NULL DROP TABLE #principals;
IF OBJECT_ID('tempdb..#role_members') IS NOT NULL DROP TABLE #role_members;
IF OBJECT_ID('tempdb..#permissions') IS NOT NULL DROP TABLE #permissions;
IF OBJECT_ID('tempdb..#objects') IS NOT NULL DROP TABLE #objects;
IF OBJECT_ID('tempdb..#schemas') IS NOT NULL DROP TABLE #schemas;
IF OBJECT_ID('tempdb..#types') IS NOT NULL DROP TABLE #types;
IF OBJECT_ID('tempdb..#xml_schema_collections') IS NOT NULL DROP TABLE #xml_schema_collections;

SELECT TOP 0 DB_NAME() AS [db_name], * INTO #principals FROM sys.database_principals;
SELECT TOP 0 DB_NAME() AS [db_name], * INTO #role_members FROM sys.database_role_members;
SELECT TOP 0 DB_NAME() AS [db_name], * INTO #permissions FROM sys.database_permissions;
SELECT TOP 0 DB_NAME() AS [db_name], * INTO #objects FROM sys.objects;
SELECT TOP 0 DB_NAME() AS [db_name], * INTO #schemas FROM sys.schemas;
SELECT TOP 0 DB_NAME() AS [db_name], * INTO #types FROM sys.types;
SELECT TOP 0 DB_NAME() AS [db_name], * INTO #xml_schema_collections FROM sys.xml_schema_collections;
GO -- schema changes go above this go

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

declare @This_Snapshot table (
	Parent varchar(500) not null default '', -- Used to exclude results depending on parent results. ex database, server principal or database and database principal
	Item_Type sysname not null, -- ex database, server principal, database principal, server permission, ...
	Item sysname not null,
	Properties varchar(500) not null default ''
);

insert @This_Snapshot (Item_Type, Item, Properties) values 
	('SNAPSHOT', '! Server', @ServerName), 
	('SNAPSHOT', '! Local Time', FORMAT(SYSDATETIME(), 'yyyy-MMM-d h:mmtt')), 
	('SNAPSHOT', '! UTC Time', FORMAT(SYSUTCDATETIME(), 'yyyy-MMM-d h:mmtt')), 
	('SNAPSHOT', '! Time Zone', CAST(DATEDIFF(hour, SYSUTCDATETIME(), SYSDATETIME()) as varchar));

INSERT @This_Snapshot (Item_Type, Item, Properties)
SELECT
	'SERVER PRINCIPAL',
	[name],
	CONCAT('type="', [type_desc] COLLATE DATABASE_DEFAULT, '", default_db="', default_database_name, '"')
FROM sys.server_principals;

INSERT @This_Snapshot (Parent, Item_Type, Item, Properties)
SELECT
	grantee.name,
	'SERVER PERMISSION',
	p1.[permission_name],
	'class="' + p1.class_desc + '", state="' + p1.state_desc + '"'
FROM sys.server_permissions p1
JOIN sys.server_principals grantee ON p1.grantee_principal_id = grantee.principal_id
WHERE p1.class_desc NOT IN ('ENDPOINT');

INSERT @This_Snapshot (Parent, Item_Type, Item, Properties)
SELECT
	grantee.[name],
	'ENDPOINT PERMISSION',
	e.[name],
	'permission="' + p1.[permission_name] + '", state="' + p1.state_desc + '"'
FROM sys.server_permissions p1
JOIN sys.server_principals grantee ON p1.grantee_principal_id = grantee.principal_id
JOIN sys.endpoints e ON p1.major_id = e.endpoint_id
WHERE p1.class_desc IN ('ENDPOINT');

INSERT @This_Snapshot (Parent, Item_Type, Item, Properties)
SELECT DISTINCT
	ISNULL(m.[name], ''),
	'SERVER ROLE MEMBER',
	r.[name],
	CONCAT('type="', m.[type_desc], '" (Item: DB member role)')
FROM sys.server_role_members rm
LEFT JOIN sys.server_principals r ON rm.role_principal_id = r.principal_id
LEFT JOIN sys.server_principals m ON rm.member_principal_id = m.principal_id;

-- ==========================================================
-- ==========================================================
-- ==========================================================

insert @This_Snapshot (Item_Type, Item, Properties)
SELECT 
	'DB',
	[name],
	CONCAT(
		'compatibility_level=',[compatibility_level]
		,', snapshot_isolation_state=',snapshot_isolation_state_desc
		,', is_auto_create_stats_incremental_on=',is_auto_create_stats_incremental_on
		,', is_ansi_nulls_on=',is_ansi_nulls_on
		,', is_local_cursor_default=',is_local_cursor_default
		,', is_fulltext_enabled=',is_fulltext_enabled
		,', is_trustworthy_on=',is_trustworthy_on
		,', is_db_chaining_on=',is_db_chaining_on
		,', is_master_key_encrypted_by_server=',is_master_key_encrypted_by_server
	)
FROM sys.databases;

-- ==========================================================
-- ==========================================================
-- ==========================================================

INSERT #principals EXEC sys.sp_MSforeachdb 'use [?]; SELECT DB_NAME(), * FROM sys.database_principals';
INSERT #role_members EXEC sys.sp_MSforeachdb 'use [?]; SELECT DB_NAME(), * FROM sys.database_role_members';
INSERT #permissions EXEC sys.sp_MSforeachdb 'use [?]; SELECT DB_NAME(), * FROM sys.database_permissions';
INSERT #objects EXEC sys.sp_MSforeachdb 'use [?]; SELECT DB_NAME(), * FROM sys.objects';
INSERT #schemas EXEC sys.sp_MSforeachdb 'use [?]; SELECT DB_NAME(), * FROM sys.schemas';
INSERT #types EXEC sys.sp_MSforeachdb 'use [?]; SELECT DB_NAME(), * FROM sys.types';
INSERT #xml_schema_collections EXEC sys.sp_MSforeachdb 'use [?]; SELECT DB_NAME(), * FROM sys.xml_schema_collections';

-- ==========================================================
-- ==========================================================
-- ==========================================================

INSERT @This_Snapshot
SELECT
	[db_name],
	'DB PRINCIPAL',
	[name],
	CONCAT(
		'type="', [type_desc] COLLATE DATABASE_DEFAULT,
		'", default_schema="', default_schema_name,
		'", authentication_type="', authentication_type_desc, '"'
	)
FROM #principals;

INSERT @This_Snapshot
SELECT DISTINCT
	rm.[db_name] + '.' + m.[name],
	'DB ROLE MEMBER',
	r.[name],
	CONCAT('type="', m.[type_desc], '" (Item: DB member role)')
FROM #role_members rm
LEFT JOIN #principals r ON rm.role_principal_id = r.principal_id AND rm.[db_name] = r.[db_name]
LEFT JOIN #principals m ON rm.member_principal_id = m.principal_id AND rm.[db_name] = m.[db_name];

INSERT @This_Snapshot (Parent, Item_Type, Item)
SELECT 
	p2.[db_name] + '.' + p2.[name],
	p1.class_desc,
	p1.[permission_name]
FROM #permissions p1
JOIN #principals p2 ON p1.grantee_principal_id = p2.principal_id AND p1.[db_name] = p2.[db_name]
WHERE p1.class_desc NOT IN ('TYPE', 'OBJECT_OR_COLUMN', 'SCHEMA', 'XML_SCHEMA_COLLECTION');

INSERT @This_Snapshot (Parent, Item_Type, Item)
SELECT 
	p2.[db_name] + '.' + p2.[name] + ' ' + x.[name],
	p1.class_desc,
	p1.[permission_name]
FROM #permissions p1
JOIN #principals p2 ON p1.grantee_principal_id = p2.principal_id AND p1.[db_name] = p2.[db_name]
LEFT JOIN #schemas x ON p1.major_id = x.[schema_id] AND p1.[db_name] = x.[db_name]
WHERE p1.class_desc IN ('SCHEMA');

INSERT @This_Snapshot (Parent, Item_Type, Item)
SELECT 
	p2.[db_name] + '.' + p2.[name] + ' ' + x.[name],
	p1.class_desc,
	p1.[permission_name]
FROM #permissions p1
JOIN #principals p2 ON p1.grantee_principal_id = p2.principal_id AND p1.[db_name] = p2.[db_name]
LEFT JOIN #xml_schema_collections x ON p1.major_id = x.xml_collection_id AND p1.[db_name] = x.[db_name]
WHERE p1.class_desc IN ('XML_SCHEMA_COLLECTION');

INSERT @This_Snapshot (Parent, Item_Type, Item)
SELECT 
	p2.[db_name] + '.' + p2.[name] + ' ' + x.[name],
	p1.class_desc,
	p1.[permission_name]
FROM #permissions p1
JOIN #principals p2 ON p1.grantee_principal_id = p2.principal_id AND p1.[db_name] = p2.[db_name]
LEFT JOIN #types x ON p1.major_id = x.user_type_id AND p1.[db_name] = x.[db_name]
WHERE p1.class_desc IN ('TYPE');

INSERT @This_Snapshot
SELECT 
	p2.[db_name] + '.' + p2.[name] + ' ' + p2.[db_name] + '.' + OBJECT_SCHEMA_NAME(x.[object_id], DB_ID(x.[db_name])) + '.' + x.[name],
	x.[type_desc],
	p1.[permission_name],
	'state="' + p1.state_desc + '"'
FROM #permissions p1
JOIN #principals p2 ON p1.grantee_principal_id = p2.principal_id AND p1.[db_name] = p2.[db_name]
JOIN #objects x ON p1.major_id = x.object_id AND p1.[db_name] = x.[db_name]
WHERE p1.class_desc IN ('OBJECT_OR_COLUMN');

-- ==========================================================
-- ==========================================================
-- ==========================================================

if OBJECT_ID('tempdb.dbo.##Other_Snapshot') is not null begin;
	declare @source table (Parent varchar(500), Item_Type sysname, Item varchar(500), Properties varchar(500));

	declare @target table (Parent varchar(500), Item_Type sysname, Item varchar(500), Properties varchar(500));

	insert @source select * from ##Other_Snapshot; -- This data is from earlier or somewhere else.

	insert @target select * from @This_Snapshot; -- This data is from here and now.

	-- The Item value is stored without the parent value. Now let's put it back in.
	update @source set Item = Parent + ' ' + Item WHERE Parent <> '';

	update @target set Item = Parent + ' ' + Item WHERE Parent <> '';

	-- Delete children if the parent doesn't exist in the other dataset.
	-- To do that, join parent to item.
	delete s
	from @source s
	left join @target t on s.Parent = t.Item and t.Item <> t.Parent
	where s.Parent > '' and t.Item is null;

	delete t
	from @target t 
	left join @source s on t.Parent = s.Item and s.Item <> s.Parent
	where t.Parent > '' and s.Item is null;

	select
		case
			when s.Item is null then 'only in target'
			when t.Item is null then 'only in source'
			when s.Properties <> t.Properties then 'different Properties'
			else 'match'
		end as Result,
		ISNULL(s.Item, t.Item) as Item,
		ISNULL(s.Item_Type, t.Item_Type) as Item_Type,
		ISNULL(s.Properties, '') as Source_Properties, 
		ISNULL(t.Properties, '') as Target_Properties
	from @source s
	full outer join @target t on s.Item = t.Item and s.Item_Type = t.Item_Type
	where ISNULL(s.Item, '') <> ISNULL(t.Item, '')
		or ISNULL(s.Item_Type, '') <> ISNULL(t.Item_Type, '')
		or ISNULL(s.Properties, '') <> ISNULL(t.Properties, '')
	order by 2, 3, 4, 1;
end;

declare @Output_Work table (Row_Num int identity, Section int, Line nvarchar(500));

INSERT @Output_Work
select 20, ',(''' + Parent + ''', ''' + Item_Type + ''', ''' + Item + ''', ''' + REPLACE(Properties, '''', '''''') + ''')'
from @This_Snapshot;

update @Output_Work 
set Line = '/*' + CAST(Row_Num / 1000 as varchar) + '*/ insert ##Other_Snapshot values ' + STUFF(Line, 1, 1, '') 
where Row_Num % 1000 = 1;

declare @SuggestedFilename varchar(MAX) = '-- Suggested file name: server-permissions-snapshot-' + @ServerName + '-' + FORMAT(SYSUTCDATETIME(), 'yyyyMMdd') + '.sql';

insert @Output_Work
values
	(5, @SuggestedFilename), 
	(10, 'if object_id(''tempdb..##Other_Snapshot'') is not null drop table ##Other_Snapshot;'), 
	(15, 'create table ##Other_Snapshot (Parent sysname, Item_Type sysname, Item sysname, Properties nvarchar(500));'), 
	(30, 'print ''The ##Other_Snapshot temp table is ready for a comparison. Once you close this script, the table will go away.'';'),
	(35, @SuggestedFilename);

select Line as [--Permissions_Snapshot] from @Output_Work order by Section, Row_Num;
GO
