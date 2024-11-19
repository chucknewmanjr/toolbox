/*
	===== instructions =====
	This script is for comparing table schemas.
	It can compare different tables, tables in different servers or the same table to an earlier version.
	This is achived with a schema data snapshot.
	If this script returns one resultset when you run it, then that's a schema snapshot.
	You can copy those results into its own query window, you can save it for later.
	Or you can execute it to make a temp table that's ready for a comparison.
	If this script returns 2 resultsets then the first resultset is the results of a comparison.
*/

if DB_NAME() = 'master' throw 50000, 'Don''t run on master', 1;
GO

-- ==================================================================
declare @Table_Name sysname = 'dbo.FactProforma'
-- ==================================================================

set nocount on;

declare @Table_ID int = OBJECT_ID(@Table_Name, 'U')

if @Table_ID is null throw 50000, 'Table not found', 1;

declare @Data table (
	Item nvarchar(500) not null, 
	Property_Type sysname not null, 
	Properties nvarchar(500) not null default ''
);



insert @Data VALUES 
	(' ! server', 'Snapshot', @@servername),
	(' ! db', 'Snapshot', DB_NAME()),
	(' ! schema', 'Snapshot', OBJECT_SCHEMA_NAME(@Table_ID)),
	(' ! table', 'Snapshot', OBJECT_NAME(@Table_ID)),
	(' ! Local Time', 'Snapshot', FORMAT(SYSDATETIME(), 'yyyy-MMM-d h:mmtt')), 
	(' ! UTC Time', 'Snapshot', FORMAT(SYSUTCDATETIME(), 'yyyy-MMM-d h:mmtt')),
	(' ! Time Zone', 'Snapshot', CAST(DATEDIFF(hour, SYSUTCDATETIME(), SYSDATETIME()) as varchar));

insert @Data
SELECT
	c.[name],
	'Column',
	CONCAT(
		CASE
			WHEN 'binary char datetime2 float nchar nvarchar varbinary varchar' LIKE '%' + TYPE_NAME(c.user_type_id) + '%'
			THEN TYPE_NAME(c.user_type_id) + ISNULL(NULLIF(CONCAT('(', c.max_length, ')'), '(-1)'), '(MAX)')
			WHEN 'decimal numeric' LIKE '%' + TYPE_NAME(c.user_type_id) + '%'
			THEN CONCAT(TYPE_NAME(c.user_type_id), '(', c.[precision], ', ', c.scale, ')')
			ELSE TYPE_NAME(c.user_type_id)
		END, 
		IIF(c.is_nullable = 0, ' NOT NULL', ' NULL'),
		IIF(c.is_identity = 0, '', ' IDENTITY'),
		ISNULL(' Default=' + d.[definition], ''),
		ISNULL(' Computed=' + cc.[definition], ''),
		IIF(ISNULL(c.collation_name, 'SQL_Latin1_General_CP1_CI_AS') = 'SQL_Latin1_General_CP1_CI_AS', '', ' Collation=' + c.collation_name)
	)
FROM sys.columns c
LEFT JOIN sys.default_constraints d ON c.default_object_id = d.object_id
LEFT JOIN sys.computed_columns cc ON c.object_id = cc.object_id AND c.column_id = cc.column_id
WHERE c.[object_id] = @Table_ID

insert @Data
select
	[name],
	'Check Constraint',
	ISNULL('Column=' + COL_NAME(parent_object_id, parent_column_id), 'Table Level') +
    ', Definition=' + [definition]
from sys.check_constraints
WHERE parent_object_id = @Table_ID

insert @Data
SELECT 
	[name],
	'Index',
	ISNULL(STUFF(CONCAT(
		IIF([type_desc] = 'NONCLUSTERED', '', ', ' + [type_desc]) COLLATE DATABASE_DEFAULT,
		IIF(is_unique = 0, '', ', UNIQUE'),
		IIF(is_primary_key = 0, '', ', PK'),
		IIF(fill_factor NOT BETWEEN 1 AND 99, '', CONCAT(', FILL FACTOR=', fill_factor)),
		ISNULL(', Filter=' + filter_definition, '')
	), 1, 2, ''), '')
FROM sys.indexes
WHERE [object_id] = @Table_ID

insert @Data
SELECT 
	i.[name] + '.' + COL_NAME(ic.[object_id], ic.column_id),
	'Index Column',
	IIF(ic.is_included_column = 1, 'INCLUDED', '')
FROM sys.indexes i
JOIN sys.index_columns ic ON i.[object_id] = ic.[object_id] AND i.index_id = ic.index_id
WHERE i.[object_id] = @Table_ID

insert @Data
SELECT 
	[name], 
	'FK',
	STUFF(CONCAT(
		', Referenced=', 
		OBJECT_SCHEMA_NAME(referenced_object_id), '.', OBJECT_NAME(referenced_object_id),
		IIF(delete_referential_action_desc = 'NO_ACTION', '', ', Delete=' + delete_referential_action_desc),
		IIF(update_referential_action_desc = 'NO_ACTION', '', ', Update=' + update_referential_action_desc)
	), 1, 2, '')
FROM sys.foreign_keys
WHERE parent_object_id = @Table_ID

insert @Data
SELECT 
	OBJECT_NAME(constraint_object_id) + '.' + COL_NAME(parent_object_id, parent_column_id),
	'FK Column',
	''
FROM sys.foreign_key_columns
WHERE parent_object_id = @Table_ID

if OBJECT_ID('tempdb.dbo.##Table_Schema_Snapshot') is not null begin;
	declare @source table (Item nvarchar(500), Property_Type sysname, Properties nvarchar(500));

	declare @target table (Item nvarchar(500), Property_Type sysname, Properties nvarchar(500));

	insert @source select * from ##Table_Schema_Snapshot; -- This data is from earlier or somewhere else.

	insert @target select * from @Data; -- This data is from here and now.

	select
		case
			when s.Item is null then 'only in target'
			when t.Item is null then 'only in source'
			when s.Properties <> t.Properties then 'different Properties'
			else 'match'
		end as Result,
		ISNULL(s.Item, t.Item) as Item,
		ISNULL(s.Property_Type, t.Property_Type) as Property_Type,
		ISNULL(s.Properties, '') as Source_Properties, 
		ISNULL(t.Properties, '') as Target_Properties
	from @source s
	full outer join @target t on s.Item = t.Item and s.Property_Type = t.Property_Type
	where ISNULL(s.Item, '') <> ISNULL(t.Item, '')
		or ISNULL(s.Property_Type, '') <> ISNULL(t.Property_Type, '')
		or ISNULL(s.Properties, '') <> ISNULL(t.Properties, '')
	order by 2, 3, 4, 1;
end;

declare @Output_Work table (Row_Num int identity, Section int, Line nvarchar(2000));

insert @Output_Work
select 20, ',(''' + Item + ''', ''' + Property_Type + ''', ''' + REPLACE(Properties, '''', '''''') + ''')'
from @Data
order by Item, Property_Type, Properties;

update @Output_Work 
set Line = '/*' + CAST(Row_Num / 1000 as varchar) + '*/ insert ##Table_Schema_Snapshot values ' + STUFF(Line, 1, 1, '') 
where Row_Num % 1000 = 1;

declare @SuggestedFilename varchar(MAX) = CONCAT(
	'-- Suggested file name: snapshot-', 
	@@SERVERNAME, '-', 
	DB_NAME(), '-',
	OBJECT_SCHEMA_NAME(@Table_ID), '-',
	OBJECT_NAME(@Table_ID), '-',
	FORMAT(SYSUTCDATETIME(), 'yyyyMMdd'), 
	'.sql'
);

insert @Output_Work
values
	(5, @SuggestedFilename), 
	(10, 'if OBJECT_ID(''tempdb..##Table_Schema_Snapshot'') is not null drop table ##Table_Schema_Snapshot;'), 
	(15, 'create table ##Table_Schema_Snapshot (Item nvarchar(500), Property_Type sysname, Properties nvarchar(500));'), 
	(30, 'print ''The ##Table_Schema_Snapshot temp table is ready for a comparison. Once you close this script, the table will go away.'';'),
	(35, @SuggestedFilename);

select Line as [--Snapshot] from @Output_Work order by Section, Row_Num;



