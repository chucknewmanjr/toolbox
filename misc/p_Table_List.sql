create or alter proc p_Table_List as
	set nocount on

	declare @t table (
		Table_ID int primary key clustered,
		[Schema_Name] sysname,
		Table_Name sysname,
		Key_Column sysname null,
		Column_Count smallint,
		Row_Count int,
		Data_MB decimal(9,3),
		Index_MB decimal(9,3),
		Unused_MB decimal(9,3),
		Total_Reserved_MB decimal(9,3)
	)

	insert @t (Table_ID, [Schema_Name], Table_Name, Column_Count, Row_Count, Data_MB, Index_MB, Unused_MB, Total_Reserved_MB)
	select 
		t.[object_id], 
		SCHEMA_NAME(t.[schema_id]), 
		t.[name], 
		t.max_column_id_used,
		t2.Row_Count, 
		t2.Data_MB, 
		t2.Used_MB - t2.Data_MB, 
		t2.Total_MB - t2.Used_MB, 
		t2.Total_MB
	from sys.tables t
	join (
		select 
			p.[object_id], 
			max(p.rows) as Row_Count, 
			sum(IIF(p.index_id < 2, au.data_pages, 0)) * 8.0 / 1024.0 as Data_MB, 
			sum(au.used_pages) * 8.0 / 1024.0 as Used_MB, 
			sum(au.total_pages) * 8.0 / 1024.0 as Total_MB
		from sys.partitions p
		join sys.allocation_units au 
			ON (p.hobt_id = au.container_id and au.[type] != 2) -- not LOB
			or (p.[partition_id] = au.container_id and au.[type] = 2) -- LOB
		group by p.[object_id]
	) t2 
		on t.[object_id] = t2.[object_id]

	update t1 set 
		Key_Column = t2.Key_Column
	from @t t1
	join (
		select 
			object_id, 
			case
				when count(*) = 2 and MIN(type) = 1 then '( PK and clustered index are different )'
				when count(*) = 2 and MIN(type) = 0 then '( PK without clustered index )'
				when count(*) = 1 and MIN(type) = 0 then '( No PK and no clustered index )'
				when count(*) = 1 and MIN(type) = 1 and MIN(is_primary_key*1) = 0 then '( Clustered index without PK )' -- maybe clustered index is not unique
				else null -- Clustered index is PK - use column name or column count
			end as Key_Column
		from sys.indexes 
		where type < 2 or is_primary_key = 1
		group by object_id
	) t2 
		on t1.Table_ID = t2.[object_id]

	update t1 set 
		Key_Column = t2.Key_Column
	from @t t1
	join (
		select 
			[object_id], 
			IIF(
				count(*) = 1, 
				string_agg(col_name([object_id], column_id),''), 
				formatmessage('( %d columns in PK-clustered index )', count(*))
			) as Key_Column
		from sys.index_columns
		where index_id = 1
		group by object_id
	) t2 
		on t1.Table_ID = t2.[object_id]

	select * from @t order by 2, 3
go

p_Table_List
