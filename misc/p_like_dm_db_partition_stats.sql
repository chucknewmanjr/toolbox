create or alter proc dbo.p_like_dm_db_partition_stats as
	select 
		p.partition_id, 
		p.object_id, 
		p.index_id, 
		p.partition_number, 
		ai.data_pages as in_row_data_page_count, 
		ai.used_pages as in_row_used_page_count, 
		ai.total_pages as in_row_reserved_page_count, 
		ISNULL(al.used_pages, 0) as lob_used_page_count, 
		ISNULL(al.total_pages, 0) as lob_reserved_page_count, 
		ISNULL(ao.used_pages, 0) as row_overflow_used_page_count, 
		ISNULL(ao.total_pages, 0) as row_overflow_reserved_page_count,
		ai.used_pages + isnull(al.used_pages, 0) + ISNULL(ao.used_pages, 0) as used_page_count, 
		ai.total_pages + ISNULL(al.total_pages, 0) + ISNULL(ao.total_pages, 0) as reserved_page_count, 
		p.rows as row_count
	from sys.partitions p
	join sys.tables t on p.object_id = t.object_id
	left JOIN sys.allocation_units ai ON ai.container_id = p.hobt_id and ai.type = 1
	left JOIN sys.allocation_units al ON al.container_id = p.partition_id and al.type = 2
	left JOIN sys.allocation_units ao ON ao.container_id = p.hobt_id and ao.type = 3
