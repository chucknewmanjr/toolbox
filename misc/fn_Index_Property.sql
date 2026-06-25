CREATE or alter FUNCTION dbo.fn_Index_Property(@table_id int, @index_id int, @column_name sysname) RETURNS nvarchar(max) AS BEGIN
	declare @x xml = (
		select * 
		from sys.indexes 
		where object_id = @table_id and index_id = @index_id 
		for xml path('index'), root('indexes')
	);

	RETURN (
		select c.value('.', 'nvarchar(max)') 
		from @x.nodes('indexes/index/*') t(c) 
		where c.value('local-name(.)', 'sysname') = LOWER(@column_name)
	);
END
GO
