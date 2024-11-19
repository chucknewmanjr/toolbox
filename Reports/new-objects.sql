SELECT 
	OBJECT_SCHEMA_NAME(o.object_id) AS [schema], 
	ISNULL(p.name, o.name) AS parent, 
	o.name, 
	o.type_desc, 
	o.modify_date,
	IIF(o.create_date = o.modify_date, 'new', 'changed') AS new
FROM sys.objects o
LEFT JOIN sys.objects p ON o.parent_object_id = p.object_id
WHERE o.modify_date > DATEADD(MINUTE, -60, SYSDATETIME())
ORDER BY 1, 2, IIF(p.name IS NULL, 0, 1), 4


