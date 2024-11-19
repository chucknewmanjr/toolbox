
SELECT 
	p2.type_desc AS grantee_type,
	p2.name AS grantee_name,
	p1.permission_name
FROM sys.database_permissions p1
JOIN sys.database_principals p2 ON p1.grantee_principal_id = p2.principal_id
WHERE p1.class_desc IN ('DATABASE')
ORDER BY 1, 2, 3

SELECT 
	p2.name AS grantee_name,
	t.name AS type_name
FROM sys.database_permissions p1
JOIN sys.database_principals p2 ON p1.grantee_principal_id = p2.principal_id
LEFT JOIN sys.types t ON p1.major_id = t.user_type_id
WHERE p1.class_desc IN ('TYPE')

SELECT 
	CONCAT(p2.name, ' (' + IIF(p2.type_desc IN ('DATABASE_ROLE'), NULL, p2.type_desc COLLATE DATABASE_DEFAULT) + ')') AS grantee,
	IIF(p1.permission_name = 'EXECUTE', '', p1.permission_name) AS permission,
	CONCAT(OBJECT_SCHEMA_NAME(p1.major_id), '.', o.name, ' (' + IIF(o.type_desc = 'SQL_STORED_PROCEDURE', NULL, o.type_desc COLLATE DATABASE_DEFAULT) + ')') AS object_name
FROM sys.database_permissions p1
JOIN sys.database_principals p2 ON p1.grantee_principal_id = p2.principal_id
JOIN sys.objects o ON p1.major_id = o.object_id
WHERE p1.class_desc IN ('OBJECT_OR_COLUMN')
	AND o.name NOT LIKE '%diagram%'



