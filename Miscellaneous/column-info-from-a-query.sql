-- ============================================================================
DECLARE @query NVARCHAR(MAX) = ' -- ===== past your query in here =====
	SELECT p.PartyID AS PharmacyID, COUNT(*)
	FROM dbo.PartyCaptureRxSwitchFee sf
	JOIN dbo.Party p
		ON (sf.PartyID = p.PartyID OR sf.PartyID = p.FinanceParentPartyID)
	JOIN dbo.Claim c
		ON p.PartyID = c.PharmacyID
	JOIN dbo.ClaimFragment cf
		ON c.ClaimID = cf.ClaimID
	JOIN WP340B.RxFileLine l
		ON cf.RxFileLineID = l.RxFileLineID
	JOIN WP340B.RxFile rxf
		ON l.RxFileID = rxf.RxFileID
	JOIN WP340B.RxFileProvider rxfp
		ON rxf.RxFileProviderID = rxfp.RxFileProviderID
	GROUP BY p.PartyID;
';
-- ============================================================================

WITH tbl AS (
	SELECT object_id
	FROM sys.tables
	WHERE @query LIKE '%[^_@#$a-z0-9]' + OBJECT_SCHEMA_NAME(object_id) + '.' + name + '[^_@#$a-z0-9]%'
), indx AS (
	SELECT 
		ic.object_id, 
		ic.column_id, 
		STRING_AGG(IIF(i.is_primary_key = 1, 'PK', IIF(i.is_unique = 1, 'UQ', 'IX') + CAST(i.index_id AS VARCHAR)), ', ') WITHIN GROUP (ORDER BY i.index_id) AS index_list
	FROM sys.indexes i
	JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
	WHERE i.object_id IN (SELECT object_id FROM tbl)
	GROUP BY ic.object_id, ic.column_id
)
SELECT DISTINCT
	OBJECT_SCHEMA_NAME(c.object_id) + '.' + OBJECT_NAME(c.object_id) AS table_name
	, c.column_id
	, name AS column_name
	, TYPE_NAME(c.user_type_id) + IIF(TYPE_NAME(c.user_type_id) LIKE '%int', '', '(' + CONCAT_WS(', ', c.max_length, NULLIF(c.precision, 0), NULLIF(c.scale, 0)) + ')') AS data_type
	, IIF(c.is_nullable = 1, 'NULL', 'NOT NULL') AS nullable
	, index_list
	, IIF(fkc.constraint_object_id IS NULL, '', 'FK') AS FK
FROM tbl
JOIN sys.columns c ON tbl.object_id = c.object_id
LEFT JOIN indx ON c.object_id = indx.object_id AND c.column_id = indx.column_id
LEFT JOIN sys.foreign_key_columns fkc ON c.object_id = fkc.parent_object_id AND c.column_id = fkc.parent_column_id
WHERE @query LIKE '%[^_@#$a-z0-9]' + name + '[^_@#$a-z0-9]%'
ORDER BY 1, c.column_id

