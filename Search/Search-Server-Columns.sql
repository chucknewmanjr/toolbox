DECLARE @ColumnName sysname = 'OPA'

-- ===== TO-DO =====
-- Is unique index a constraint? don't confuse with clustered
-- multiple columns can participate in a check constraint
-- more descriptions and suffix descriptions

-- =================================================================
-- ===== collect data from all of the databases in preporation =====

SELECT * FROM (VALUES
	('IX1', 'Column participates in index #1'),
	('UQ1', 'Column is in unique index or constraint #1'),
	('PK', 'Column participates in primary key'),
	('PKC', 'C suffix means clustered'),
	('(IX1)', 'Parenthises mean column is "included" in index'),
	('IXf', 'f suffix means index is filtered'),
	('FK1', 'Column participates in foreign key #1'),
	('FK1-action', 'update or delete actions cascade to other table'),
	('CK', 'Check constraint'),
	('CK1', 'Column is in CK #1 with other columns'),
	('DF', 'Default'),
	('I', 'Identity'),
	('comp', 'Computed column')
) t (Feature, [Description])

IF OBJECT_ID('temp..#tables') IS NULL BEGIN;
	PRINT FORMAT(SYSDATETIME(), 'HH:mm:ss.fff');

	SELECT TOP 0 DB_ID() AS Database_ID, * INTO #tables FROM sys.tables;
	ALTER TABLE #tables ADD UNIQUE CLUSTERED (Database_ID, [object_id]);

	SELECT TOP 0 DB_ID() AS Database_ID, * INTO #columns FROM sys.columns;
	ALTER TABLE #columns ADD UNIQUE CLUSTERED (Database_ID, [object_id], column_id);

	SELECT TOP 0 DB_ID() AS Database_ID, * INTO #indexes FROM sys.indexes;
	ALTER TABLE #indexes ADD UNIQUE CLUSTERED (Database_ID, [object_id], index_id);

	SELECT TOP 0 DB_ID() AS Database_ID, * INTO #index_columns FROM sys.index_columns;
	ALTER TABLE #index_columns ADD UNIQUE CLUSTERED (Database_ID, [object_id], index_id, column_id);

	SELECT TOP 0 DB_ID() AS Database_ID, * INTO #foreign_keys FROM sys.foreign_keys;
	ALTER TABLE #foreign_keys ADD UNIQUE CLUSTERED (Database_ID, [object_id]);

	SELECT TOP 0 DB_ID() AS Database_ID, * INTO #foreign_key_columns FROM sys.foreign_key_columns;
	ALTER TABLE #foreign_key_columns ADD UNIQUE CLUSTERED (Database_ID, constraint_object_id, constraint_column_id);

	SELECT TOP 0 DB_ID() AS Database_ID, * INTO #check_constraints FROM sys.check_constraints;

	SELECT TOP 0 DB_ID() AS Database_ID, * INTO #extended_properties FROM sys.extended_properties;

	-- Excluding tempdb avoids common lock issues
	INSERT #tables EXEC sp_msforeachdb 'use ?; SELECT db_id(), * FROM sys.tables WITH (NOLOCK) WHERE DB_NAME() NOT IN (''tempdb'');';
	INSERT #columns EXEC sp_msforeachdb 'use ?; SELECT db_id(), * FROM sys.columns WITH (NOLOCK) WHERE DB_NAME() NOT IN (''tempdb'');';
	INSERT #indexes EXEC sp_msforeachdb 'use ?; SELECT db_id(), * FROM sys.indexes WITH (NOLOCK) WHERE DB_NAME() NOT IN (''tempdb'');'; -- 10k rows
	INSERT #index_columns EXEC sp_msforeachdb 'use ?; SELECT db_id(), * FROM sys.index_columns WITH (NOLOCK) WHERE DB_NAME() NOT IN (''tempdb'');'; -- 20k rows
	INSERT #foreign_keys EXEC sp_msforeachdb 'use ?; SELECT db_id(), * FROM sys.foreign_keys WITH (NOLOCK) WHERE DB_NAME() NOT IN (''tempdb'');';
	INSERT #foreign_key_columns EXEC sp_msforeachdb 'use ?; SELECT db_id(), * FROM sys.foreign_key_columns WITH (NOLOCK) WHERE DB_NAME() NOT IN (''tempdb'');';
	INSERT #check_constraints EXEC sp_msforeachdb 'use ?; SELECT db_id(), * FROM sys.check_constraints WITH (NOLOCK) WHERE DB_NAME() NOT IN (''tempdb'');';
	INSERT #extended_properties EXEC sp_msforeachdb 'use ?; SELECT db_id(), * FROM sys.extended_properties WITH (NOLOCK) WHERE DB_NAME() NOT IN (''tempdb'');';
END;

PRINT FORMAT(SYSDATETIME(), 'HH:mm:ss.fff');

DROP TABLE IF EXISTS #Column_Label;

DECLARE @Index_and_FK_Column TABLE(
	Database_ID INT,
	Table_ID INT,
	Is_Index BIT,
	Index_or_FK_ID INT,
	Column_ID INT,
	Is_Included_Column BIT
);

INSERT @Index_and_FK_Column
SELECT Database_ID, [object_id], 1, index_id, column_id, is_included_column
FROM #index_columns
UNION ALL
SELECT fkc.Database_ID, fkc.parent_object_id, 0, fkc.constraint_object_id, fkc.parent_column_id, 0
FROM #foreign_key_columns fkc;

DELETE @Index_and_FK_Column WHERE OBJECT_SCHEMA_NAME(Table_ID, Database_ID) IN ('sys');

DECLARE @AVG_Column TABLE(
	Database_ID INT,
	Table_ID INT,
	Is_Index BIT,
	Index_or_FK_ID INT,
	PRIMARY KEY (Database_ID, Table_ID, Is_Index, Index_or_FK_ID),
	Avg_Column_ID INT
);

-- To rename indexes, order indexes by average column number
INSERT @AVG_Column
SELECT Database_ID, Table_ID, Is_Index, Index_or_FK_ID, AVG(Column_ID + 0.0) AS Avg_Column_ID
FROM @Index_and_FK_Column
WHERE Is_Included_Column = 0
GROUP BY Database_ID, Table_ID, Is_Index, Index_or_FK_ID;

WITH Label_Prep AS (
	-- perpare to make labels
	SELECT 
		c.Database_ID, -- - -\
		c.Table_ID, -- - - - ->- KEY for join
		c.Index_or_FK_ID, -- /
		CASE
			WHEN i.is_primary_key IS NULL THEN 4 -- PK=1, UQ=3, IX=3, FK=4
			WHEN i.is_primary_key = 1 THEN 1
			WHEN i.is_unique = 1 THEN 2
			ELSE 3
		END AS Category,
		IIF(i.[type_desc] = 'CLUSTERED', 'C', '') AS Cluster_Suffix,
		ROW_NUMBER() OVER (
			PARTITION BY c.Database_ID, c.Table_ID, i.is_unique, i.is_primary_key -- Ordered by average column number
			ORDER BY c.Avg_Column_ID
		) AS Label_Number,
		IIF(i.has_filter = 1, 'f', '') AS Filter_Suffix,
		IIF(fk.delete_referential_action > 0 OR fk.update_referential_action > 0, '-action', '') AS FK_Action_Suffix
	FROM @AVG_Column c
	LEFT JOIN #indexes i ON c.Database_ID = i.Database_ID AND c.Table_ID = i.[object_id] AND c.Index_or_FK_ID = i.index_id
	LEFT JOIN #foreign_keys fk ON c.Database_ID = fk.Database_ID AND c.Index_or_FK_ID = fk.[object_id]
) -- make labels for every index and also join in columns
SELECT 
	lp.Database_ID, -- \
	lp.Table_ID, -- - - >- these 3 are for grouping
	ifkc.Column_ID, -- /
	ifkc.Is_Included_Column, -- \
	lp.Category, -- - - - - - - ->- these 3 are for ordering
	lp.Label_Number, -- - - - - /
	CONCAT(
		IIF(ifkc.Is_Included_Column = 1, '(', ''), 
		CHOOSE(lp.Category, 'PK', 'UQ', 'IX', 'FK'), 
		Cluster_Suffix, 
		IIF(lp.Category = 1, '', LTRIM(STR(lp.Label_Number))), -- Only 1 PK per table. So no number needed.
		Filter_Suffix,
		FK_Action_Suffix,
		IIF(ifkc.Is_Included_Column = 1, ')', '')
	) AS [Label]
INTO #Column_Label
FROM Label_Prep lp
JOIN @Index_and_FK_Column ifkc ON lp.Database_ID = ifkc.Database_ID AND lp.Table_ID = ifkc.Table_ID AND lp.Index_or_FK_ID = ifkc.Index_or_FK_ID;

INSERT #Column_Label SELECT Database_ID, parent_object_id, parent_column_id, 0, 5, 0, 'CK' FROM #check_constraints WHERE parent_column_id <> 0;

DECLARE @Label_List TABLE (
	Database_ID INT NOT NULL,
	Table_ID INT NOT NULL,
	Column_ID INT NOT NULL,
	PRIMARY KEY (Database_ID, Table_ID, Column_ID),
	Label_List VARCHAR(1000)
);

-- make column label lists by concatenating index column labels
INSERT @Label_List
SELECT 
	Database_ID,
	Table_ID,
	Column_ID,
	STRING_AGG([Label], ',') WITHIN GROUP (ORDER BY is_included_column, Category, Label_Number)
FROM #Column_Label c
GROUP BY Database_ID, Table_ID, column_id;

SELECT --TOP 10000
	DB_NAME(c.Database_ID) AS [DB_Name],
	OBJECT_SCHEMA_NAME(c.[object_id], c.Database_ID) + '.' + OBJECT_NAME(c.[object_id], c.Database_ID) AS Table_Name,
	c.Column_ID,
	c.[name],
	CONCAT(
		TYPE_NAME(c.user_type_id), 
		'(' +
		CASE
			WHEN TYPE_NAME(c.user_type_id) IN ('varchar', 'nvarchar', 'char', 'nchar', 'varbinary') THEN ISNULL(CAST(NULLIF(c.max_length, -1) AS VARCHAR), 'MAX')
			WHEN TYPE_NAME(c.user_type_id) IN ('decimal', 'numeric') THEN CONCAT(c.[precision], ',', c.scale)
			WHEN TYPE_NAME(c.user_type_id) IN ('datetime2', 'time') THEN cast(NULLIF(c.scale, 7) as varchar)
			WHEN TYPE_NAME(c.user_type_id) IN ('float') THEN CONCAT(c.[precision], '') -- else null
		END + ')'
	) AS Data_Type,
	IIF(c.is_nullable = 1, 'NULL', 'NOT NULL') AS Nullability,
	CONCAT_WS(
		',',
		ll.Label_List, 
		IIF(c.is_identity = 0, NULL, 'I'),
		IIF(c.is_computed = 0, NULL, 'comp'),
		IIF(c.default_object_id = 0, NULL, 'DF')
	) AS Features,
	ISNULL(x.[value], '') AS [Description]
FROM #tables t
JOIN #columns c ON t.Database_ID = c.Database_ID AND t.[object_id] = c.[object_id]
LEFT JOIN @Label_List ll ON ll.Database_ID = c.Database_ID AND ll.Table_ID = c.[object_id] AND ll.Column_ID = c.column_id
LEFT JOIN #extended_properties x ON ll.Database_ID = x.Database_ID AND ll.Table_ID = x.major_id AND ll.Column_ID = x.minor_id AND x.[name] = 'MS_Description'
WHERE OBJECT_SCHEMA_NAME(c.[object_id], c.Database_ID) NOT IN ('sys')
	AND DB_NAME(c.Database_ID) NOT LIKE '%snapshot%'
	AND DB_NAME(c.Database_ID) NOT IN ('tempdb', 'msdb', 'SSISDB', 'TBD')
	AND c.[name] LIKE '%' + @ColumnName + '%'
ORDER BY 1, 2, 3;

	--LEFT JOIN (VALUES
	--	('NDC', 1, 'National Drug Code'),
	--	('NPI', 1, 'National Provider ID'),
	--	('MDQ', 0, 'Metric Decimal Quantity'),
	--	('GPI', 0, 'Generic Product ID'),
	--	('NDCID', 0, 'NDC as bigint and without dashes'),
	--	('WAC', 1, 'Wholesale Acquisition Cost'),
	--	('PCN', 1, 'Processor Control Number'),
	--	('BIN', 1, 'Bank Identification Number')
	--) cmt (Column_Name, SuffixFlag, Comment)
	--	ON col.Column_Name LIKE IIF(cmt.SuffixFlag = 0, '', '%') + cmt.Column_Name

PRINT FORMAT(SYSDATETIME(), 'HH:mm:ss.fff');


