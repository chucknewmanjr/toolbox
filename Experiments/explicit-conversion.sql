IF OBJECT_ID('#t') IS NULL BEGIN;
	DROP TABLE IF EXISTS #t;

	-- These can all be implicitly converted with eachother.
	-- But the implicit conversion is always to a higher precedence.
	CREATE TABLE #t (
		BigIntID BIGINT CONSTRAINT PK_t PRIMARY KEY, -- precedence 15 "highest"
		IntID      INT          INDEX IX_IntID,      -- precedence 16
		NVarCharID NVARCHAR(11) INDEX IX_NVarCharID, -- precedence 25
		NCharID    NCHAR(11)    INDEX IX_NCharID,    -- precedence 26
		VarCharID  VARCHAR(11)  INDEX IX_VarCharID,  -- precedence 27
		CharID     CHAR(11)     INDEX IX_CharID,     -- precedence 28
	);

	WITH t AS (SELECT 1 AS x UNION ALL SELECT x + 1 FROM t WHERE x < 5e5) -- 5e5 is half a million rows
	INSERT #t (BigIntID)
	SELECT x
	FROM t
	OPTION (MAXRECURSION 0);

	UPDATE #t
	SET IntID = BigIntID,
		NVarCharID = BigIntID,
		NCharID = BigIntID,
		VarCharID = BigIntID,
		CharID = BigIntID;

	ALTER INDEX ALL ON #t REBUILD;

	SELECT i.type_desc, i.index_id, i.name, s.index_depth, s.page_count * 8 / 1024 AS leaf_level_mb
	FROM tempdb.sys.dm_db_index_physical_stats(DB_ID('tempdb'), OBJECT_ID('tempdb.dbo.#t'), NULL, NULL, 'detailed') s
	JOIN tempdb.sys.indexes i ON s.object_id = i.object_id AND s.index_id = i.index_id
	WHERE s.index_level = 0
	ORDER BY 1, 2 DESC;
END;

-- The execution plan has a scan caused by an implicit conversion.
SELECT t2.BigIntID FROM #t t1 JOIN #t t2 ON t1.IntID = t2.CharID WHERE t1.BigIntID = 100

-- So here, I converted one side of the join. I chose poorly. It still has a scan.
SELECT t2.BigIntID FROM #t t1 JOIN #t t2 ON t1.IntID = CAST(t2.CharID AS INT) WHERE t1.BigIntID = 100

-- This explicit conversion prevents the scan.
SELECT t2.BigIntID FROM #t t1 JOIN #t t2 ON CAST(t1.IntID AS CHAR(11)) = t2.CharID WHERE t1.BigIntID = 100

-- This don't have a scan. And there's no sign of a conversion.
SELECT t2.BigIntID FROM #t t1 JOIN #t t2 ON t1.CharID = t2.VarCharID WHERE t1.BigIntID = 100

-- Here, I've added an explicit conversion that makes things worse.
-- The plan even tells you that explicit conversion is causing a scan in a warning on the final step.
SELECT t2.BigIntID FROM #t t1 JOIN #t t2 ON t1.CharID = CAST(t2.VarCharID AS CHAR(11)) WHERE t1.BigIntID = 100

-- ============================================================================
-- SQL seeks on the smaller resultset
SELECT COUNT(*) FROM #t t1 JOIN #t t2 ON t1.CharID = t2.VarCharID WHERE t1.BigIntID < 5000 AND t2.BigIntID < 4700
SELECT COUNT(*) FROM #t t1 JOIN #t t2 ON t1.CharID = t2.VarCharID WHERE t1.BigIntID < 4700 AND t2.BigIntID < 5000

-- It might also seek both 
SELECT COUNT(*) FROM #t t1 JOIN #t t2 ON t1.CharID = t2.VarCharID WHERE t1.BigIntID < 200




