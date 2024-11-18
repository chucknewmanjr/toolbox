-- takes seconds

IF CAST(SERVERPROPERTY('ServerName') AS VARCHAR) NOT LIKE 'SQLDEV%' THROW 50000, 'wrong server', 1;

IF EXISTS (SELECT * FROM sys.databases WHERE DB_NAME(source_database_id) = DB_NAME()) THROW 50000, 'theres already a snapshot', 1;

DECLARE @SnapshotName NVARCHAR(MAX) = DB_NAME() + '_Snapshot_' + REPLACE(SYSTEM_USER, 'WELLPARTNER\', '');

DECLARE @PartsToDiscard INT = 2 -- Discard database folder name and file name.

DECLARE @FolderPath VARCHAR(MAX) = 'R:\SQLData'; -- default -- EXEC xp_cmdshell 'dir "R:\SQLData"';

/*
	WITH Parts AS (
		-- Break down the path.
		SELECT DISTINCT -- Use distinct in case there are many files.
			ROW_NUMBER() OVER (PARTITION BY f.[file_id] ORDER BY f.[file_id]) AS Position, 
			s.[value] AS Part
		FROM sys.database_files f
		CROSS APPLY STRING_SPLIT(f.physical_name, '\') s
		WHERE type_desc = 'ROWS'
	)
	-- Put those parts we want to keep back together.
	SELECT @FolderPath = STRING_AGG(Part, '\') WITHIN GROUP (ORDER BY Position)
	FROM Parts
	WHERE Position <= (SELECT MAX(Position) - @PartsToDiscard FROM Parts);
--*/

DECLARE @sql NVARCHAR(MAX) = (
	SELECT STRING_AGG('(NAME = ' + name + ', FILENAME = ''' + @FolderPath + '\' + REPLACE(name, DB_NAME(), @SnapshotName) + '.ss'')', ', ')
	FROM sys.database_files
	WHERE type_desc = 'ROWS'
);

SET @sql = 'CREATE DATABASE ' + @SnapshotName + ' ON ' + @sql + ' AS SNAPSHOT OF ' + DB_NAME() + ';';

PRINT @sql;

EXEC (@sql);

PRINT @SnapshotName + ' created';
GO


