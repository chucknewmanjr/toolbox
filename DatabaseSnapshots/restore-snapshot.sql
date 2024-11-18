-- this takes about 10 minutes

IF XACT_STATE() <> 0 ROLLBACK;
GO

IF CAST(SERVERPROPERTY('ServerName') AS VARCHAR) NOT LIKE 'SQLDEV%' THROW 50000, 'wrong server', 1;

DECLARE @Source sysname = (SELECT DB_NAME(ISNULL(source_database_id, database_id)) FROM sys.databases WHERE database_id = DB_ID());

DECLARE @Snapshot sysname = (SELECT name FROM sys.databases WHERE source_database_id = DB_ID(@Source));

IF @Snapshot IS NULL THROW 50000, 'Current database is neither source nor snapshot.', 1;

USE [master];

DECLARE @Sql NVARCHAR(MAX);

SET @Sql = 'ALTER DATABASE ' + @Source + ' SET SINGLE_USER WITH ROLLBACK IMMEDIATE;';

EXEC (@sql);

SET @Sql = 'ALTER DATABASE ' + @Source + ' SET MULTI_USER;';

EXEC (@sql);

SET @Sql = 'RESTORE DATABASE ' + @Source + ' FROM DATABASE_SNAPSHOT = ''' + @Snapshot + ''';';

PRINT @Sql;

EXEC (@sql);
GO

