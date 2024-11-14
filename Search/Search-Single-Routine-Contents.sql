-- ============================================================================
DECLARE @RoutineName sysname = '[WP340B].[ExportSetupOracle]';
DECLARE @Saught sysname = 'WP340B.ExportVoucherStaging';
DECLARE @MatchWholeWord BIT = 1;
-- ============================================================================

DECLARE @Definition VARCHAR(MAX) = (SELECT definition FROM sys.sql_modules WHERE object_id = OBJECT_ID(@RoutineName));

SET @Definition = TRANSLATE(@Definition, CHAR(13) + CHAR(10) + CHAR(9), '   ')

declare @Match table (Position int);

DECLARE @Position INT = -1

SET NOCOUNT ON;

while @Position <> 0 begin;
	SET @Position = CHARINDEX(@Saught, @Definition, @Position + LEN(@Saught));

	insert @Match select @Position;
end;

DECLARE @Pattern sysname = REPLACE(REPLACE(@Saught, '[', '[[]'), '_', '[_]');

IF @MatchWholeWord = 1
	SET @Pattern = '[^_@#$a-z0-9]' + @Pattern + '[^_@#$a-z0-9]';

SET @Pattern = '%' + @Pattern + '%';

SET NOCOUNT OFF;

SELECT
	SUBSTRING(@Definition, Position - 100, LEN(@Saught) + 200) as Snip,
	Position as Pos,
	CAST(ROUND(Position * 100.0 / LEN(@Definition), 0) as int) as PCT
from @Match 
WHERE PATINDEX(@Pattern, SUBSTRING(@Definition, Position - 5, LEN(@Saught) + 10)) <> 0
order by Pos;

