-- ====================================================
DECLARE @Saught sysname = 'WHERE EXISTS (SELECT *';
DECLARE @MatchWholeWord BIT = 1;
-- ====================================================

DECLARE @Pattern sysname = REPLACE(REPLACE(@Saught, '[', '[[]'), '_', '[_]');

IF @MatchWholeWord = 1
	SET @Pattern = '[^_@#$a-z0-9]' + @Pattern + '[^_@#$a-z0-9]';

SET @Pattern = '%' + @Pattern + '%';

declare @Routine table (
	Routine_ID INT IDENTITY PRIMARY KEY, 
	Routine_Name sysname, 
	Contents nvarchar(max), 
	Position int, 
	Modify_Date DATE
);

insert @Routine
SELECT
	OBJECT_SCHEMA_NAME(m.[object_id]) + '.' + o.[name],
	m.[definition],
	1,
	o.modify_date
FROM sys.sql_modules m WITH (NOLOCK)
JOIN sys.objects o WITH (NOLOCK) ON m.[object_id] = o.[object_id]
where m.[definition] like @Pattern;

delete @Routine where Routine_Name like 'dbo.sp[_]%diagram%';

declare @Match table (Routine_ID int, Position int);

SET NOCOUNT ON;

while exists (select * from @Routine where Position <> 0) begin;
	update @Routine set Position = CHARINDEX(@Saught, Contents, Position + LEN(@Saught)) where Position > 0;

	insert @Match select Routine_ID, Position from @Routine where Position > 0;
end;

SET NOCOUNT OFF;

SELECT
	Routine_Name,
	SUBSTRING(t1.Contents, t2.Position - 30, LEN(@Saught) + 60) as Snip,
	CAST((SELECT SUBSTRING(t1.Contents, t2.Position - 300, LEN(@Saught) + 1000) FOR XML PATH('x')) AS XML) as [XML],
	t2.Position as Pos,
	LEN(t1.Contents) as Total,
	CAST(ROUND(t2.Position * 100.0 / LEN(t1.Contents), 0) as int) as PCT,
	Modify_Date as Modified
from @Routine t1 join @Match t2 on t1.Routine_ID = t2.Routine_ID
WHERE PATINDEX(@Pattern, SUBSTRING(t1.Contents, t2.Position - 5, LEN(@Saught) + 10)) <> 0
order by Routine_Name, Pos;

