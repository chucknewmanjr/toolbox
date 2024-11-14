-- ====================================================
DECLARE @Saught sysname = 'SQLPRD1';
DECLARE @MatchWholeWord BIT = 0;
-- ====================================================

DECLARE @Pattern sysname = REPLACE(REPLACE(@Saught, '[', '[[]'), '_', '[_]');

IF @MatchWholeWord = 1
	SET @Pattern = '[^_@#$a-z0-9]' + @Pattern + '[^_@#$a-z0-9]';

SET @Pattern = '%' + @Pattern + '%';

declare @JobStep table (
	JobStep_ID INT IDENTITY PRIMARY KEY,
	Job_Name sysname,
	Step_Number int,
	Step_Name sysname,
	Step_Type sysname,
	Contents nvarchar(max), 
	Position int, 
	Modify_Date DATE
);

insert @JobStep
SELECT
	j.[name],
	s.step_id,
	s.step_name,
	s.subsystem,
	s.command,
	-1,
	j.date_modified
FROM msdb.dbo.sysjobsteps s WITH (NOLOCK)
JOIN msdb.dbo.sysjobs j WITH (NOLOCK) ON s.job_id = j.job_id
where s.command like @Pattern;

update @JobStep set Contents = REPLACE(Contents, CHAR(13) + CHAR(10), '  ') -- Replace CRLF with 2 spaces

update @JobStep set Contents = REPLACE(Contents, CHAR(10), ' ') -- Sometimes there's only CR

update @JobStep set Contents = REPLACE(Contents, CHAR(9), ' ') -- replace tabs

declare @Match table (JobStep_ID int, Position int);

SET NOCOUNT ON;

while exists (select * from @JobStep where Position <> 0) begin;
	update @JobStep set Position = CHARINDEX(@Saught, Contents, Position + LEN(@Saught)) where Position <> 0;

	insert @Match select JobStep_ID, Position from @JobStep where Position <> 0;
end;

SET NOCOUNT OFF;

SELECT
	Job_Name,
	Step_Number,
	Step_Name,
	Step_Type,
	SUBSTRING(t1.Contents, t2.Position - 100, LEN(@Saught) + 200) as Snip,
	t2.Position as Pos,
	LEN(t1.Contents) as Total,
	CAST(ROUND(t2.Position * 100.0 / LEN(t1.Contents), 0) as int) as PCT,
	Modify_Date as Modified
from @JobStep t1 join @Match t2 on t1.JobStep_ID = t2.JobStep_ID
WHERE PATINDEX(@Pattern, SUBSTRING(t1.Contents, t2.Position - 5, LEN(@Saught) + 10)) <> 0
order by Job_Name, Step_Number, Pos;

