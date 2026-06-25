drop table if exists t_Sudoku;
go

create table t_Sudoku (
	[Row] tinyint identity primary key, 
	Col1 tinyint, 
	Col2 tinyint, 
	Col3 tinyint, 
	Col4 tinyint, 
	Col5 tinyint, 
	Col6 tinyint, 
	Col7 tinyint, 
	Col8 tinyint, 
	Col9 tinyint
);
go

drop table if exists t_Cell;
go

create table t_Cell (
	Cell_ID int identity primary key, 
	Sudoku_ID tinyint, 
	[Row] tinyint, 
	Col tinyint, 
	[Block] tinyint, 
	[Entry] tinyint, 
	Candidate_Count tinyint, -- summary
	Candidate_List varchar(17), -- summary
	unique (Sudoku_ID, [Row], Col)
)
go

drop table if exists t_Candidate
go

create table t_Candidate (
	Cell_ID int,
	Sudoku_ID tinyint, 
	[Row] tinyint, 
	Col tinyint, 
	[Block] tinyint, 
	[Entry] tinyint, 
	primary key (Cell_ID, [Entry]),
	unique (Sudoku_ID, [Row], Col, [Entry])
)
go

drop table if exists t_Action
go

create table t_Action (
	Action_ID int identity primary key, 
	[Action] varchar(99), 
	Cell_ID int, 
	Sudoku_ID tinyint, 
	[Row] tinyint, 
	Col tinyint, 
	[Block] tinyint, 
	[Entry] tinyint, 
	Candidate_Count tinyint, 
	Candidate_List varchar(17)
)
go

create or alter proc p_Sudoku_to_Cells @Sudoku_ID tinyint as
	with t1 as (
		select [Row], 1 as Col, Col1 as [Entry] from t_Sudoku
		union select [Row], 2, Col2 from t_Sudoku
		union select [Row], 3, Col3 from t_Sudoku
		union select [Row], 4, Col4 from t_Sudoku
		union select [Row], 5, Col5 from t_Sudoku
		union select [Row], 6, Col6 from t_Sudoku
		union select [Row], 7, Col7 from t_Sudoku
		union select [Row], 8, Col8 from t_Sudoku
		union select [Row], 9, Col9 from t_Sudoku
	)
	insert t_Cell (Sudoku_ID, [Row], Col, [Block], [Entry])
	select 
		@Sudoku_ID, 
		[Row], 
		Col, 
		([Row]-1) / 3 * 3 + (Col-1) / 3 + 1 as [Block], 
		[Entry]
	from t1;

	truncate table t_Sudoku;
go
declare @Sudoku_ID tinyint = 0

insert t_Sudoku (Col1, Col2, Col3, Col4, Col5, Col6, Col7, Col8, Col9)
values 
	(null, null, null, null, null,  8,   null, null, null),
	( 8,   null,  3,   null,  2,   null, null, null,  6),
	( 1,   null, null, null, null,  5,   null,  8,   null),

	(null,  5,   null, null, null, null,  3,   null, null),
	( 7,    3,   null,  1,   null,  4,   null,  9,    5),
	(null, null,  6,   null, null, null, null,  2,   null),

	(null,  2,   null,  3,   null, null, null, null,  7),
	( 3,   null, null, null,  4,   null,  1,   null,  8),
	(null, null, null,  9,   null, null, null, null, null);

set @Sudoku_ID += 1

exec p_Sudoku_to_Cells @Sudoku_ID

insert t_Sudoku (Col1, Col2, Col3, Col4, Col5, Col6, Col7, Col8, Col9)
values 
	(null, null, 8, 3, null, 5, 1, null, null),
	(9, null, null, 4, null, null, null, null, null),
	(4, null, null, null, 8, null, 9, null, null),

	(3, null, 5, 1, null, null, null, 9, null),
	(null, 2, null, null, null, null, null, 4, null),
	(null, 1, null, null, null, 4, 5, null, 6),

	(null, null, 6, null, 3, null, null, null, 2),
	(null, null, null, null, null, 6, null, null, 9),
	(null, null, 3, 2, null, 9, 8, null, null);

set @Sudoku_ID += 1

exec p_Sudoku_to_Cells @Sudoku_ID

insert t_Sudoku (Col1, Col2, Col3, Col4, Col5, Col6, Col7, Col8, Col9)
values 
	(null, null, null, null, 5,    null, null, 1, null),
	(null, null, 1,    null, null, 8,    2,    null, 9),
	(8,    null, null, 4,    null, 1,    5,    null, null),

	(null, 7,    3,    null, null, null, null, null, null),
	(null, null, null, 9,    7,    4,    null, null, null),
	(null, null, null, null, null, null, 6,    4, null),

	(null, null, 6,    3,    null, 7,    null, null, 8),
	(7,    null, 9,    8,    null, null, 3,    null, null),
	(null, 8,    null, null, 2,    null, null, null, null);

set @Sudoku_ID += 1

exec p_Sudoku_to_Cells @Sudoku_ID

insert t_Sudoku (Col1, Col2, Col3, Col4, Col5, Col6, Col7, Col8, Col9)
values 
	(null, null, null, null, null, 6, null, null, null),
	(null, null, null, null, 5, null, 9, null, 1),
	(null, null, 9, 7, null, null, null, 2, null),

	(9, null, null, 6, 2, null, 5, null, 7),
	(null, 2, null, null, 8, null, null, 9, null),
	(3, null, 6, 4, null, null, 2, null, null),

	(null, null, 1, null, null, 8, null, null, 4),
	(4, null, null, null, null, null, null, null, null),
	(null, null, null, 1, null, null, null, 3, null);

set @Sudoku_ID += 1

exec p_Sudoku_to_Cells @Sudoku_ID

--insert t_Sudoku (Col1, Col2, Col3, Col4, Col5, Col6, Col7, Col8, Col9)
--values 
--	(null, null, null, null, null, null, null, null, null),
--	(null, null, null, null, null, null, null, null, null),
--	(null, null, null, null, null, null, null, null, null),

--	(null, null, null, null, null, null, null, null, null),
--	(null, null, null, null, null, null, null, null, null),
--	(null, null, null, null, null, null, null, null, null),

--	(null, null, null, null, null, null, null, null, null),
--	(null, null, null, null, null, null, null, null, null),
--	(null, null, null, null, null, null, null, null, null);

--set @Sudoku_ID += 1

--exec p_Sudoku_to_Cells @Sudoku_ID
go

insert t_Candidate
select Cell_ID, Sudoku_ID, [Row], Col, [Block], t4.[Entry]
from t_Cell
cross join (values (1), (2), (3), (4), (5), (6), (7), (8), (9)) as t4([Entry]);
go

Back_to_Cleaning_Out_Candidates: -- GOTO LABEL
-- These deletes are only helpful if an entry is made.
-- They're not needed if a t_Candidate is eliminated. 
print 'Cleaning_Out_Candidates';

delete t from t_Candidate t join t_Cell s on t.Cell_ID = s.Cell_ID and s.[Entry] is not null;

delete t from t_Candidate t join t_Cell s on t.Sudoku_ID = s.Sudoku_ID and t.[Row] = s.[Row] and t.[Entry] = s.[Entry];

delete t from t_Candidate t join t_Cell s on t.Sudoku_ID = s.Sudoku_ID and t.Col = s.Col and t.[Entry] = s.[Entry];

delete t from t_Candidate t join t_Cell s on t.Sudoku_ID = s.Sudoku_ID and t.[Block] = s.[Block] and t.[Entry] = s.[Entry];

Back_to_Making_Entries: -- GOTO LABEL
-- If a t_Candidate is eliminated, re-check for singles that can be entered.
print 'Making_Entries';

declare @Is_Changed bit = 0;

-- NAKED SINGLE - only 1 t_Candidate left
with t1 as (select Cell_ID, MAX([Entry]) as [Entry] from t_Candidate group by Cell_ID having COUNT(*) = 1)
update c set [Entry] = t1.[Entry]
output 'naked single', inserted.* into t_Action ([Action], Cell_ID, Sudoku_ID, [Row], Col, [Block], [Entry], Candidate_Count, Candidate_List)
from t_Cell c join t1 on c.Cell_ID = t1.Cell_ID;

if @@ROWCOUNT > 0 set @Is_Changed = 1;

if @Is_Changed = 1 goto Back_to_Cleaning_Out_Candidates;

-- HIDDEN SINGLE - only t_Candidate left in a row
with t1 as (select MAX(Cell_ID) as Cell_ID, [Entry] from t_Candidate group by Sudoku_ID, [Row], [Entry] having count(*) = 1)
update c set [Entry] = t1.[Entry]
output 'hidden single - row', inserted.* into t_Action ([Action], Cell_ID, Sudoku_ID, [Row], Col, [Block], [Entry], Candidate_Count, Candidate_List)
from t_Cell c join t1 on c.Cell_ID = t1.Cell_ID;

if @@ROWCOUNT > 0 set @Is_Changed = 1;

-- HIDDEN SINGLE - only t_Candidate left in a column
with t1 as (select MAX(Cell_ID) as Cell_ID, [Entry] from t_Candidate group by Sudoku_ID, Col, [Entry] having count(*) = 1)
update c set [Entry] = t1.[Entry]
output 'hidden single - column', inserted.* into t_Action ([Action], Cell_ID, Sudoku_ID, [Row], Col, [Block], [Entry], Candidate_Count, Candidate_List)
from t_Cell c join t1 on c.Cell_ID = t1.Cell_ID;

if @@ROWCOUNT > 0 set @Is_Changed = 1;

if @Is_Changed = 1 goto Back_to_Cleaning_Out_Candidates;

-- HIDDEN SINGLE - only t_Candidate left in a block
with t1 as (select MAX(Cell_ID) as Cell_ID, [Entry] from t_Candidate group by Sudoku_ID, [Block], [Entry] having count(*) = 1)
update c set [Entry] = t1.[Entry]
output 'hidden single - block', inserted.* into t_Action ([Action], Cell_ID, Sudoku_ID, [Row], Col, [Block], [Entry], Candidate_Count, Candidate_List)
from t_Cell c join t1 on c.Cell_ID = t1.Cell_ID;

if @@ROWCOUNT > 0 set @Is_Changed = 1;

if @Is_Changed = 1 goto Back_to_Cleaning_Out_Candidates;

print 'validation';

-- dup in row - where did it come from?
if exists (select * from t_Cell where [Entry] is not null group by Sudoku_ID, [Row], [Entry] having COUNT(*) > 1) begin
	select * from t_Cell c join (
		select Sudoku_ID, [Row], [Entry] from t_Cell where [Entry] is not null group by Sudoku_ID, [Row], [Entry] having COUNT(*) > 1
	) g on c.Sudoku_ID = g.Sudoku_ID and c.[Row] = g.[Row] and c.[Entry] = g.[Entry]

	select * from t_Candidate c join (
		select Sudoku_ID, [Row], [Entry] from t_Cell where [Entry] is not null group by Sudoku_ID, [Row], [Entry] having COUNT(*) > 1
	) g on c.Sudoku_ID = g.Sudoku_ID and c.[Row] = g.[Row] and c.[Entry] = g.[Entry]
end

if exists (select * from t_Cell where [Entry] is not null group by Sudoku_ID, [Row], [Entry] having COUNT(*) > 1)
	raiserror('dup entry in row', 18, 1)
else if exists (select * from t_Cell where [Entry] is not null group by Sudoku_ID, Col, [Entry] having COUNT(*) > 1)
	raiserror('dup entry in column', 18, 1)
else if exists (select * from t_Cell where [Entry] is not null group by Sudoku_ID, [Block], [Entry] having COUNT(*) > 1)
	raiserror('dup entry in block', 18, 1);

-- ------------------------------
-- No more easy singles to enter.
-- Now, let's eliminate candidates.

-- But first, refresh the summaries
with t1 as (
	select 
		Cell_ID, 
		COUNT(*) as Candidate_Count, 
		STRING_AGG(CAST([Entry] as varchar), ',') within group (order by [Entry]) as Candidate_List 
	from t_Candidate 
	group by Cell_ID
)
update b set Candidate_Count = t1.Candidate_Count, Candidate_List = t1.Candidate_List
from t_Cell b join t1 on b.Cell_ID = t1.Cell_ID;


-- NAKED PAIR - 2 boxes have 2 matching candidates. And they're both in the same house (row, column or block).
-- In that case, all the matching candidates in that house can be removed.
-- What about a naked trio?
print 'naked pair';

with 
t1 as (select Sudoku_ID, [Row], Candidate_List from t_Cell where Candidate_Count = 2 group by Sudoku_ID, [Row], Candidate_List having count(*) = 2), 
t2 as (select Sudoku_ID, [Row], [value] as [Entry] from t1 cross apply string_split(Candidate_List, ',')), 
t3 as (select Cell_ID from t_Cell c join t1 on c.Sudoku_ID = t1.Sudoku_ID and c.[Row] = t1.[Row] and c.Candidate_List = t1.Candidate_List)
delete c
output 'naked pair - row', deleted.* into t_Action ([Action], Cell_ID, Sudoku_ID, [Row], Col, [Block], [Entry])
from t_Candidate c
join t2 on c.Sudoku_ID = t2.Sudoku_ID and c.[Row] = t2.[Row] and c.[Entry] = t2.[Entry] -- Matching candidates in these rows.
left join t3 on c.Cell_ID = t3.Cell_ID -- But not these specific cells.
where t3.Cell_ID is null;

if @@ROWCOUNT > 0 set @Is_Changed = 1;

with 
t1 as (select Sudoku_ID, Col, Candidate_List from t_Cell where Candidate_Count = 2 group by Sudoku_ID, Col, Candidate_List having count(*) = 2), 
t2 as (select Sudoku_ID, Col, [value] as [Entry] from t1 cross apply string_split(Candidate_List, ',')), 
t3 as (select Cell_ID from t_Cell c join t1 on c.Sudoku_ID = t1.Sudoku_ID and c.Col = t1.Col and c.Candidate_List = t1.Candidate_List)
delete c
output 'naked pair - column', deleted.* into t_Action ([Action], Cell_ID, Sudoku_ID, [Row], Col, [Block], [Entry])
from t_Candidate c 
join t2 on c.Sudoku_ID = t2.Sudoku_ID and c.Col = t2.Col and c.[Entry] = t2.[Entry] -- Matching candidates in these columns.
left join t3 on c.Cell_ID = t3.Cell_ID -- But not these specific cells.
where t3.Cell_ID is null;

if @@ROWCOUNT > 0 set @Is_Changed = 1;

if @Is_Changed = 1 goto Back_to_Making_Entries;

with 
t1 as (select Sudoku_ID, [Block], Candidate_List from t_Cell where Candidate_Count = 2 group by Sudoku_ID, [Block], Candidate_List having count(*) = 2), 
t2 as (select Sudoku_ID, [Block], [value] as [Entry] from t1 cross apply string_split(Candidate_List, ',')), 
t3 as (select Cell_ID from t_Cell c join t1 on c.Sudoku_ID = t1.Sudoku_ID and c.[Block] = t1.[Block] and c.Candidate_List = t1.Candidate_List)
delete c
output 'naked pair - block', deleted.* into t_Action ([Action], Cell_ID, Sudoku_ID, [Row], Col, [Block], [Entry])
from t_Candidate c 
join t2 on c.Sudoku_ID = t2.Sudoku_ID and c.[Block] = t2.[Block] and c.[Entry] = t2.[Entry] -- Matching candidates in these columns.
left join t3 on c.Cell_ID = t3.Cell_ID -- But not these specific cells.
where t3.Cell_ID is null;

if @@ROWCOUNT > 0 set @Is_Changed = 1;

if @Is_Changed = 1 goto Back_to_Making_Entries;


print 'pointing';

with 
t1 as (select Sudoku_ID, [Block], [Entry] from t_Candidate group by Sudoku_ID, [Block], [Entry] having count(*) = 2),
t2 as (
	select c.Sudoku_ID, c.[Row], c.[Block], c.[Entry] 
	from t_Candidate c join t1 on c.Sudoku_ID = t1.Sudoku_ID and c.[Block] = t1.[Block] and c.[Entry] = t1.[Entry]
	group by c.Sudoku_ID, c.[Row], c.[Block], c.[Entry] 
	having COUNT(*) = 2
)
delete c
output 'pointing - row', deleted.* into t_Action ([Action], Cell_ID, Sudoku_ID, [Row], Col, [Block], [Entry])
from t_Candidate c
join t2 on c.Sudoku_ID = t2.Sudoku_ID and c.[Row] = t2.[Row] and c.[Entry] = t2.[Entry] and c.[Block] != t2.[Block];

if @@ROWCOUNT > 0 set @Is_Changed = 1;

if @Is_Changed = 1 goto Back_to_Making_Entries;

with 
t1 as (select Sudoku_ID, [Block], [Entry] from t_Candidate group by Sudoku_ID, [Block], [Entry] having count(*) = 2),
t2 as (
	select c.Sudoku_ID, c.Col, c.[Block], c.[Entry] 
	from t_Candidate c join t1 on c.Sudoku_ID = t1.Sudoku_ID and c.[Block] = t1.[Block] and c.[Entry] = t1.[Entry]
	group by c.Sudoku_ID, c.Col, c.[Block], c.[Entry] 
	having COUNT(*) = 2
)
delete c
output 'pointing - column', deleted.* into t_Action ([Action], Cell_ID, Sudoku_ID, [Row], Col, [Block], [Entry])
from t_Candidate c
join t2 on c.Sudoku_ID = t2.Sudoku_ID and c.Col = t2.Col and c.[Entry] = t2.[Entry] and c.[Block] != t2.[Block];

if @@ROWCOUNT > 0 set @Is_Changed = 1;

if @Is_Changed = 1 goto Back_to_Making_Entries;
go


print 'x wing';

with
t1 as (
	select Sudoku_ID, [Row], [Entry], STRING_AGG(CAST(Col as varchar), ',') within group (order by Col) as Column_List 
	from t_Candidate 
	group by Sudoku_ID, [Row], [Entry] 
	having COUNT(*) = 2
),
t2 as (
	select Sudoku_ID, [Entry], Column_List, STRING_AGG(CAST([Row] as varchar), ',') within group (order by [Row]) as Row_List
	from t1 
	group by Sudoku_ID, [Entry], Column_List 
	having COUNT(*) = 2
)
select c.*
from t_Candidate c
join t1 on c.Sudoku_ID = t1.Sudoku_ID and c.[Entry] = t1.[Entry] and c.[Row] = t1.[Row]
join t2 on t1.Sudoku_ID = t2.Sudoku_ID and t1.[Entry] = t2.[Entry] and t1.Column_List = t2.Column_List
order by c.Sudoku_ID, c.[Entry], c.[Row], c.Col

if @@ROWCOUNT > 0 print 'no x wings found';
go

select Sudoku_ID, [Row], 
	max(iif(Col = 1, cast([Entry] as varchar), '')) as c1, 
	max(iif(Col = 2, cast([Entry] as varchar), '')) as c2, 
	max(iif(Col = 3, cast([Entry] as varchar), '')) as c3, 
	max(iif(Col = 4, cast([Entry] as varchar), '')) as c4, 
	max(iif(Col = 5, cast([Entry] as varchar), '')) as c5, 
	max(iif(Col = 6, cast([Entry] as varchar), '')) as c6, 
	max(iif(Col = 7, cast([Entry] as varchar), '')) as c7, 
	max(iif(Col = 8, cast([Entry] as varchar), '')) as c8, 
	max(iif(Col = 9, cast([Entry] as varchar), '')) as c9 
from t_Cell
group by Sudoku_ID, [Row]
order by Sudoku_ID, [Row];
go

select Sudoku_ID, Action_ID, [Action], [Row], Col, [Entry] from t_Action order by 1, 2
go
