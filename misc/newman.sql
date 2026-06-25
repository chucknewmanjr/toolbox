-- ------- --
-- encoder --
-- ------- --
-- len(text) = 40; 3^40    = 12 * 10^18 (Quintillion)
-- bigint 8 bytes 2^(64-1) =  9 * 10^18 Careful!
declare @t table ([Text] varchar(999), Quotient bigint default 0);

insert @t ([Text]) values
('        /\                    /\		  '),
(' /\  / / /  \  /\  / /\  /\  / /\  /\  /'),
('/  \/  \/ /  \/  \/ /  \/  \  / / /  \/ '),
('        \/                    \/		  ')

declare 
	@Max as int = (select max(len(Text)) from @t), 
	@This as int = 1, 
	@x as int

set nocount on

while @This <= @Max begin
	update @t
	set Quotient = Quotient * 3 + 
		case substring([Text], @This, 1)
			when '/' then 1
			when '\' then 2
			else 0
		end

	print @This

	set @This += 1
end
select * from @t
go

-- ------- --
-- decoder --
-- ------- --
with t as(select x,0 i,CAST(''as varchar(MAX))a from(values(1029455660506050),(2270171348526990934),(4407174318369854082),(1441237924708470))t(x)union all select x,i+1,a+SUBSTRING(' /\',FLOOR(x/POWER(3.0,i))%3+1,1)from t where i<40)select'--'+a from t where i=40
-- 263 characters

