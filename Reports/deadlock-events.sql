declare @event table (event_id int primary key, event_time datetime, victim_xml xml, process_xml xml, resource_xml xml);

with event1 as (
	select ROW_NUMBER() over (order by len(event_data) desc) as event_id, cast(event_data as xml) as event_xml
	from sys.fn_xe_file_target_read_file('system_health*.xel', NULL, NULL, NULL)
	where object_name = 'xml_deadlock_report'
), event2 as (
	select event1.*, t.c.value('@timestamp', 'datetime') as event_time
	from event1
	cross apply event1.event_xml.nodes('event') t(c)
)
insert @event
select event2.event_id, event2.event_time, t.c.query('victim-list'), t.c.query('process-list'), t.c.query('resource-list')
from event2
cross apply event2.event_xml.nodes('event/data/value/deadlock') t(c);

select * from @event order by event_time;

select evnt.event_id, t.c.value('(@id)[1]', 'varchar(20)') as victim_process_id
from @event evnt
cross apply evnt.victim_xml.nodes('victim-list/victimProcess') t(c)
order by 1;

select 
	evnt.event_id, 
	evnt.event_time, 
	t.c.value('(@id)[1]', 'varchar(20)') as process_id, 
	t.c.value('(@transactionname)[1]', 'varchar(max)') as transactionname, 
	t.c.value('(@lockMode)[1]', 'varchar(max)') as lockMode, 
	t.c.value('(@clientapp)[1]', 'varchar(max)') as clientapp, 
	t.c.value('(@hostname)[1]', 'varchar(max)') as hostname, 
	t.c.value('(@loginname)[1]', 'varchar(max)') as loginname, 
	t.c.query('executionStack/frame') as executionStack, 
	t.c.value('(inputbuf)[1]', 'varchar(max)') as inputbuf
from @event evnt
cross apply evnt.process_xml.nodes('process-list/process') t(c)
order by 1

select evnt.event_id, evnt.event_time, t.c.query('.')
from @event evnt
cross apply evnt.resource_xml.nodes('resource-list/*') t(c)
order by 1

