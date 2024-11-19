	select distinct
		DB_NAME(p.[dbid]) as db,
		p.spid,
		p.blocked as blocked_by,
		l.locks,
		p.waittime / 1000 / 60 as wait_mins,
		p.lastwaittype,
		USER_NAME(p.[uid]) as username,
		--p.open_tran,
		(
			SELECT 
				SUM(DATEDIFF(MINUTE, d.database_transaction_begin_time, SYSDATETIME()))
			FROM sys.dm_tran_session_transactions s
			JOIN sys.dm_tran_database_transactions d ON s.transaction_id = d.transaction_id
			WHERE s.is_user_transaction = 1 AND s.session_id = p.spid AND d.database_id = p.[dbid]
		) AS trans_mins,
		p.[status],
		p.hostname,
		p.[program_name],
		p.cmd,
		p.loginame,
		SUBSTRING(
			(select [text] from sys.dm_exec_sql_text(p.[sql_handle])),
			stmt_start / 2 + 1,
			IIF(stmt_end = -1, 4000, (stmt_end - stmt_start) / 2 + 1)
		) as SQL_Statement
	from sys.sysprocesses p
	left join (select req_spid, COUNT(*) as locks from [master].dbo.syslockinfo group by req_spid) l on p.spid = l.req_spid
	where p.blocked <> 0 -- include the blocked session
		or p.spid in (select blocked from sys.sysprocesses) -- include the blocking session
		or (
			p.[dbid] = DB_ID() -- exclude other DBs
			and p.spid <> @@SPID -- exclude this session
			and l.locks is not null -- exclude sessions without locks
			and (
				l.locks > 1 -- include sessions with multiple locks
				or (
					l.locks = 1 -- include sessions with a single lock. But only if ...
					and (
						p.lastwaittype <> 'MISCELLANEOUS' -- but only if it's not the usual
						or p.[status] <> 'sleeping'
						or p.cmd <> 'AWAITING COMMAND'
					)
				)
			)
		);
