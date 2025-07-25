CREATE OR ALTER PROC dbo.p_AsyncExperiment AS WAITFOR DELAY '00:00:10';
GO

CREATE OR ALTER PROC dbo.p_RunAsyncronusly @job_name sysname, @command NVARCHAR(MAX) AS
	/*
		Create the job and run it.
		Control returns to this proc before the job is done.

		EXEC dbo.p_RunAsyncronusly @job_name = 'Async Job 6', @command = 'EXEC dbo.p_AsyncExperiment;';
	*/
	DECLARE @database_name sysname = DB_NAME();

	-- If the job already exists, we'll just try running it.
	IF NOT EXISTS (SELECT * FROM msdb.dbo.sysjobs WHERE [name] = @job_name) BEGIN;
		EXEC msdb.dbo.sp_add_job @job_name=@job_name;

		EXEC msdb.dbo.sp_add_jobstep @job_name=@job_name, @step_name=N'OnlyStep', @command=@command, @database_name=@database_name;

		EXEC msdb.dbo.sp_add_jobserver @job_name=@job_name, @server_name='(local)';
	END;

	-- If the job is already running, this will cause a failure.
	EXEC msdb.dbo.sp_start_job @job_name=@job_name;
GO

CREATE OR ALTER PROC dbo.p_DeleteJobs @JobsCSV VARCHAR(MAX) AS
	/*
		Wait until the job stops running.

		DECLARE @JobsCSV VARCHAR(MAX) = (SELECT STRING_AGG(job_name, ',') FROM @Jobs);

		EXEC dbo.p_DeleteJobs @JobsCSV;
	*/

	DECLARE @Jobs TABLE (job_id INT IDENTITY, job_name sysname);

	---- Convert CSV into a table.
	INSERT @Jobs (job_name) SELECT [value] FROM STRING_SPLIT(@JobsCSV, ',');

	DECLARE @RunningCount INT = 99;

	-- Wait until they're all done.
	WHILE 1 = 1 BEGIN;
		-- Are these jobs running?
		SELECT @RunningCount = COUNT(*)
		FROM @Jobs j 
		JOIN msdb.dbo.sysjobs AS sj ON j.job_name = sj.[name]
		JOIN msdb.dbo.sysjobactivity AS sja ON sja.job_id = sj.job_id
		WHERE sja.run_requested_date IS NOT NULL -- Job started.
			AND sja.stop_execution_date IS NULL; -- Job hasn't stopped yet.

		IF @RunningCount = 0 BREAK;

		WAITFOR DELAY '00:00:10'; -- wait a few seconds.
	END;

	DECLARE @job_id INT = (SELECT MAX(job_id) FROM @Jobs);
	DECLARE @job_name sysname;

	-- Delete all the jobs.
	WHILE @job_id > 0 BEGIN;
		SELECT @job_name = job_name FROM @Jobs WHERE job_id = @job_id;

		EXEC msdb.dbo.sp_delete_job @job_name=@job_name;

		SET @job_id -= 1;
	END;
GO

DECLARE @Jobs TABLE (job_name sysname);

DECLARE @job_id TINYINT = 5; -- We'll make 5 jobs.
DECLARE @job_name sysname;
DECLARE @command NVARCHAR(MAX);

-- Start 
WHILE @job_id > 0 BEGIN;
	SET @job_name = CONCAT('Async Job ', @job_id);
	SET @command = CONCAT('EXEC dbo.p_AsyncExperiment; -- ', @job_id);

	INSERT @Jobs VALUES (@job_name);

	EXEC dbo.p_RunAsyncronusly @job_name=@job_name, @command=@command;

	SET @job_id -= 1;
END;

---- Convert the list to CSV to use as a parameter.
DECLARE @JobsCSV VARCHAR(MAX) = (SELECT STRING_AGG(job_name, ',') FROM @Jobs);

EXEC dbo.p_DeleteJobs @JobsCSV;

