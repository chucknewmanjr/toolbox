USE [TBD];
GO

--This screenshot shows a script and an execution plan. (1) The table has 2 unique constraints. 
--One is the clustered index. (2) The two select queries use different indexes to get the USER's permission level. 
--(3) The first one runs twice as fast as (4) the second. 
--(5) That’s because the second one spends half its time doing lookups.

--QUESTION: How can we change the table schema to make the second query just as fast?

DROP TABLE IF EXISTS [dbo].[user];

-- create a clustered table that also has a unique index
CREATE TABLE [dbo].[user] (
	UserID INT IDENTITY CONSTRAINT PK_User PRIMARY KEY,
	BadgeNumber VARCHAR(10) NOT NULL CONSTRAINT UQ_User UNIQUE,
	PermissionLevel TINYINT
);

-- dummy data just for demo
INSERT [dbo].[user] SELECT object_id, object_id % 5 FROM sys.objects WHERE object_id > 100;

-- use the PK
SELECT PermissionLevel FROM [dbo].[user] WHERE UserID = 100;

-- use the unique index
SELECT PermissionLevel FROM [dbo].[user] WHERE BadgeNumber = '1662628966';



