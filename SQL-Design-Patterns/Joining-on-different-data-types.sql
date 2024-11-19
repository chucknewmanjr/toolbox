USE CPDB;

-- WP340B.SentryEligibilityChangeReversal.ClaimFragmentID INT
-- dbo.ClaimFragment BIGINT Indexed

-- These 2 columns have a different data type. IF you join on them, SQL Server must make them match. 
-- According to data type precedence, SQL Server will convert the INT to BIGINT.
-- https://learn.microsoft.com/en-us/sql/t-sql/data-types/data-type-precedence-transact-sql?view=sql-server-2017

SELECT TOP 1 t1.ClaimFragmentID, t2.ClaimFragmentID
FROM WP340B.SentryEligibilityChangeReversal t1
JOIN dbo.ClaimFragment t2 ON t1.ClaimFragmentID = t2.ClaimFragmentID

-- Maybe it would be better to explicitly convert the data type yourself. Nope.
SELECT TOP 1 t1.ClaimFragmentID, t2.ClaimFragmentID
FROM WP340B.SentryEligibilityChangeReversal t1
JOIN dbo.ClaimFragment t2 ON CAST(t1.ClaimFragmentID AS BIGINT) = t2.ClaimFragmentID

-- Somehow, converting from BIGINT to INT makes it faster. But then you might cause truncation.
SELECT TOP 1 t1.ClaimFragmentID, t2.ClaimFragmentID
FROM WP340B.SentryEligibilityChangeReversal t1
JOIN dbo.ClaimFragment t2 ON t1.ClaimFragmentID = CAST(t2.ClaimFragmentID AS INT)



