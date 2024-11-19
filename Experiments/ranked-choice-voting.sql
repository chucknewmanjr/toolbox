DECLARE @NumberOfCandidates TINYINT = 5;
DECLARE @VoteShareBase REAL = 1.0005; -- Share of votes compared to the previous candidate
DECLARE @NumberOfConstituents INT = 10000;

-- ============================================================================

DROP TABLE IF EXISTS #Candidate;

CREATE TABLE #Candidate (
	CandidateID TINYINT NOT NULL PRIMARY KEY,
	VoteShare REAL NOT NULL
);

WITH Candidate1 AS (
	SELECT 1 AS CandidateID
	UNION ALL
	SELECT CandidateID + 1
	FROM Candidate1
	WHERE CandidateID < @NumberOfCandidates
), Candidate2 AS (
	SELECT 
		CandidateID, 
		POWER(@VoteShareBase, CandidateID) AS VoteShare
	FROM Candidate1
)
INSERT #Candidate
SELECT
	CandidateID,
	VoteShare / (SELECT SUM(VoteShare) FROM Candidate2) AS VoteShare
FROM Candidate2;

PRINT SYSDATETIME();

-- ============================================================================

DROP TABLE IF EXISTS #Constituent;

CREATE TABLE #Constituent (
	ConstituentID INT NOT NULL PRIMARY KEY
);

WITH Constituent1 AS (
	SELECT 1 AS ConstituentID
	UNION ALL
	SELECT ConstituentID + 1
	FROM Constituent1
	WHERE ConstituentID < @NumberOfConstituents
)
INSERT #Constituent
SELECT *
FROM Constituent1
OPTION (MAXRECURSION 0);
GO

PRINT SYSDATETIME(); -- 20 seconds

-- ============================================================================

DROP TABLE IF EXISTS #Vote;

CREATE TABLE #Vote (
	VoteID INT NOT NULL IDENTITY PRIMARY KEY,
	ConstituentID INT NOT NULL,
	CandidateID TINYINT NOT NULL,
	RandomNumber REAL NULL,
	[Rank] TINYINT NULL
);

INSERT #Vote
SELECT 
	co.ConstituentID, 
	ca.CandidateID, 
	CHECKSUM(NEWID()) / 1e9 + 2.15, 
	NULL
FROM #Constituent co
CROSS JOIN #Candidate ca;
GO

PRINT SYSDATETIME(); -- 10 seconds

WITH Src AS (
	SELECT 
		v.VoteID,
		RANK() OVER (PARTITION BY v.ConstituentID ORDER BY v.RandomNumber * ca.VoteShare desc) AS [Rank]
	FROM #Vote v
	JOIN #Candidate ca ON ca.CandidateID = v.CandidateID
)
UPDATE targt
SET [Rank] = src.[Rank]
FROM #Vote targt
JOIN Src ON Src.VoteID = targt.VoteID;
GO

PRINT SYSDATETIME(); -- 20 seconds

-- ============================================================================

DROP TABLE IF EXISTS #Ranking;

CREATE TABLE #Ranking (
	RankingID INT NOT NULL IDENTITY PRIMARY KEY,
	RankedCandidateList VARCHAR(500) NOT NULL,
	VoteCount INT NOT NULL
);

WITH Ranking AS (
	SELECT 
		ConstituentID, 
		STRING_AGG(CandidateID, ',') within GROUP (ORDER BY [Rank]) AS RankedCandidateList
	FROM #Vote
	GROUP BY ConstituentID
)
INSERT #Ranking
SELECT RankedCandidateList, COUNT(*)
FROM Ranking
GROUP BY RankedCandidateList
ORDER BY COUNT(*) DESC;
GO

-- ============================================================================

DROP TABLE IF EXISTS #CondensedVote;

CREATE TABLE #CondensedVote (
	CondensedVoteID INT NOT NULL IDENTITY PRIMARY KEY,
	RankingID INT NOT NULL,
	CandidateID TINYINT NOT NULL,
	[Rank] TINYINT NOT NULL,
	VoteCount INT NOT NULL
);

INSERT #CondensedVote
SELECT 
	r.RankingID, 
	v.[value], 
	ROW_NUMBER() OVER (PARTITION BY r.RankingID ORDER BY r.RankingID), 
	r.VoteCount
FROM #Ranking r
CROSS APPLY STRING_SPLIT(r.RankedCandidateList, ',') v;
GO

-- ============================================================================

DECLARE @TopVoteGetter TINYINT;
DECLARE @TopVoteCount INT;
DECLARE @VoteTotal INT = (SELECT SUM(VoteCount) FROM #CondensedVote WHERE [Rank] = 1);
DECLARE @BottomVoteGetter TINYINT;

BEGIN TRAN;

WHILE 1 = 1 BEGIN;
	SELECT 
		CandidateID, 
		SUM(VoteCount), 
		CAST(ROUND(SUM(VoteCount) * 100.0 / @VoteTotal, 0) AS REAL)
	FROM #CondensedVote
	WHERE [Rank] = 1
	GROUP BY CandidateID
	ORDER BY SUM(VoteCount) DESC;

	SELECT TOP 1
		@TopVoteGetter = CandidateID, 
		@TopVoteCount = SUM(VoteCount)
	FROM #CondensedVote
	WHERE [Rank] = 1
	GROUP BY CandidateID
	ORDER BY SUM(VoteCount) DESC;

	IF @TopVoteCount * 2 > @VoteTotal BEGIN;
		BREAK;
	END;

	SELECT TOP 1
		@BottomVoteGetter = CandidateID
	FROM #CondensedVote
	WHERE [Rank] = 1
	GROUP BY CandidateID
	ORDER BY SUM(VoteCount);

	UPDATE cv1
	SET [Rank] -= 1
	FROM #CondensedVote cv1
	JOIN #CondensedVote cv2 ON cv1.RankingID = cv2.RankingID AND cv1.[Rank] > cv2.[Rank]
	WHERE cv2.CandidateID = @BottomVoteGetter;

	DELETE #CondensedVote 
	WHERE CandidateID = @BottomVoteGetter;
END;

ROLLBACK;


