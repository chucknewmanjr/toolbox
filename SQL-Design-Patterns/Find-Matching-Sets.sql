DECLARE @Permission TABLE (
	UserID INT, 
	StateID CHAR(2), 
	PRIMARY KEY (UserID, StateID)
);

INSERT @Permission VALUES
	(1, 'CA'),
	(1, 'TX'),
	(2, 'CA'),
	(2, 'TX'),
	(3, 'CA'),
	(3, 'TX'),
	(3, 'FL'),
	(4, 'CA'),
	(5, 'FL'),
	(5, 'TX');

DECLARE @User TABLE (
	UserID INT PRIMARY KEY,
	StateCount INT NOT NULL,
	StateList VARCHAR(50) NOT NULL
);

INSERT @User
SELECT UserID, COUNT(*), STRING_AGG(StateID, '') WITHIN GROUP (ORDER BY StateID)
FROM @Permission
GROUP BY UserID;

WITH Dup AS (
	-- Find duplicate StateLists
	SELECT StateList, COUNT(*) AS DupCount
	FROM @User
	GROUP BY StateList
	HAVING COUNT(*) <> 1
)
SELECT Dup.StateList, Dup.DupCount, p.UserID, p.StateID
FROM Dup
JOIN @User u ON Dup.StateList = u.StateList
JOIN @Permission p ON u.UserID = p.UserID;

WITH Matches (LeftUserID, RightUserID, MatchCount) AS (
	-- Count the number of matches between 2 users
	SELECT p1.UserID, p2.UserID, COUNT(*)
	FROM @Permission p1
	JOIN @Permission p2 ON p1.StateID = p2.StateID
	WHERE p1.UserID <> p2.UserID
	GROUP BY p1.UserID, p2.UserID
)
SELECT 
	m.MatchCount,
	m.LeftUserID,
	m.RightUserID,
	p.StateID
FROM Matches m
JOIN @User lft ON m.LeftUserID = lft.UserID AND m.MatchCount = lft.StateCount
JOIN @User rght ON m.RightUserID = rght.UserID AND m.MatchCount = rght.StateCount
JOIN @Permission p ON m.LeftUserID = p.UserID;

WITH Matches (LeftUserID, RightUserID, MatchCount) AS (
	-- Count the number of matches between 2 users
	SELECT p1.UserID, p2.UserID, COUNT(*)
	FROM @Permission p1
	JOIN @Permission p2 ON p1.StateID = p2.StateID
	WHERE p1.UserID < p2.UserID
	GROUP BY p1.UserID, p2.UserID
)
SELECT 
	m.LeftUserID, 
	m.RightUserID, 
	m.MatchCount, 
	CASE
		WHEN lft.StateCount < rght.StateCount THEN 'Left subset of right'
		WHEN lft.StateCount > rght.StateCount THEN 'Right subset of left'
		ELSE 'Match'
	END
FROM Matches m
JOIN @User lft ON m.LeftUserID = lft.UserID
JOIN @User rght ON m.RightUserID = rght.UserID
WHERE (m.MatchCount = lft.StateCount OR m.MatchCount = rght.StateCount)



