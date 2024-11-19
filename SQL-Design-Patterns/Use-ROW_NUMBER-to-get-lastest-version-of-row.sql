DECLARE @AssignmentHistory TABLE (
	UserID INT NOT NULL, 
	ClientID INT NOT NULL,
	AssignmentDate DATE
);

INSERT @AssignmentHistory VALUES 
	(1, 3, '2000-01-01'),
	(1, 1, '2000-02-01'),
	(1, 2, '2000-03-01'),
	(2, 4, '2000-02-01');

WITH t1 AS (
	SELECT UserID, MAX(AssignmentDate) AS LatestAssignmentDate
	FROM @AssignmentHistory
	GROUP BY UserID
)
SELECT h.*
FROM @AssignmentHistory h
JOIN t1 ON h.UserID = t1.UserID AND h.AssignmentDate = t1.LatestAssignmentDate
ORDER BY UserID;

WITH t1 AS (
	SELECT *, ROW_NUMBER() OVER (PARTITION BY UserID ORDER BY AssignmentDate DESC) AS RowNumber
	FROM @AssignmentHistory
)
SELECT *
FROM t1
WHERE RowNumber = 1
ORDER BY UserID;






