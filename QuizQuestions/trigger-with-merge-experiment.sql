USE [TBD];
GO

DROP TABLE IF EXISTS dbo.Aaa;
GO

CREATE TABLE dbo.Aaa (X INT PRIMARY KEY, Y INT);
GO

INSERT dbo.Aaa VALUES (0, 1), (2, 1), (4, 1), (6, 1), (8, 1);
GO

CREATE OR ALTER TRIGGER dbo.tiAaa ON dbo.Aaa FOR INSERT AS; SELECT 'i' AS i, * FROM Inserted;
GO

CREATE OR ALTER TRIGGER dbo.tuAaa ON dbo.Aaa FOR UPDATE AS;
SELECT 'u' AS u, * FROM deleted AS d FULL OUTER JOIN Inserted AS i ON d.X = i.X;
GO

CREATE OR ALTER TRIGGER dbo.tiudAaa ON dbo.Aaa FOR INSERT, UPDATE, DELETE AS; 
SELECT 'iud' AS iud, * FROM deleted AS d FULL OUTER JOIN Inserted AS i ON d.X = i.X;
GO

CREATE OR ALTER TRIGGER dbo.tdAaa ON dbo.Aaa FOR DELETE AS; SELECT 'd' AS d, * FROM Deleted;
GO

DECLARE @o TABLE (a CHAR(6), Xdel INT, Ydel INT, Xins INT, Yins INT);

MERGE dbo.Aaa AS targt
USING (VALUES (0, 2), (3, 2), (6, 2), (9, 2)) AS src (X, Y)
ON targt.X = src.X
WHEN MATCHED THEN UPDATE SET Y = src.Y
WHEN NOT MATCHED BY TARGET THEN INSERT VALUES (src.X, src.Y)
WHEN NOT MATCHED BY SOURCE THEN DELETE
output $action, deleted.*, inserted.* INTO @o;

SELECT * FROM @o;







