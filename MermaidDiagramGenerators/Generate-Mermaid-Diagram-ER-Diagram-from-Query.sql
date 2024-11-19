-- ================================================
-- This script returns Mermaid code that displays an ER diagram for the query.
-- https://mermaid-js.github.io/mermaid/#/flowchart -- documentation
-- https://mermaid-js.github.io/mermaid-live-editor -- for testing
-- ================================================

declare @Query_Text VARCHAR(MAX) = '
	SELECT	
			(SELECT A.AgreementAttributeTypeID
					FROM WP340B.AgreementAttributeType AS A
					WHERE A.AgreementAttributeTypeDescription = ''Macro Helix Export Replenishment Model'') AS AgreementAttributeTypeID,
			I.MacroHelixInvoicedRxID AS ReferenceID,
			COALESCE(NULLIF(RTRIM(LTRIM(I.TransactionNumber)), ''), T.TransactionNumber) AS ReferenceKey,
			C.ClaimID,
			T.ReversalFlag
	FROM	WP340B.MacroHelixInvoicedRx AS I
			JOIN WP340B.MacroHelixInvoicedRxFile AS F
				ON F.MacroHelixInvoicedRxFileID = I.MacroHelixInvoicedRxFileID
			JOIN WP340B.RxTranInventory AS RTI
				ON RTI.RxTranInventoryID = I.RxTranInventoryID
			JOIN WP340B.RxTran AS T
				ON T.RxTranID = RTI.RxTranID
			JOIN dbo.Claim AS C
				ON C.PharmacyID = T.PharmacyID
				AND C.Rx = T.Rx
				AND C.FillNumber = T.FillNumber
			LEFT OUTER JOIN WP340B.ExportReplenishmentModel AS ERM
				ON ERM.ReferenceID = I.MacroHelixInvoicedRxID
				AND ERM.AgreementAttributeTypeID = (SELECT A.AgreementAttributeTypeID
						FROM WP340B.AgreementAttributeType AS A
						WHERE A.AgreementAttributeTypeDescription = ''Macro Helix Export Replenishment Model'')
';

DECLARE @Pattern VARCHAR(MAX) = '[^_@#$a-z0-9]';

DECLARE @Table TABLE (TableNumber INT IDENTITY, TableID INT, TableName sysname);

WITH t AS (
	SELECT [object_id] AS TableID, OBJECT_SCHEMA_NAME(object_id) + '.' + OBJECT_NAME(object_id) AS [FileName]
	FROM sys.tables 
)
INSERT @Table
SELECT TableID, '[' + [FileName] + ']'
FROM t
WHERE @Query_Text LIKE '%' + @Pattern + [FileName] + @Pattern + '%';

select 'graph BT' as [%% code]
union all
SELECT DISTINCT CONCAT('T', p.TableNumber, p.TableName, '-->', 'T', r.TableNumber, r.TableName)
FROM sys.foreign_keys fk
JOIN @Table p ON fk.parent_object_id = p.TableID
JOIN @Table r ON fk.referenced_object_id = r.TableID
WHERE fk.parent_object_id <> fk.referenced_object_id;

