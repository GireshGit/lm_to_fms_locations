-- Verbatim from 9_pl_LM_To_FMS_Locations LookupLocationChanges (result set 2); @params -> camel-sql :#params.
WITH changed AS (SELECT TOP (:#maxLocations) l.LocationId
FROM dbo.Location AS l WITH (NOLOCK)
WHERE COALESCE(l.DateModified, l.DateCreated) <= CONVERT(datetime, :#endTs, 121) AND (COALESCE(l.DateModified, l.DateCreated) > CONVERT(datetime, :#beginTs, 121) OR (:#beginId >= 0 AND COALESCE(l.DateModified, l.DateCreated) = CONVERT(datetime, :#beginTs, 121) AND l.LocationId > :#beginId))
ORDER BY COALESCE(l.DateModified, l.DateCreated), l.LocationId)
SELECT CAST(ltrm.LocationId AS VARCHAR(20)) AS LocationId, CASE WHEN rt.RequirementTypeName = 'PickUp' THEN 'O' WHEN rt.RequirementTypeName = 'Delivery' THEN 'D' ELSE 'X' END AS ReqType, LEFT(RTRIM(LTRIM(r.Description)) + ':' + RTRIM(LTRIM(ltrm.Value)) + SPACE(100), 100) AS ReqText, CAST(ltrm.LocationRequirementId AS VARCHAR(20)) AS ReqSeq
FROM dbo.LocationToRequirementsMapping AS ltrm WITH (NOLOCK)
INNER JOIN dbo.Requirement AS r WITH (NOLOCK) ON r.LocationRequirementId = ltrm.LocationRequirementId
INNER JOIN dbo.RequirementType AS rt WITH (NOLOCK) ON rt.RequirementTypeId = ltrm.RequirementTypeId
INNER JOIN changed AS ch ON ch.LocationId = ltrm.LocationId
UNION ALL SELECT '-1', SPACE(0), SPACE(0), SPACE(0) ORDER BY 1, 4
