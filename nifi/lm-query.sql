SET NOCOUNT ON;
-- p9 LookupLocationChanges folded into one record set for NiFi: requirements ride along
-- as a FOR JSON column, so one ExecuteSQLRecord feeds one FlowFile. Params bind via sql.args.N.
DECLARE @beginTs VARCHAR(23) = ?, @endTs VARCHAR(23) = ?, @beginId INT = ?, @maxLocations INT = ?;
WITH changed AS (
  SELECT TOP (@maxLocations)
    CAST(l.LocationId AS VARCHAR(20)) AS LocationId, l.LocationId AS LocationIdNum, l.LocationName,
    l.StreetAddress1, l.StreetAddress2, l.City, l.State, l.Country, l.PostalCode,
    C.Phone, C.ContactName, C.Fax, CAST(l.Status AS VARCHAR(10)) AS Status,
    lc.LocationCategoryName, lc.Description AS LocationCategoryDesc, CAST(l.ParentId AS VARCHAR(20)) AS ParentId,
    CASE WHEN CHARINDEX('-', lmDropSiteKey.Value) > 0 THEN LEFT(lmDropSiteKey.Value, CHARINDEX('-', lmDropSiteKey.Value) - 1) ELSE lmDropSiteKey.Value END AS FacilityDropsite,
    CASE WHEN CHARINDEX('-', lmLocaleKey.Value) > 0 THEN LEFT(lmLocaleKey.Value, CHARINDEX('-', lmLocaleKey.Value) - 1) ELSE lmLocaleKey.Value END AS FacilityLocaleKey,
    CONVERT(VARCHAR(23), COALESCE(l.DateModified, l.DateCreated), 121) AS ChangedTs
  FROM dbo.Location AS l WITH (NOLOCK)
  INNER JOIN dbo.LocationCategory AS lc WITH (NOLOCK) ON lc.LocationCategoryId = l.LocationCategoryId
  OUTER APPLY (SELECT TOP (1) c.Phone, c.ContactName, c.Fax FROM dbo.Contact AS c WITH (NOLOCK)
               WHERE c.LocationId = l.LocationId AND c.IsPrimaryContact = 1 ORDER BY c.DateModified) AS C
  OUTER APPLY (SELECT TOP (1) m.Value FROM dbo.LocationtoIDMapping AS m WITH (NOLOCK)
               WHERE m.LocationId = l.LocationId AND m.AlternateID = 1 ORDER BY m.DateModified DESC) AS lmLocaleKey
  OUTER APPLY (SELECT TOP (1) m.Value FROM dbo.LocationtoIDMapping AS m WITH (NOLOCK)
               WHERE m.LocationId = l.LocationId AND m.AlternateID = 2 ORDER BY m.DateModified DESC) AS lmDropSiteKey
  WHERE COALESCE(l.DateModified, l.DateCreated) <= CONVERT(datetime, @endTs, 121)
    AND (COALESCE(l.DateModified, l.DateCreated) > CONVERT(datetime, @beginTs, 121)
         OR (@beginId >= 0 AND COALESCE(l.DateModified, l.DateCreated) = CONVERT(datetime, @beginTs, 121) AND l.LocationId > @beginId))
  ORDER BY COALESCE(l.DateModified, l.DateCreated), l.LocationId)
SELECT ch.LocationId, ch.LocationName, ch.StreetAddress1, ch.StreetAddress2, ch.City, ch.State, ch.Country, ch.PostalCode,
       ch.Phone, ch.ContactName, ch.Fax, ch.Status, ch.LocationCategoryName, ch.LocationCategoryDesc, ch.ParentId,
       ch.FacilityDropsite, ch.FacilityLocaleKey, ch.ChangedTs,
       (SELECT CAST(ltrm.LocationId AS VARCHAR(20)) AS LocationId,
               CASE WHEN rt.RequirementTypeName = 'PickUp' THEN 'O' WHEN rt.RequirementTypeName = 'Delivery' THEN 'D' ELSE 'X' END AS ReqType,
               LEFT(RTRIM(LTRIM(r.Description)) + ':' + RTRIM(LTRIM(ltrm.Value)) + SPACE(100), 100) AS ReqText,
               CAST(ltrm.LocationRequirementId AS VARCHAR(20)) AS ReqSeq
          FROM dbo.LocationToRequirementsMapping AS ltrm WITH (NOLOCK)
          INNER JOIN dbo.Requirement AS r WITH (NOLOCK) ON r.LocationRequirementId = ltrm.LocationRequirementId
          INNER JOIN dbo.RequirementType AS rt WITH (NOLOCK) ON rt.RequirementTypeId = ltrm.RequirementTypeId
         WHERE ltrm.LocationId = ch.LocationIdNum
         ORDER BY CAST(ltrm.LocationRequirementId AS VARCHAR(20))
           FOR JSON PATH) AS RequirementsJson
  FROM changed AS ch
UNION ALL
SELECT '-1', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', CONVERT(VARCHAR(23), SYSDATETIME(), 121), NULL;
