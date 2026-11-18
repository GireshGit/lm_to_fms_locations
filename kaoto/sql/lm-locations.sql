-- Verbatim from 9_pl_LM_To_FMS_Locations LookupLocationChanges (result set 1); @params -> camel-sql :#params.
WITH changed AS (SELECT TOP (:#maxLocations) CAST(l.LocationId AS VARCHAR(20)) AS LocationId, l.LocationName, l.StreetAddress1, l.StreetAddress2, l.City, l.State, l.Country, l.PostalCode, C.Phone, C.ContactName, C.Fax, CAST(l.Status AS VARCHAR(10)) AS Status, lc.LocationCategoryName, lc.Description AS LocationCategoryDesc, CAST(l.ParentId AS VARCHAR(20)) AS ParentId, CASE WHEN CHARINDEX('-', lmDropSiteKey.Value) > 0 THEN LEFT(lmDropSiteKey.Value, CHARINDEX('-', lmDropSiteKey.Value) - 1) ELSE lmDropSiteKey.Value END AS FacilityDropsite, CASE WHEN CHARINDEX('-', lmLocaleKey.Value) > 0 THEN LEFT(lmLocaleKey.Value, CHARINDEX('-', lmLocaleKey.Value) - 1) ELSE lmLocaleKey.Value END AS FacilityLocaleKey, CONVERT(VARCHAR(23), COALESCE(l.DateModified, l.DateCreated), 121) AS ChangedTs
FROM dbo.Location AS l WITH (NOLOCK)
INNER JOIN dbo.LocationCategory AS lc WITH (NOLOCK) ON lc.LocationCategoryId = l.LocationCategoryId
OUTER APPLY (SELECT TOP (1) c.Phone, c.ContactName, c.Fax
FROM dbo.Contact AS c WITH (NOLOCK)
WHERE c.LocationId = l.LocationId AND c.IsPrimaryContact = 1 ORDER BY c.DateModified) AS C
OUTER APPLY (SELECT TOP (1) m.Value
FROM dbo.LocationtoIDMapping AS m WITH (NOLOCK)
WHERE m.LocationId = l.LocationId AND m.AlternateID = 1 ORDER BY m.DateModified DESC) AS lmLocaleKey
OUTER APPLY (SELECT TOP (1) m.Value
FROM dbo.LocationtoIDMapping AS m WITH (NOLOCK)
WHERE m.LocationId = l.LocationId AND m.AlternateID = 2 ORDER BY m.DateModified DESC) AS lmDropSiteKey
WHERE COALESCE(l.DateModified, l.DateCreated) <= CONVERT(datetime, :#endTs, 121) AND (COALESCE(l.DateModified, l.DateCreated) > CONVERT(datetime, :#beginTs, 121) OR (:#beginId >= 0 AND COALESCE(l.DateModified, l.DateCreated) = CONVERT(datetime, :#beginTs, 121) AND l.LocationId > :#beginId))
ORDER BY COALESCE(l.DateModified, l.DateCreated), l.LocationId)
SELECT LocationId, LocationName, StreetAddress1, StreetAddress2, City, State, Country, PostalCode, Phone, ContactName, Fax, Status, LocationCategoryName, LocationCategoryDesc, ParentId, FacilityDropsite, FacilityLocaleKey, ChangedTs
FROM changed
UNION ALL SELECT '-1', SPACE(0), SPACE(0), SPACE(0), SPACE(0), SPACE(0), SPACE(0), SPACE(0), SPACE(0), SPACE(0), SPACE(0), SPACE(0), SPACE(0), SPACE(0), SPACE(0), SPACE(0), SPACE(0), CONVERT(VARCHAR(23), SYSDATETIME(), 121)
