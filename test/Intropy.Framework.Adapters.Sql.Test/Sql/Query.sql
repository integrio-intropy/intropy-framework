SELECT Id, Name, ModifiedAt
FROM dbo.Campaign
WHERE ModifiedAt > @watermark
ORDER BY ModifiedAt;
