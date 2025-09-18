IF NOT EXISTS (
    SELECT 1 
    FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_SCHEMA = 'stg' 
      AND TABLE_NAME = 'RelatedStations'
)
BEGIN
    CREATE TABLE [stg].[RelatedStations] (
    [id] INT NULL,
    [relatedStationsId] INT NULL,
    [relatedStationsAccessCode] NVARCHAR(255) NULL,
    [relatedStationsFuelTypeCode] NVARCHAR(255) NULL,
    [InsertedAt] DATETIME NOT NULL DEFAULT GETDATE()
    );
END;