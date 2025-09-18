IF NOT EXISTS (
    SELECT 1 
    FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_SCHEMA = 'stg' 
      AND TABLE_NAME = 'MPG_Detail'
)
BEGIN
    CREATE TABLE [stg].[MPG_Detail] (
    [cityPercent] INT NULL,
    [highwayPercent] INT NULL,
    [lastDate] DATETIMEOFFSET NULL,
    [mpg] FLOAT NULL,
    [state] NVARCHAR(255) NULL,
    [vehicleId] INT NULL,
    [InsertedAt] DATETIME NOT NULL DEFAULT GETDATE()
    );
END;