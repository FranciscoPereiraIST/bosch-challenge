IF NOT EXISTS (
    SELECT 1 
    FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_SCHEMA = 'stg' 
      AND TABLE_NAME = 'MPG_Summary'
)
BEGIN
    CREATE TABLE [stg].[MPG_Summary] (
    [avgMpg] FLOAT NULL,
    [cityPercent] INT NULL,
    [highwayPercent] INT NULL,
    [maxMpg] INT NULL,
    [minMpg] INT NULL,
    [recordCount] INT NULL,
    [vehicleId] INT NULL,
    [InsertedAt] DATETIME NOT NULL DEFAULT GETDATE()
    );
END;