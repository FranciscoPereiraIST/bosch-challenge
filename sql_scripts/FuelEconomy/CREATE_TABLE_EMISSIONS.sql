IF NOT EXISTS (
    SELECT 1 
    FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_SCHEMA = 'stg' 
      AND TABLE_NAME = 'Emissions'
)
BEGIN
    CREATE TABLE [stg].[Emissions] (
    [efid] NVARCHAR(255) NULL,
    [id] INT NULL,
    [salesArea] INT NULL,
    [score] FLOAT NULL,
    [scoreAlt] FLOAT NULL,
    [smartwayScore] INT NULL,
    [standard] NVARCHAR(255) NULL,
    [stdText] NVARCHAR(255) NULL,
    [InsertedAt] DATETIME NOT NULL DEFAULT GETDATE()
    );
END;