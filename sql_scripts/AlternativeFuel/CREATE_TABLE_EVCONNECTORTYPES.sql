IF NOT EXISTS (
    SELECT 1 
    FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_SCHEMA = 'stg' 
      AND TABLE_NAME = 'EvConnectorTypes'
)
BEGIN
    CREATE TABLE [stg].[EvConnectorTypes] (
    [id] INT NULL,
    [evConnectorTypes] NVARCHAR(255) NULL,
    [InsertedAt] DATETIME NOT NULL DEFAULT GETDATE()
    );
END;