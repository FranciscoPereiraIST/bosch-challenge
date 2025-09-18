IF NOT EXISTS (
    SELECT 1 
    FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_SCHEMA = 'stg' 
      AND TABLE_NAME = 'HyPressures'
)
BEGIN
    CREATE TABLE [stg].[HyPressures] (
    [id] INT NULL,
    [hyPressures] INT NULL,
    [InsertedAt] DATETIME NOT NULL DEFAULT GETDATE()
    );
END;