IF NOT EXISTS (
    SELECT 1 
    FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_SCHEMA = 'stg' 
      AND TABLE_NAME = 'HyStandards'
)
BEGIN
    CREATE TABLE [stg].[HyStandards] (
    [id] INT NULL,
    [hyStandards] NVARCHAR(255) NULL,
    [InsertedAt] DATETIME NOT NULL DEFAULT GETDATE()
    );
END;