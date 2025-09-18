IF NOT EXISTS (
    SELECT 1 
    FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_SCHEMA = 'stg' 
      AND TABLE_NAME = 'Complaints'
)
BEGIN
    CREATE TABLE [stg].[Complaints] (
    [odiNumber] INT NULL,
    [manufacturer] NVARCHAR(255) NULL,
    [type] NVARCHAR(255) NULL,
    [productYear] INT NULL,
    [productMake] NVARCHAR(255) NULL,
    [productModel] NVARCHAR(255) NULL,
    [crash] BIT NULL,
    [fire] BIT NULL,
    [numberOfInjuries] INT NULL,
    [numberOfDeaths] INT NULL,
    [dateOfIncident] DATETIMEOFFSET NULL,
    [dateComplaintFiled] DATETIMEOFFSET NULL,
    [vin] NVARCHAR(255) NULL,
    [components] NVARCHAR(255) NULL,
    [summary] NVARCHAR(MAX) NULL,
    [crash_bool] BIT NULL,
    [fire_bool] BIT NULL,
    [InsertedAt] DATETIME NOT NULL DEFAULT GETDATE()
    );
END;