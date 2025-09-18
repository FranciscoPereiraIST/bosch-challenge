IF NOT EXISTS (
    SELECT 1 
    FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_SCHEMA = 'stg' 
      AND TABLE_NAME = 'Recalls'
)
BEGIN
    CREATE TABLE [stg].[Recalls] (
    [manufacturer] NVARCHAR(255) NULL,
    [nHTSACampaignNumber] NVARCHAR(255) NULL,
    [parkIt] BIT NULL,
    [parkOutSide] BIT NULL,
    [overTheAirUpdate] BIT NULL,
    [reportReceivedDate] DATETIMEOFFSET NULL,
    [component] NVARCHAR(255) NULL,
    [summary] NVARCHAR(MAX) NULL,
    [consequence] NVARCHAR(255) NULL,
    [remedy] NVARCHAR(MAX) NULL,
    [notes] NVARCHAR(MAX) NULL,
    [modelYear] INT NULL,
    [make] NVARCHAR(255) NULL,
    [model] NVARCHAR(255) NULL,
    [nHTSAActionNumber] NVARCHAR(255) NULL,
    [parkIt_bool] BIT NULL,
    [parkOutSide_bool] BIT NULL,
    [overTheAirUpdate_bool] BIT NULL,
    [InsertedAt] DATETIME NOT NULL DEFAULT GETDATE()
    );
END;