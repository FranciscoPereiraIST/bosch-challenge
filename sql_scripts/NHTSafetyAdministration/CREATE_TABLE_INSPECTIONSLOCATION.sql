IF NOT EXISTS (
    SELECT 1 
    FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_SCHEMA = 'stg' 
      AND TABLE_NAME = 'InspectionsLocation'
)
BEGIN
    CREATE TABLE [stg].[InspectionsLocation] (
    [state] NVARCHAR(255) NULL,
    [city] NVARCHAR(255) NULL,
    [zip] NVARCHAR(255) NULL,
    [organization] NVARCHAR(255) NULL,
    [contactFirstName] NVARCHAR(255) NULL,
    [contactLastName] NVARCHAR(255) NULL,
    [addressLine1] NVARCHAR(255) NULL,
    [email] NVARCHAR(255) NULL,
    [fax] NVARCHAR(255) NULL,
    [phone1] NVARCHAR(255) NULL,
    [cPSWeekEventFlag] NVARCHAR(255) NULL,
    [lastUpdatedDate] DATETIMEOFFSET NULL,
    [mobileStationFlag] NVARCHAR(255) NULL,
    [countiesServed] NVARCHAR(255) NULL,
    [locationLatitude] FLOAT NULL,
    [locationLongitude] FLOAT NULL,
    [cPSWeekEventFlag_bool] BIT NULL,
    [mobileStationFlag_bool] BIT NULL,
    [InsertedAt] DATETIME NOT NULL DEFAULT GETDATE()
    );
END;