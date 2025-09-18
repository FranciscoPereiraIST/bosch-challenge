IF NOT EXISTS (
    SELECT 1 
    FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_SCHEMA = 'stg' 
      AND TABLE_NAME = 'SafetyRatings'
)
BEGIN
    CREATE TABLE [stg].[SafetyRatings] (
    [vehicleId] INT NULL,
    [vehicleDescription] NVARCHAR(255) NULL,
    [make] NVARCHAR(255) NULL,
    [model] NVARCHAR(255) NULL,
    [modelYear] INT NULL,
    [complaintsCount] INT NULL,
    [recallsCount] INT NULL,
    [overallRating] FLOAT NULL,
    [overallFrontCrashRating] FLOAT NULL,
    [frontCrashDriversideRating] FLOAT NULL,
    [frontCrashPassengersideRating] FLOAT NULL,
    [frontCrashPicture] NVARCHAR(255) NULL,
    [frontCrashVideo] NVARCHAR(255) NULL,
    [overallSideCrashRating] FLOAT NULL,
    [sideCrashDriversideRating] FLOAT NULL,
    [sideCrashPassengersideRating] FLOAT NULL,
    [sideCrashPicture] NVARCHAR(255) NULL,
    [sideCrashVideo] NVARCHAR(255) NULL,
    [combinedSideBarrierAndPoleRating-Front] FLOAT NULL,
    [combinedSideBarrierAndPoleRating-Rear] FLOAT NULL,
    [sideBarrierRating-Overall] FLOAT NULL,
    [rolloverRating] FLOAT NULL,
    [rolloverRating2] FLOAT NULL,
    [rolloverPossibility] FLOAT NULL,
    [rolloverPossibility2] FLOAT NULL,
    [dynamicTipResult] NVARCHAR(255) NULL,
    [sidePoleCrashRating] FLOAT NULL,
    [sidePolePicture] NVARCHAR(255) NULL,
    [sidePoleVideo] NVARCHAR(255) NULL,
    [nHTSAElectronicStabilityControl] NVARCHAR(255) NULL,
    [nHTSAForwardCollisionWarning] NVARCHAR(255) NULL,
    [nHTSALaneDepartureWarning] NVARCHAR(255) NULL,
    [investigationCount] INT NULL,
    [vehiclePicture] NVARCHAR(255) NULL,
    [InsertedAt] DATETIME NOT NULL DEFAULT GETDATE()
    );
END;