USE smart_logistics_dwh;
GO

-- 1. جدول تتبع حركة الشاحنات والـ GPS الأساسي (Gold / Serving Layer)
IF OBJECT_ID('dbo.FactDeliveryTracking', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.FactDeliveryTracking (
        tracking_id BIGINT IDENTITY(1,1) PRIMARY KEY,
        event_id VARCHAR(64) NOT NULL,
        event_version INT NOT NULL,
        event_timestamp DATETIME2 NOT NULL,
        driver_id VARCHAR(32) NOT NULL,
        vehicle_id VARCHAR(32) NOT NULL,
        route_id VARCHAR(32) NOT NULL,
        hub_id VARCHAR(32) NOT NULL,
        latitude DECIMAL(9, 6) NOT NULL,
        longitude DECIMAL(9, 6) NOT NULL,
        speed_kmh DECIMAL(5, 2) NOT NULL,
        delivery_status VARCHAR(32) NOT NULL,
        ingested_at DATETIME2 DEFAULT SYSUTCDATETIME()
    );

    CREATE NONCLUSTERED INDEX IX_FactDeliveryTracking_Timestamp 
    ON dbo.FactDeliveryTracking (event_timestamp DESC);

    CREATE NONCLUSTERED INDEX IX_FactDeliveryTracking_Vehicle 
    ON dbo.FactDeliveryTracking (vehicle_id, event_timestamp DESC);
END;
GO

-- 2. جدول عزل البيانات التالفة (Streaming Quarantine Table)
IF OBJECT_ID('dbo.Quarantine_Tracking', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.Quarantine_Tracking (
        quarantine_id BIGINT IDENTITY(1,1) PRIMARY KEY,
        raw_payload NVARCHAR(MAX) NULL,
        rejection_reason VARCHAR(255) NOT NULL,
        schema_version INT NULL,
        rejected_at DATETIME2 DEFAULT SYSUTCDATETIME()
    );
END;
GO

-- 3. جدول ملخص حالة المركبات اللحظية (لتحسين أداء خريطة Grafana)
IF OBJECT_ID('dbo.FleetLatestStatus', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.FleetLatestStatus (
        vehicle_id VARCHAR(32) PRIMARY KEY,
        driver_id VARCHAR(32) NOT NULL,
        route_id VARCHAR(32) NOT NULL,
        hub_id VARCHAR(32) NOT NULL,
        latitude DECIMAL(9, 6) NOT NULL,
        longitude DECIMAL(9, 6) NOT NULL,
        speed_kmh DECIMAL(5, 2) NOT NULL,
        delivery_status VARCHAR(32) NOT NULL,
        last_updated DATETIME2 NOT NULL
    );
END;
GO