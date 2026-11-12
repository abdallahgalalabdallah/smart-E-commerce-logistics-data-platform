USE smart_logistics_dwh;
GO
IF OBJECT_ID('dbo.FactDeliveryTracking','U') IS NULL
CREATE TABLE dbo.FactDeliveryTracking(
 tracking_id BIGINT IDENTITY(1,1) PRIMARY KEY,
 event_id VARCHAR(64) NOT NULL, event_version INT NOT NULL,
 event_timestamp DATETIME2(3) NOT NULL, driver_id VARCHAR(32) NOT NULL,
 vehicle_id VARCHAR(32) NOT NULL, route_id VARCHAR(32) NOT NULL,
 hub_id VARCHAR(32) NOT NULL, latitude DECIMAL(9,6) NOT NULL,
 longitude DECIMAL(9,6) NOT NULL, speed_kmh DECIMAL(6,2) NOT NULL,
 delivery_status VARCHAR(32) NOT NULL,
 ingested_at DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME()
);
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id=OBJECT_ID('dbo.FactDeliveryTracking') AND name='UX_FactDeliveryTracking_event_id')
 CREATE UNIQUE INDEX UX_FactDeliveryTracking_event_id ON dbo.FactDeliveryTracking(event_id);
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id=OBJECT_ID('dbo.FactDeliveryTracking') AND name='IX_FactDeliveryTracking_vehicle_time')
 CREATE INDEX IX_FactDeliveryTracking_vehicle_time ON dbo.FactDeliveryTracking(vehicle_id,event_timestamp DESC);
GO
IF OBJECT_ID('dbo.Quarantine_Tracking','U') IS NULL
CREATE TABLE dbo.Quarantine_Tracking(
 quarantine_id BIGINT IDENTITY(1,1) PRIMARY KEY, raw_payload NVARCHAR(MAX) NULL,
 rejection_reason VARCHAR(500) NOT NULL, schema_version INT NULL,
 kafka_timestamp DATETIME2(3) NULL,
 rejected_at DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME()
);
GO
IF OBJECT_ID('dbo.FleetLatestStatus','U') IS NULL
CREATE TABLE dbo.FleetLatestStatus(
 vehicle_id VARCHAR(32) PRIMARY KEY, driver_id VARCHAR(32) NOT NULL,
 route_id VARCHAR(32) NOT NULL, hub_id VARCHAR(32) NOT NULL,
 latitude DECIMAL(9,6) NOT NULL, longitude DECIMAL(9,6) NOT NULL,
 speed_kmh DECIMAL(6,2) NOT NULL, delivery_status VARCHAR(32) NOT NULL,
 event_timestamp DATETIME2(3) NOT NULL,
 last_updated DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME()
);
GO
IF OBJECT_ID('dbo.FleetMetrics_1Min','U') IS NULL
CREATE TABLE dbo.FleetMetrics_1Min(
 run_id VARCHAR(36) NOT NULL, batch_id BIGINT NOT NULL,
 window_start DATETIME2(3) NOT NULL, scope_type VARCHAR(10) NOT NULL,
 scope_id VARCHAR(64) NOT NULL, event_count BIGINT NOT NULL,
 distinct_vehicles INT NOT NULL, avg_speed_kmh DECIMAL(8,2) NULL,
 moving_count BIGINT NOT NULL, idle_count BIGINT NOT NULL, delayed_count BIGINT NOT NULL,
 written_at DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
 CONSTRAINT PK_FleetMetrics_1Min PRIMARY KEY(run_id,batch_id,window_start,scope_type,scope_id)
);
GO
IF OBJECT_ID('dbo.StreamingPipelineMetrics','U') IS NULL
CREATE TABLE dbo.StreamingPipelineMetrics(
 run_id VARCHAR(36) NOT NULL, batch_id BIGINT NOT NULL,
 batch_started_at DATETIME2(3) NOT NULL, batch_finished_at DATETIME2(3) NOT NULL,
 input_records BIGINT NOT NULL, valid_records BIGINT NOT NULL,
 rejected_records BIGINT NOT NULL, processing_duration_ms BIGINT NOT NULL,
 max_event_timestamp DATETIME2(3) NULL,
 written_at DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
 CONSTRAINT PK_StreamingPipelineMetrics PRIMARY KEY(run_id,batch_id)
);
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id=OBJECT_ID('dbo.FleetMetrics_1Min') AND name='IX_FleetMetrics_window')
 CREATE INDEX IX_FleetMetrics_window ON dbo.FleetMetrics_1Min(window_start DESC,scope_type,scope_id);
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id=OBJECT_ID('dbo.Quarantine_Tracking') AND name='IX_Quarantine_rejected')
 CREATE INDEX IX_Quarantine_rejected ON dbo.Quarantine_Tracking(rejected_at DESC);
GO
