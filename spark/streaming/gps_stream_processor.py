import os
import pyodbc
from dotenv import load_dotenv
from pyspark.sql import SparkSession, Window
import uuid
from pyspark.sql.functions import (
    from_json, col, when, row_number, window, count, countDistinct, avg, sum as _sum, lit
)

RUN_ID = str(uuid.uuid4())

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType
)

load_dotenv(override=True)

# 1. Schema
gps_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_version", IntegerType(), False),
    StructField("event_timestamp", TimestampType(), False),
    StructField("driver_id", StringType(), False),
    StructField("vehicle_id", StringType(), False),
    StructField("route_id", StringType(), False),
    StructField("hub_id", StringType(), False),
    StructField("latitude", DoubleType(), False),
    StructField("longitude", DoubleType(), False),
    StructField("speed_kmh", DoubleType(), False),
    StructField("status", StringType(), False)
])

# 2. إعدادات قاعدة البيانات
DB_HOST = os.getenv("SQL_SERVER_HOST", "localhost")
DB_PORT = os.getenv("SQL_SERVER_PORT", "14333")
DB_NAME = os.getenv("SQL_SERVER_DB", "smart_logistics_dwh")
DB_USER = os.getenv("SQL_SERVER_USER", "sa")
DB_PASS = os.getenv("SQL_SERVER_PASSWORD", "Logistics@2026!")
ODBC_DRIVER = os.getenv("SQL_SERVER_ODBC_DRIVER", "ODBC Driver 18 for SQL Server")

JDBC_URL = f"jdbc:sqlserver://{DB_HOST}:{DB_PORT};databaseName={DB_NAME};encrypt=false;trustServerCertificate=true;"


def get_odbc_connection():
    conn_str = (
        f"DRIVER={{{ODBC_DRIVER}}};"
        f"SERVER={DB_HOST},{DB_PORT};"
        f"DATABASE={DB_NAME};"
        f"UID={DB_USER};"
        f"PWD={{{DB_PASS}}};"
        f"Encrypt=no;TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str, timeout=10)


def upsert_fleet_latest_status(valid_df):
    """تحديث آخر موقع وحالة وسرعة لكل مركبة في dbo.FleetLatestStatus (UPSERT)."""
    w = Window.partitionBy("vehicle_id").orderBy(col("event_timestamp").desc())
    latest_rows = (
        valid_df.withColumn("rn", row_number().over(w))
        .filter(col("rn") == 1)
        .select(
            "vehicle_id", "driver_id", "route_id", "hub_id",
            "latitude", "longitude", "speed_kmh",
            col("status").alias("delivery_status"),
            col("event_timestamp").alias("last_updated")
        )
        .collect()
    )

    if not latest_rows:
        return

    merge_sql = """
    MERGE dbo.FleetLatestStatus AS target
    USING (SELECT ? AS vehicle_id, ? AS driver_id, ? AS route_id, ? AS hub_id,
                  ? AS latitude, ? AS longitude, ? AS speed_kmh,
                  ? AS delivery_status, ? AS last_updated) AS source
    ON (target.vehicle_id = source.vehicle_id)
    WHEN MATCHED AND source.last_updated >= target.last_updated THEN
        UPDATE SET
            driver_id = source.driver_id,
            route_id = source.route_id,
            hub_id = source.hub_id,
            latitude = source.latitude,
            longitude = source.longitude,
            speed_kmh = source.speed_kmh,
            delivery_status = source.delivery_status,
            last_updated = source.last_updated
    WHEN NOT MATCHED THEN
        INSERT (vehicle_id, driver_id, route_id, hub_id, latitude, longitude, speed_kmh, delivery_status, last_updated)
        VALUES (source.vehicle_id, source.driver_id, source.route_id, source.hub_id, source.latitude, source.longitude, source.speed_kmh, source.delivery_status, source.last_updated);
    """

    conn = get_odbc_connection()
    try:
        cursor = conn.cursor()
        for r in latest_rows:
            cursor.execute(
                merge_sql,
                r.vehicle_id, r.driver_id, r.route_id, r.hub_id,
                float(r.latitude), float(r.longitude), float(r.speed_kmh),
                r.delivery_status, r.last_updated
            )
        conn.commit()
    finally:
        conn.close()


def upsert_fleet_metrics_1min(valid_df, batch_id: int):
    """تجميع مؤشرات الأداء كل دقيقة (FLEET / HUB / ROUTE) وتحديث dbo.FleetMetrics_1Min."""
    def agg_by_scope(df, scope_type, group_cols, scope_expr):
        return (
            df.groupBy(window(col("event_timestamp"), "1 minute").alias("w"), *group_cols)
            .agg(
                count("*").alias("event_count"),
                countDistinct("vehicle_id").alias("distinct_vehicles"),
                avg("speed_kmh").alias("avg_speed_kmh"),
                _sum(when(col("status") == "moving", 1).otherwise(0)).alias("moving_count"),
                _sum(when(col("status") == "idle", 1).otherwise(0)).alias("idle_count"),
                _sum(when(col("status") == "delayed", 1).otherwise(0)).alias("delayed_count"),
            )
            .select(
                col("w.start").alias("window_start"),
                lit(scope_type).alias("scope_type"),
                scope_expr.alias("scope_id"),
                "event_count",
                "distinct_vehicles",
                "avg_speed_kmh",
                "moving_count",
                "idle_count",
                "delayed_count"
            )
        )

    fleet_agg = agg_by_scope(valid_df, "FLEET", [], lit("ALL"))
    hub_agg = agg_by_scope(valid_df, "HUB", ["hub_id"], col("hub_id"))
    route_agg = agg_by_scope(valid_df, "ROUTE", ["route_id"], col("route_id"))

    metrics_rows = fleet_agg.unionByName(hub_agg).unionByName(route_agg).collect()
    if not metrics_rows:
        return

    merge_metrics_sql = """
    MERGE dbo.FleetMetrics_1Min AS target
    USING (SELECT ? AS run_id, ? AS batch_id, ? AS window_start, ? AS scope_type, ? AS scope_id,
                  ? AS event_count, ? AS distinct_vehicles, ? AS avg_speed_kmh,
                  ? AS moving_count, ? AS idle_count, ? AS delayed_count) AS source
    ON (target.run_id = source.run_id
        AND target.batch_id = source.batch_id
        AND target.window_start = source.window_start
        AND target.scope_type = source.scope_type
        AND target.scope_id = source.scope_id)
    WHEN MATCHED THEN
        UPDATE SET
            event_count = source.event_count,
            distinct_vehicles = source.distinct_vehicles,
            avg_speed_kmh = source.avg_speed_kmh,
            moving_count = source.moving_count,
            idle_count = source.idle_count,
            delayed_count = source.delayed_count,
            written_at = SYSUTCDATETIME()
    WHEN NOT MATCHED THEN
        INSERT (run_id, batch_id, window_start, scope_type, scope_id, event_count, distinct_vehicles, avg_speed_kmh, moving_count, idle_count, delayed_count, written_at)
        VALUES (source.run_id, source.batch_id, source.window_start, source.scope_type, source.scope_id, source.event_count, source.distinct_vehicles, source.avg_speed_kmh, source.moving_count, source.idle_count, source.delayed_count, SYSUTCDATETIME());
    """

    conn = get_odbc_connection()
    try:
        cursor = conn.cursor()
        for r in metrics_rows:
            cursor.execute(
                merge_metrics_sql,
                RUN_ID, int(batch_id), r.window_start, r.scope_type, r.scope_id,
                int(r.event_count), int(r.distinct_vehicles), float(r.avg_speed_kmh or 0.0),
                int(r.moving_count), int(r.idle_count), int(r.delayed_count)
            )
        conn.commit()
    finally:
        conn.close()


def process_micro_batch(batch_df, batch_id):
    count_total = batch_df.count()
    print(f"\n--- Processing Batch ID: {batch_id} | Total Records: {count_total} ---")
    if count_total == 0:
        return

    try:
        valid_cond = (
            col("latitude").between(-90.0, 90.0) &
            col("longitude").between(-180.0, 180.0) &
            col("speed_kmh").between(0.0, 160.0) &
            col("event_id").isNotNull() &
            col("vehicle_id").isNotNull() &
            col("driver_id").isNotNull() &
            col("status").isin("moving", "idle", "delayed")
        )

        # عزل السجلات السليمة
        valid_df = batch_df.filter(valid_cond).cache()

        # عزل السجلات الخاطئة (Quarantine)
        invalid_df = (
            batch_df.filter(~valid_cond)
            .withColumn(
                "rejection_reason",
                when((col("speed_kmh") < 0) | (col("speed_kmh") > 160), "Speed out of valid threshold (0-160 km/h)")
                .when((col("latitude") < -90) | (col("latitude") > 90) | (col("longitude") < -180) | (col("longitude") > 180), "Out of boundary coordinates")
                .when(~col("status").isin("moving", "idle", "delayed"), "Invalid delivery status")
                .otherwise("Malformed telemetry payload or missing required fields")
            )
            .select(
                col("raw_json").alias("raw_payload"),
                col("rejection_reason"),
                col("event_version").alias("schema_version")
            )
        )

        valid_count = valid_df.count()
        invalid_count = invalid_df.count()
        print(f"Valid: {valid_count} | Invalid (Quarantine): {invalid_count}")

        if valid_count > 0:
            # 1. الحفظ في FactDeliveryTracking
            valid_df.select(
                "event_id", "event_version", "event_timestamp",
                "driver_id", "vehicle_id", "route_id", "hub_id",
                "latitude", "longitude", "speed_kmh",
                col("status").alias("delivery_status")
            ).write \
                .format("jdbc") \
                .option("url", JDBC_URL) \
                .option("dbtable", "dbo.FactDeliveryTracking") \
                .option("user", DB_USER) \
                .option("password", DB_PASS) \
                .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
                .mode("append") \
                .save()
            print("-> Written to dbo.FactDeliveryTracking")

            # 2. تحديث آخر حالة لكل مركبة في FleetLatestStatus
            upsert_fleet_latest_status(valid_df)
            print("-> Upserted into dbo.FleetLatestStatus")

            # 3. تحديث تجميعات الدقيقة في FleetMetrics_1Min
            upsert_fleet_metrics_1min(valid_df, batch_id)
            print("-> Upserted into dbo.FleetMetrics_1Min")

        # 4. الحفظ في Quarantine_Tracking
        if invalid_count > 0:
            invalid_df.write \
                .format("jdbc") \
                .option("url", JDBC_URL) \
                .option("dbtable", "dbo.Quarantine_Tracking") \
                .option("user", DB_USER) \
                .option("password", DB_PASS) \
                .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
                .mode("append") \
                .save()
            print("-> Written to dbo.Quarantine_Tracking")

        valid_df.unpersist()

    except Exception as e:
        print(f"Error writing batch {batch_id} to SQL Server: {e}")


def main():
    kafka_pkg = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.3.0"
    jdbc_pkg = "com.microsoft.sqlserver:mssql-jdbc:12.4.2.jre11"

    print(f"Loading Spark packages: {kafka_pkg}, {jdbc_pkg}")

    spark = SparkSession.builder \
        .appName("LogisticsRealTimeTelemetry") \
        .config("spark.jars.packages", f"{kafka_pkg},{jdbc_pkg}") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    kafka_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", "delivery_tracking") \
        .option("startingOffsets", "earliest") \
        .load()

    parsed_stream = kafka_stream.selectExpr("CAST(value AS STRING) as raw_json") \
        .withColumn("data", from_json(col("raw_json"), gps_schema)) \
        .select("raw_json", "data.*")

    checkpoint_dir = os.path.join(os.getcwd(), "tmp", "spark_checkpoint")

    query = parsed_stream.writeStream \
        .foreachBatch(process_micro_batch) \
        .option("checkpointLocation", checkpoint_dir) \
        .outputMode("append") \
        .start()

    print("Spark Streaming Query started. Listening to Kafka...")
    query.awaitTermination()


if __name__ == "__main__":
    main()