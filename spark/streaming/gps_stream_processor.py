import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, TimestampType
)

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
DB_PASS = os.getenv("SQL_SERVER_PASSWORD", "LogisticsPass2026")

JDBC_URL = f"jdbc:sqlserver://{DB_HOST}:{DB_PORT};databaseName={DB_NAME};encrypt=false;trustServerCertificate=true;"

def process_micro_batch(batch_df, batch_id):
    count = batch_df.count()
    print(f"\n--- Processing Batch ID: {batch_id} | Total Records: {count} ---")
    if count == 0:
        return

    try:
        # عزل السجلات السليمة
        valid_df = batch_df.filter(
            (col("latitude").between(-90.0, 90.0)) &
            (col("longitude").between(-180.0, 180.0)) &
            (col("speed_kmh") >= 0.0) &
            (col("speed_kmh") <= 160.0) &
            col("event_id").isNotNull()
        )

        # عزل السجلات الخاطئة (Quarantine)
        invalid_df = batch_df.filter(
            (col("latitude") < -90.0) | (col("latitude") > 90.0) |
            (col("longitude") < -180.0) | (col("longitude") > 180.0) |
            (col("speed_kmh") < 0.0) | (col("speed_kmh") > 160.0) |
            col("event_id").isNull()
        ).withColumn("rejection_reason", 
            when(col("speed_kmh") < 0, "Negative speed threshold")
            .when((col("latitude") < -90) | (col("latitude") > 90), "Out of boundary coordinates")
            .otherwise("Malformed telemetry payload")
        ).select(
            col("raw_json").alias("raw_payload"),
            col("rejection_reason"),
            col("event_version").alias("schema_version")
        )

        valid_count = valid_df.count()
        invalid_count = invalid_df.count()
        print(f"Valid: {valid_count} | Invalid (Quarantine): {invalid_count}")

        # الحفظ في FactDeliveryTracking
        if valid_count > 0:
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
            print("Successfully written valid records to dbo.FactDeliveryTracking")

        # الحفظ في Quarantine_Tracking
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
            print("Successfully written invalid records to dbo.Quarantine_Tracking")

    except Exception as e:
        print(f"Error writing batch {batch_id} to SQL Server: {e}")

def main():
    # الحزم المتوافقة مع PySpark 4.3.0 و Scala 2.13
    kafka_pkg = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.3.0"
    jdbc_pkg = "com.microsoft.sqlserver:mssql-jdbc:12.4.2.jre11"

    print(f"Loading Spark packages: {kafka_pkg}, {jdbc_pkg}")

    spark = SparkSession.builder \
        .appName("LogisticsRealTimeTelemetry") \
        .config("spark.jars.packages", f"{kafka_pkg},{jdbc_pkg}") \
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