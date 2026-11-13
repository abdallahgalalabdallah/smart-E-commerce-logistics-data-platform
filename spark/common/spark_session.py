import os
from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "OlistLogisticsPlatform") -> SparkSession:
    """Create and configure a SparkSession connected to LocalStack S3."""
    if os.name == "nt":
        hadoop_home = r"C:\hadoop"
        hadoop_bin = os.path.join(hadoop_home, "bin")
        os.environ["HADOOP_HOME"] = hadoop_home
        if hadoop_bin not in os.environ.get("PATH", ""):
            os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ.get("PATH", "")

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.4.0,software.amazon.awssdk:bundle:2.24.6",
        )
        # LocalStack S3 endpoint and credentials
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:4566")
        .config("spark.hadoop.fs.s3a.access.key", "test")
        .config("spark.hadoop.fs.s3a.secret.key", "test")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        # Use in-memory buffer for S3A uploads to bypass Windows NativeIO disk checks
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer")
        # Performance and timezone settings
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark