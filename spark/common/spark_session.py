import os
import sys
from pyspark.sql import SparkSession

# تعريف مسار Hadoop الوهمي على الويندوز لحل مشكلة winutils.exe تلقائياً
if sys.platform.startswith("win"):
    os.environ["HADOOP_HOME"] = "C:\\hadoop"
    os.environ["PATH"] += os.pathsep + "C:\\hadoop\\bin"


def get_spark_session(app_name: str = "SmartLogistics"):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        # تحديث hadoop-aws إلى 3.4.1 لحل تعارض VectoredReadUtils مع Spark 4.0
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.1,software.amazon.awssdk:bundle:2.24.6")
        # إعدادات الاتصال بـ LocalStack S3
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:4566")
        .config("spark.hadoop.fs.s3a.access.key", "test")
        .config("spark.hadoop.fs.s3a.secret.key", "test")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # حل مشكلة "60s" و "24h"
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000")
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60")
        .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400")
        # حل مشكلة winutils.exe عند الكتابة على S3 من الويندوز
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer")
        # تعطيل Vectorized Reader كحماية إضافية
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .config("spark.hadoop.fs.s3a.vectored.read.min.seek.size", "128K")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


if __name__ == "__main__":
    spark = get_spark_session()
    print("Spark connected to S3 successfully!")
    spark.stop()