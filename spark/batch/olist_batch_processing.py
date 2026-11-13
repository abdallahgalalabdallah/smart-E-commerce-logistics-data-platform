import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from pyspark.sql import functions as F
from spark.common.spark_session import get_spark_session
from spark.common.schemas import (
    ORDERS_SCHEMA,
    ORDER_ITEMS_SCHEMA,
    CUSTOMERS_SCHEMA,
    SELLERS_SCHEMA,
    PRODUCTS_SCHEMA,
    ORDER_PAYMENTS_SCHEMA,
    ORDER_REVIEWS_SCHEMA,
    GEOLOCATION_SCHEMA,
    CATEGORY_TRANSLATION_SCHEMA,
)

BUCKET = "s3a://smart-logistics"

# Helper function to save clean vs invalid records to Silver and Quarantine folders
def save_silver_and_quarantine(df, valid_cond, dedup_cols, silver_table, quarantine_reason):
    """Helper function to split clean vs invalid records and write Parquet to S3."""
    silver_df = (
        df.filter(valid_cond)
        .dropDuplicates(dedup_cols)
        .withColumn("processed_at", F.current_timestamp())
    )

    quarantine_df = (
        df.filter(~valid_cond)
        .withColumn("rejection_reason", F.lit(quarantine_reason))
        .withColumn("rejected_at", F.current_timestamp())
    )

    silver_df.write.mode("overwrite").parquet(f"{BUCKET}/silver/olist/{silver_table}/")

    bad_count = quarantine_df.count()
    if bad_count > 0:
        quarantine_df.write.mode("overwrite").parquet(f"{BUCKET}/quarantine/olist/{silver_table}/")

    print(f"[OK] {silver_table:<25} | Silver: {silver_df.count():<7} | Quarantine: {bad_count}")

# process_orders
def process_orders(spark):
    df = (
        spark.read.option("header", "true")
        .schema(ORDERS_SCHEMA)
        .csv(f"{BUCKET}/bronze/olist/olist_orders_dataset/")
    )
    date_cols = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]
    for c in date_cols:
        df = df.withColumn(c, F.to_timestamp(F.col(c)))

    df = df.withColumn("order_status", F.lower(F.trim(F.col("order_status"))))
    valid_cond = (F.col("order_id").isNotNull() &
                  F.col("customer_id").isNotNull() &
                  F.col("order_purchase_timestamp").isNotNull())
    
    save_silver_and_quarantine(df, valid_cond, ["order_id"], "orders", "Missing ID or purchase timestamp")

# process_order_items 
def process_order_items(spark):
    df = (
        spark.read.option("header", "true")
        .schema(ORDER_ITEMS_SCHEMA)
        .csv(f"{BUCKET}/bronze/olist/olist_order_items_dataset/")
    )
    df = df.withColumn("shipping_limit_date", F.to_timestamp(F.col("shipping_limit_date")))
    valid_cond = (
        F.col("order_id").isNotNull()
        & F.col("order_item_id").isNotNull()
        & (F.col("price") >= 0)
        & (F.col("freight_value") >= 0)
    )
    save_silver_and_quarantine(df, valid_cond, ["order_id", "order_item_id"], "order_items", "Invalid ID or negative price/freight")

# process_customers
def process_customers(spark):
    df = (
        spark.read.option("header", "true")
        .schema(CUSTOMERS_SCHEMA)
        .csv(f"{BUCKET}/bronze/olist/olist_customers_dataset/")
    )
    df = (
        df.withColumn("customer_city", F.lower(F.trim(F.col("customer_city"))))
        .withColumn("customer_state", F.upper(F.trim(F.col("customer_state"))))
        .withColumn("customer_zip_code_prefix", F.trim(F.col("customer_zip_code_prefix")))
    )
    valid_cond = F.col("customer_id").isNotNull() & F.col("customer_unique_id").isNotNull()
    save_silver_and_quarantine(df, valid_cond, ["customer_id"], "customers", "Missing customer_id")

# process_sellers
def process_sellers(spark):
    df = (
        spark.read.option("header", "true")
        .schema(SELLERS_SCHEMA)
        .csv(f"{BUCKET}/bronze/olist/olist_sellers_dataset/")
    )
    df = (
        df.withColumn("seller_city", F.lower(F.trim(F.col("seller_city"))))
        .withColumn("seller_state", F.upper(F.trim(F.col("seller_state"))))
        .withColumn("seller_zip_code_prefix", F.trim(F.col("seller_zip_code_prefix")))
    )
    valid_cond = F.col("seller_id").isNotNull()
    save_silver_and_quarantine(df, valid_cond, ["seller_id"], "sellers", "Missing seller_id")

# process_products
def process_products(spark):
    df = (
        spark.read.option("header", "true")
        .schema(PRODUCTS_SCHEMA)
        .csv(f"{BUCKET}/bronze/olist/olist_products_dataset/")
    )
    # Fix typo in source CSV column names and fill null categories
    df = (
        df.withColumnRenamed("product_name_lenght", "product_name_length")
        .withColumnRenamed("product_description_lenght", "product_description_length")
        .withColumn("product_category_name", F.coalesce(F.lower(F.trim(F.col("product_category_name"))), F.lit("unknown")))
    )
    valid_cond = F.col("product_id").isNotNull()
    save_silver_and_quarantine(df, valid_cond, ["product_id"], "products", "Missing product_id")

# process_order_payments
def process_order_payments(spark):
    df = (
        spark.read.option("header", "true")
        .schema(ORDER_PAYMENTS_SCHEMA)
        .csv(f"{BUCKET}/bronze/olist/olist_order_payments_dataset/")
    )
    df = df.withColumn("payment_type", F.lower(F.trim(F.col("payment_type"))))
    valid_cond = F.col("order_id").isNotNull() & (F.col("payment_value") >= 0)
    save_silver_and_quarantine(df, valid_cond, ["order_id", "payment_sequential"], "order_payments", "Missing order_id or negative payment")

# process_order_reviews
def process_order_reviews(spark):
    df = (
        spark.read.option("header", "true")
        .option("multiLine", "true")
        .option("escape", '"')
        .schema(ORDER_REVIEWS_SCHEMA)
        .csv(f"{BUCKET}/bronze/olist/olist_order_reviews_dataset/")
    )
    df = (
        df.withColumn("review_creation_date", F.to_timestamp(F.col("review_creation_date")))
        .withColumn("review_answer_timestamp", F.to_timestamp(F.col("review_answer_timestamp")))
    )
    valid_cond = (
        F.col("review_id").isNotNull() &
        F.col("order_id").isNotNull() &
        F.col("review_score").between(1, 5)
    )
    save_silver_and_quarantine(df, valid_cond, ["review_id", "order_id"], "order_reviews", "Invalid review ID or score out of range (1-5)")


def process_geolocation(spark):
    df = (
        spark.read.option("header", "true")
        .schema(GEOLOCATION_SCHEMA)
        .csv(f"{BUCKET}/bronze/olist/olist_geolocation_dataset/")
    )
    # Aggregate coordinates per zip_code to remove ~1M duplicate rows
    clean_geo = (
        df.filter(F.col("geolocation_zip_code_prefix").isNotNull())
        .withColumn("geolocation_zip_code_prefix", F.trim(F.col("geolocation_zip_code_prefix")))
        .groupBy("geolocation_zip_code_prefix")
        .agg(  # to avoid duplicates, we take the average of lat/lng and first city/state
            F.avg("geolocation_lat").alias("geolocation_lat"),
            F.avg("geolocation_lng").alias("geolocation_lng"),
            F.first(F.lower(F.trim(F.col("geolocation_city")))).alias("geolocation_city"),
            F.first(F.upper(F.trim(F.col("geolocation_state")))).alias("geolocation_state"),
        )
        .withColumn("processed_at", F.current_timestamp())
    )
    clean_geo.write.mode("overwrite").parquet(f"{BUCKET}/silver/olist/geolocation/")
    print(f"[OK] {'geolocation':<25} | Silver: {clean_geo.count():<7} | Quarantine: 0")


def process_category_translation(spark):
    df = (
        spark.read.option("header", "true")
        .schema(CATEGORY_TRANSLATION_SCHEMA)
        .csv(f"{BUCKET}/bronze/olist/product_category_name_translation/")
    )
    df = (
        df.withColumn("product_category_name", F.lower(F.trim(F.col("product_category_name"))))
        .withColumn("product_category_name_english", F.lower(F.trim(F.col("product_category_name_english"))))
    )
    valid_cond = F.col("product_category_name").isNotNull()
    save_silver_and_quarantine(df, valid_cond, ["product_category_name"], "category_translation", "Missing category name")


if __name__ == "__main__":
    print("=== Starting Olist Bronze -> Silver Batch Pipeline ===")
    spark = get_spark_session()

    process_orders(spark)
    process_order_items(spark)
    process_customers(spark)
    process_sellers(spark)
    process_products(spark)
    process_order_payments(spark)
    process_order_reviews(spark)
    process_geolocation(spark)
    process_category_translation(spark)


    print("=== Pipeline Finished Successfully! ===")
    spark.stop()