import os
import sys
import glob
import uuid
import json
import logging
from datetime import datetime
import boto3
from botocore.client import Config
from dotenv import load_dotenv

# إعداد الـ Logging المنظم
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("OlistBatchLoader")

# تحميل متغيرات البيئة من .env
load_dotenv()

# الإعدادات
LOCALSTACK_ENDPOINT = os.getenv("S3_ENDPOINT_URL", "http://localhost:4566")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "test")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "test")
S3_BUCKET_NAME = os.getenv("S3_BUCKET", "smart-logistics")
RAW_DATA_DIR = os.getenv("RAW_DATA_DIR", "data/sample")

def get_s3_client():
    """تهيئة عميل S3 للاتصال بـ LocalStack."""
    return boto3.client(
        "s3",
        endpoint_url=LOCALSTACK_ENDPOINT,
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        config=Config(s3={"addressing_style": "path"})
    )

def ensure_bucket_exists(s3_client, bucket_name):
    """التحقق من وجود الـ Bucket أو إنشائه تلقائياً في LocalStack."""
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        logger.info(f"Bucket '{bucket_name}' already exists.")
    except Exception:
        logger.info(f"Bucket '{bucket_name}' not found. Creating it...")
        s3_client.create_bucket(Bucket=bucket_name)
        logger.info(f"Bucket '{bucket_name}' created successfully.")

def count_file_lines(file_path):
    """حساب عدد أسطر البيانات دون تحميل الملف بالكامل في الذاكرة."""
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        return sum(1 for _ in f) - 1  # طرح سطر الـ Header

def ingest_olist_batch():
    s3_client = get_s3_client()
    ensure_bucket_exists(s3_client, S3_BUCKET_NAME)

    if not os.path.exists(RAW_DATA_DIR):
        logger.error(f"Source directory '{RAW_DATA_DIR}' does not exist. Aborting.")
        sys.exit(1)

    csv_files = glob.glob(os.path.join(RAW_DATA_DIR, "*.csv"))
    if not csv_files:
        logger.warning(f"No CSV files found in '{RAW_DATA_DIR}'.")
        return

    # إنشاء معرف فريد للـ Batch وطابع زمني موحد لكافة ملفات الدفعة
    batch_id = str(uuid.uuid4())[:8]
    ingestion_date = datetime.utcnow().strftime("%Y-%m-%d")
    ingestion_timestamp = datetime.utcnow().isoformat()

    logger.info(f"Starting batch ingestion | Batch ID: {batch_id} | Date: {ingestion_date}")

    batch_manifest = {
        "batch_id": batch_id,
        "ingestion_date": ingestion_date,
        "ingestion_timestamp": ingestion_timestamp,
        "files_ingested": []
    }

    for file_path in csv_files:
        file_name = os.path.basename(file_path)
        dataset_name = file_name.replace(".csv", "")
        file_size_bytes = os.path.getsize(file_path)
        row_count = count_file_lines(file_path)

        # مسار التخزين في Bronze Layer بنمط يضمن الـ Idempotency والتقسيم الزمني
        s3_key = f"bronze/olist/{dataset_name}/ingestion_date={ingestion_date}/batch_id={batch_id}/{file_name}"

        logger.info(f"Uploading {file_name} ({row_count} rows, {file_size_bytes / 1024:.2f} KB) to s3://{S3_BUCKET_NAME}/{s3_key}")
        
        # رفع الملف الفعلي
        s3_client.upload_file(file_path, S3_BUCKET_NAME, s3_key)

        # تسجيل ميتاداتا كل ملف
        file_meta = {
            "dataset_name": dataset_name,
            "original_filename": file_name,
            "row_count": row_count,
            "size_bytes": file_size_bytes,
            "s3_path": f"s3://{S3_BUCKET_NAME}/{s3_key}"
        }
        batch_manifest["files_ingested"].append(file_meta)

    # رفع ملف الميتاداتا الخاص بالدفعة (Manifest / Audit Log)
    manifest_key = f"bronze/olist/_metadata/ingestion_date={ingestion_date}/manifest_{batch_id}.json"
    s3_client.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=manifest_key,
        Body=json.dumps(batch_manifest, indent=2),
        ContentType="application/json"
    )
    logger.info(f"Batch metadata saved at: s3://{S3_BUCKET_NAME}/{manifest_key}")
    logger.info(f"✅ Batch {batch_id} ingested successfully into Bronze layer.")

if __name__ == "__main__":
    ingest_olist_batch()