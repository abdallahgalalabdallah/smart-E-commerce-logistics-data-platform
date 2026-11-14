import os
import sys
import urllib.parse
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from spark.common.spark_session import get_spark_session

# Load environment variables from infrastructure/.env
load_dotenv(os.path.join(os.path.dirname(__file__), "../../infrastructure/.env"))

BUCKET = "s3a://smart-logistics"

# SQL Server Configuration (Unified with Streaming)
DB_HOST = os.getenv("SQL_SERVER_HOST", "localhost")
DB_PORT = os.getenv("SQL_SERVER_PORT", "14333")
DB_NAME = os.getenv("SQL_SERVER_DB", "smart_logistics_dwh")
DB_USER = os.getenv("SQL_SERVER_USER", "sa")
DB_PASS = os.getenv("SQL_SERVER_PASSWORD", "LogisticsPass2026")
DB_SCHEMA = os.getenv("SQL_SERVER_SCHEMA", "silver")

SILVER_TABLES = [
    "orders",
    "order_items",
    "customers",
    "sellers",
    "products",
    "order_payments",
    "order_reviews",
    "geolocation",
    "category_translation",
]


def get_sqlserver_engine():
    """Create a SQLAlchemy engine for SQL Server using ODBC Driver 17/18."""
    # Try ODBC Driver 18 first, fallback to 17
    driver = "ODBC Driver 17 for SQL Server"
    try:
        import pyodbc
        drivers = [d for d in pyodbc.drivers() if "SQL Server" in d]
        if any("18" in d for d in drivers):
            driver = "ODBC Driver 18 for SQL Server"
        elif drivers:
            driver = drivers[0]
    except Exception:
        pass

    params = urllib.parse.quote_plus(
        f"DRIVER={{{driver}}};"
        f"SERVER={DB_HOST},{DB_PORT};"
        f"DATABASE={DB_NAME};"
        f"UID={DB_USER};"
        f"PWD={DB_PASS};"
        "TrustServerCertificate=yes;"
    )
    return create_engine(
        f"mssql+pyodbc:///?odbc_connect={params}",
        fast_executemany=True,
    )


def ensure_schema_exists(engine, schema_name: str):
    """Create target schema ('silver') in SQL Server if it does not exist."""
    with engine.begin() as conn:
        conn.execute(
            text(
                f"""
                IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{schema_name}')
                BEGIN
                    EXEC('CREATE SCHEMA [{schema_name}]')
                END
                """
            )
        )


def load_silver_to_sqlserver():
    print("=== Starting Silver (S3) -> SQL Server Data Warehouse Load ===")
    spark = get_spark_session()
    engine = get_sqlserver_engine()

    ensure_schema_exists(engine, DB_SCHEMA)
    print(f"[INFO] Connected to {DB_HOST}:{DB_PORT}/{DB_NAME} (Schema: {DB_SCHEMA})")

    for table in SILVER_TABLES:
        s3_path = f"{BUCKET}/silver/olist/{table}/"
        print(f"Reading {table} from S3...")
        df = spark.read.parquet(s3_path)

        # Convert to Pandas for fast bulk insert via fast_executemany
        pdf = df.toPandas()
        row_count = len(pdf)

        pdf.to_sql(
            name=table,
            con=engine,
            schema=DB_SCHEMA,
            if_exists="replace",
            index=False,
            chunksize=5000,
        )
        print(f"[DB OK] {DB_SCHEMA}.{table:<22} | Loaded Rows: {row_count}")

    spark.stop()
    print("=== All 9 Silver Tables Loaded to SQL Server Successfully! ===")


if __name__ == "__main__":
    load_silver_to_sqlserver()