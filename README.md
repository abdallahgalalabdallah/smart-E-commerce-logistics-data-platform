```markdown
# 🚚 Smart E-Commerce Logistics Data Platform

[![Data Engineering Project](https://img.shields.io/badge/Data%20Engineering-Graduation%20Project-blue.svg)](https://github.com)
[![Docker Compose](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)
[![Apache Kafka](https://img.shields.io/badge/Apache-Kafka-231F20?logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![Apache Airflow](https://img.shields.io/badge/Apache-Airflow-017CEE?logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![SQL Server](https://img.shields.io/badge/Microsoft-SQL%20Server-CC292B?logo=microsoftsqlserver&logoColor=white)](https://www.microsoft.com/sql-server)
[![Python](https://img.shields.io/badge/Python-3.10%2B-3776AB?logo=python&logoColor=white)](https://www.python.org/)

An enterprise-grade, end-to-end data platform designed to process, model, and analyze real-time fleet delivery streams and historical e-commerce logistics data using a **Medallion Architecture (Bronze, Silver, Gold)**, scheduled orchestration, automated data quality validation, and real-time visualization.

---

## 📑 Table of Contents
- [Business Problem & Objectives](#-business-problem--objectives)
- [Key Business Questions Answered](#-key-business-questions-answered)
- [System Architecture](#-system-architecture)
- [Data Storage & Medallion Design](#-data-storage--medallion-design)
- [Data Model & Schema (ERD)](#-data-model--schema-erd)
- [Data Quality & Quarantine Mechanism](#-data-quality--quarantine-mechanism)
- [Design Decisions & Trade-Offs](#-design-decisions--trade-offs)
- [Project Structure](#-project-structure)
- [Single-Command Quickstart](#-single-command-quickstart)
- [Monitoring & Alerting](#-monitoring--alerting)

---

## 🎯 Business Problem & Objectives
Logistics and delivery operations struggle with lack of real-time visibility, route delays, high fleet fuel waste, and SLA breaches. 

This platform solves these challenges by:
1. **Streaming Processing:** Ingesting real-time driver GPS locations and shipment telemetry.
2. **Batch Processing:** Aggregating high-volume transactional sales and delivery logs.
3. **Automated Quality:** Isolating corrupted records into quarantine tables without failing the pipelines.
4. **Actionable Insights:** Providing operations managers and executives with real-time operational maps and historical delivery KPIs.

---

## ❓ Key Business Questions Answered
The analytical layer and connected dashboards directly answer three core business questions:
1. **What is the real-time SLA breach rate across shipping hubs, and which delivery routes experience the highest delay frequencies?**
2. **How does driver velocity and transit duration correlate with package damage and delayed shipment status?**
3. **What are the peak operational delivery hours and geographical zones to optimize fleet resource allocation?**

---

## 🏗 System Architecture

The end-to-end pipeline implements a hybrid Lambda-like flow orchestrated seamlessly:


```

[IoT Fleet GPS] --------> Apache Kafka (KRaft) --------> Python Consumer --------> SQL Server (FactDeliveryTracking)
^
|
[E-Commerce Logs] ------> LocalStack S3 (Bronze) ------> PySpark / Python (Silver) ---------+ (Gold DWH)
|
[ Apache Airflow DAGs (Scheduled Workflows) ]               |
v
Power BI / Grafana

```

---

## 🥉🥈🥇 Data Storage & Medallion Design
The platform organizes data across three distinct progressive refinement layers:

| Layer | Technology | Description & Storage Format |
| :--- | :--- | :--- |
| **Bronze (Raw)** | LocalStack S3 | Immutable, append-only raw batch logs (CSV/JSON) and original streaming payloads partitioned by extraction date (`YYYY/MM/DD`). |
| **Silver (Cleansed)** | LocalStack S3 (Parquet) | Cleaned, deduplicated, standardized schemas with handled nulls and filtered valid types. Stored in columnar Parquet for query performance. |
| **Gold (Curated / DWH)** | SQL Server 2022 | Production-ready Star Schema optimized for OLAP analytics, aggregated metrics, and incremental watermarking loads. |

---

## 📊 Data Model & Schema (ERD)
The analytical serving layer uses an optimized **Star Schema**:

### Fact Tables
* **`FactOrders`:** Transactional records covering order status, transit duration, shipping cost, delivery cost, and delivery timestamps.
* **`FactDeliveryTracking`:** Granular streaming GPS telemetries, driver velocities, and coordinates.

### Dimension Tables
* **`DimCustomers`:** Customer profile, address, and localized zone information.
* **`DimDrivers`:** Fleet driver metadata, vehicle specifications, and status.
* **`DimProducts`:** Product catalogue, dimensions, weight, and pricing.
* **`DimDate`:** Complete calendar dimension supporting fiscal and operational time slicing.

---

## 🛡 Data Quality & Quarantine Mechanism
To ensure pipeline resilience, records passing through transformation undergo strict automated validation checks (Uniqueness, Not-Null, Referential Integrity, Valid Coordinate Ranges).


```

Raw Ingestion ---> Validation Filter ---> [Valid Records]   ---> Silver / Gold DWH
---> [Invalid Records] ---> Quarantine / Reject Table

```
* **Quarantine Pattern:** Erroneous rows (e.g., negative freight costs, invalid GPS lat/lon boundaries, null foreign keys) are diverted to a dedicated `Quarantine_Records` table with error codes and ingestion timestamps.
* **Zero Pipeline Crash:** Pipelines finish their execution cycles gracefully while alerting engineers to examine quarantine batches.

---

## ⚖️ Design Decisions & Trade-Offs

| Technology Chosen | Considered Alternative | Justification |
| :--- | :--- | :--- |
| **Apache Kafka (KRaft)** | RabbitMQ / AWS Kinesis | High-throughput distributed log structure with KRaft eliminating ZooKeeper dependency, simplifying local dockerized deployment. |
| **LocalStack S3** | MinIO / Native HDFS | Provides local, 100% cloud-compatible AWS S3 API parity, allowing zero-code adjustments if moving to production AWS. |
| **SQL Server 2022** | PostgreSQL / ClickHouse | Robust enterprise relational warehouse capabilities, mature SSMS tooling, and native Windows/Docker hybrid driver support. |
| **Apache Airflow** | Cron / Prefect | Proven dependency DAG graphs, rich ecosystem operators, backfill support, and production-grade retry/failure policies. |

---

## 📁 Project Structure

```bash
smart-logistics-project/
├── .github/
│   └── workflows/ci.yml         # CI/CD pipeline (Linting, pytest)
├── infrastructure/
│   ├── docker-compose.yml       # Complete multi-service orchestration
│   ├── Dockerfile.airflow       # Airflow customized image with drivers
│   └── scripts/                 # Health checks and initialization scripts
├── ingestion/
│   ├── gps_producer.py          # Kafka streaming GPS event generator
│   ├── gps_consumer.py          # Kafka consumer and database sink
│   └── s3_uploader.py           # Batch raw data uploader to LocalStack S3
├── transformation/
│   ├── clean_orders.py          # Bronze to Silver transformation
│   └── quality_checks.py        # Great Expectations / Data assertions
├── dags/
│   ├── daily_batch_dag.py       # Airflow DAG for daily ETL processing
│   └── streaming_monitor_dag.py # Airflow DAG for monitoring stream health
├── tests/
│   ├── test_transformations.py  # Unit tests for cleansing functions
│   └── test_quality.py          # Validation assertion tests
├── .env.example                 # Environment variables template
├── .gitignore                   # Ignored files (secrets, virtual environments)
├── requirements.txt             # Python dependencies
└── README.md                    # Project documentation

```

---

## 🚀 Single-Command Quickstart

### Prerequisites

* Docker Desktop (v20.10+) with WSL2 enabled.
* Python 3.10+
* Git

### 1. Clone & Prepare Environment

```bash
git clone [https://github.com/abdallahgalalabdallah/smart-E-commerce-logistics-data-platform.git](https://github.com/abdallahgalalabdallah/smart-E-commerce-logistics-data-platform.git)
cd smart-E-commerce-logistics-data-platform
cp .env.example .env

```

### 2. Start Full Infrastructure

Run the entire platform with a single Docker Compose command:

```bash
docker compose -f infrastructure/docker-compose.yml up -d

```

### 3. Verify Services

* **Airflow UI:** `http://localhost:8080` (Default: `airflow` / `airflow`)
* **Kafka Broker:** `localhost:9092`
* **LocalStack (S3):** `http://localhost:4566`
* **SQL Server:** `localhost:1433` (User: `sa`, Database: `smart_logistics_dwh`)
* **Grafana:** `http://localhost:3000`

### 4. Run Unit Tests

```bash
pytest tests/

```

---

## 📈 Monitoring & Alerting

* **Airflow SLA & Retries:** Tasks define a 2-retry policy with exponential backoff on failure.
* **Failure Notifications:** Configured for webhook alerts (Telegram/Slack/Email) whenever critical DAG tasks fail.
* **Runtime Metrics:** Logging tracks execution runtime, processed row volumes, and quarantine error percentages for performance tuning audits.

```

```
