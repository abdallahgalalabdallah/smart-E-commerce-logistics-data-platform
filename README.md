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
