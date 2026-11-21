# `dbt_logistics` — Smart E-Commerce Logistics Data Warehouse

SQL Server dimensional warehouse built with **dbt (`dbt-sqlserver`)** on top of the Spark-processed **Silver** layer (`smart_logistics_dwh.silver`).

## Architecture & Data Lineage

```text
silver.* (9 tables)
   │
   ▼  [Views: staging.*]
stg_olist__*  ──► Type casting, column standardization, metadata stripping
   │
   ▼  [Views: intermediate.*]
int_order_item_enriched      (Grain: order_id + order_item_id)
int_order_payments           (Grain: order_id)
int_order_reviews            (Grain: order_id)
int_order_delivery_metrics   (Grain: order_id)
   │
   ▼  [Tables: gold.*]
Dimensions: dim_customer, dim_seller, dim_product, dim_geography, dim_date
Facts:      fact_orders (99,441 rows), fact_order_items (112,650 rows)
Aggregates: agg_daily_logistics_summary