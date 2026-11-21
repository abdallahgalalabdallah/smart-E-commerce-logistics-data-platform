{% docs overview_project %}
# Smart E-Commerce Logistics Data Warehouse (`dbt_logistics`)

This dbt project transforms cleaned Olist e-commerce tables from the **Silver** layer (`smart_logistics_dwh.silver`) into a dimensional **Gold** star schema (`smart_logistics_dwh.gold`) optimized for Power BI and logistics analytics.

### Architectural Boundaries
* **Apache Spark (Upstream):** Handles distributed file ingestion, schema normalization, deduplication, and ZIP-prefix coordinate averaging into Silver.
* **dbt (This Project):** Handles SQL warehouse modeling, surrogate key generation, controlled grain aggregations, star schema joins, and automated data quality testing.
{% enddocs %}

{% docs customer_grain_note %}
**Grain Note (`customer_id` vs. `customer_unique_id`):**  
In the Olist dataset, `customer_id` is an order-scoped foreign key (99,441 rows—one per order). `customer_unique_id` identifies the actual human buyer (~96,096 unique people). `dim_customer` deduplicates to one row per `customer_unique_id` using the customer's most recent order address.
{% enddocs %}

{% docs fact_grain_separation %}
**Grain Separation Rule:**  
Order-level metrics (`fact_orders`, 99,441 rows) and line-item metrics (`fact_order_items`, 112,650 rows) are intentionally isolated into separate fact tables. Joining items directly into an order-level table would duplicate payment totals, delivery days, and review scores across multi-item orders.
{% enddocs %}

{% docs delivery_delay_logic %}
Calculated as `DATEDIFF(DAY, order_estimated_delivery_date, order_delivered_customer_date)`:
* **Positive value (`> 0`):** Delivered late (days past estimated delivery date).
* **Zero (`0`):** Delivered on the exact estimated date.
* **Negative value (`< 0`):** Delivered early.
* **`NULL`:** Order has not yet been delivered to the customer (never imputed as zero).
{% enddocs %}