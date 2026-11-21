WITH order_item_totals AS (
    SELECT
        order_id,
        COUNT(order_item_id) AS item_count,
        SUM(freight_value) AS freight_total
    FROM "smart_logistics_dwh"."staging"."stg_olist__order_items"
    GROUP BY order_id
)
SELECT
    
    lower(convert(varchar(50), hashbytes('md5', coalesce(convert(varchar(8000), coalesce(cast(o.order_id as VARCHAR(MAX)), '_dbt_utils_surrogate_key_null_')), '')), 2))
 AS order_key,
    o.order_id,
    dc.customer_key,
    dg.geography_key,
    dpd.date_key AS purchase_date_key,
    ddd.date_key AS delivered_date_key,
    o.order_status,
    COALESCE(oit.item_count, 0) AS item_count,
    COALESCE(oit.freight_total, 0) AS freight_total,
    COALESCE(op.payment_total, 0) AS payment_total,
    op.installment_count,
    op.main_payment_type,
    orv.avg_review_score,
    COALESCE(orv.review_count, 0) AS review_count,
    dm.delivery_days,
    dm.delay_days
FROM "smart_logistics_dwh"."staging"."stg_olist__orders" o
LEFT JOIN "smart_logistics_dwh"."staging"."stg_olist__customers" c
    ON o.customer_id = c.customer_id
LEFT JOIN "smart_logistics_dwh"."gold"."dim_customer" dc
    ON c.customer_unique_id = dc.customer_unique_id
LEFT JOIN "smart_logistics_dwh"."gold"."dim_geography" dg
    ON c.customer_zip_code_prefix = dg.zip_code_prefix
LEFT JOIN "smart_logistics_dwh"."gold"."dim_date" dpd
    ON CAST(o.order_purchase_timestamp AS DATE) = dpd.full_date
LEFT JOIN "smart_logistics_dwh"."gold"."dim_date" ddd
    ON CAST(o.order_delivered_customer_date AS DATE) = ddd.full_date
LEFT JOIN order_item_totals oit
    ON o.order_id = oit.order_id
LEFT JOIN "smart_logistics_dwh"."intermediate"."int_order_payments" op
    ON o.order_id = op.order_id
LEFT JOIN "smart_logistics_dwh"."intermediate"."int_order_reviews" orv
    ON o.order_id = orv.order_id
LEFT JOIN "smart_logistics_dwh"."intermediate"."int_order_delivery_metrics" dm
    ON o.order_id = dm.order_id