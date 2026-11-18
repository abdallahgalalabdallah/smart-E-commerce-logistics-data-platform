SELECT
    order_id,
    CASE
        WHEN order_delivered_customer_date IS NOT NULL
        THEN DATEDIFF(DAY, order_purchase_timestamp, order_delivered_customer_date)
        ELSE NULL
    END AS delivery_days,
    CASE
        WHEN order_delivered_customer_date IS NOT NULL
        THEN DATEDIFF(DAY, order_estimated_delivery_date, order_delivered_customer_date)
        ELSE NULL
    END AS delay_days
FROM "smart_logistics_dwh"."staging"."stg_olist__orders"