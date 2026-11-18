SELECT
    CAST(order_id AS VARCHAR(50)) AS order_id,
    CAST(customer_id AS VARCHAR(50)) AS customer_id,
    LOWER(TRIM(CAST(order_status AS VARCHAR(30)))) AS order_status,
    CAST(order_purchase_timestamp AS DATETIME2) AS order_purchase_timestamp,
    CAST(order_approved_at AS DATETIME2) AS order_approved_at,
    CAST(order_delivered_carrier_date AS DATETIME2) AS order_delivered_carrier_date,
    CAST(order_delivered_customer_date AS DATETIME2) AS order_delivered_customer_date,
    CAST(order_estimated_delivery_date AS DATETIME2) AS order_estimated_delivery_date
FROM "smart_logistics_dwh"."silver"."orders"