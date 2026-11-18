SELECT
    CAST(order_id AS VARCHAR(50)) AS order_id,
    CAST(payment_sequential AS INT) AS payment_sequential,
    LOWER(TRIM(CAST(payment_type AS VARCHAR(30)))) AS payment_type,
    CAST(payment_installments AS INT) AS payment_installments,
    CAST(payment_value AS DECIMAL(10, 2)) AS payment_value
FROM "smart_logistics_dwh"."silver"."order_payments"