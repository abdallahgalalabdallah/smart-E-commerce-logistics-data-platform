SELECT
    CAST(order_id AS VARCHAR(50)) AS order_id,
    CAST(order_item_id AS INT) AS order_item_id,
    CAST(product_id AS VARCHAR(50)) AS product_id,
    CAST(seller_id AS VARCHAR(50)) AS seller_id,
    CAST(shipping_limit_date AS DATETIME2) AS shipping_limit_date,
    CAST(price AS DECIMAL(10, 2)) AS price,
    CAST(freight_value AS DECIMAL(10, 2)) AS freight_value
FROM "smart_logistics_dwh"."silver"."order_items"