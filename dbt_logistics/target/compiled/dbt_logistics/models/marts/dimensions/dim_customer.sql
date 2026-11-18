WITH customer_orders AS (
    SELECT
        c.customer_unique_id,
        c.customer_zip_code_prefix,
        c.customer_city,
        c.customer_state,
        ROW_NUMBER() OVER (
            PARTITION BY c.customer_unique_id
            ORDER BY o.order_purchase_timestamp DESC
        ) AS rn
    FROM "smart_logistics_dwh"."staging"."stg_olist__customers" c
    LEFT JOIN "smart_logistics_dwh"."staging"."stg_olist__orders" o
        ON c.customer_id = o.customer_id
)
SELECT
    
    lower(convert(varchar(50), hashbytes('md5', coalesce(convert(varchar(8000), coalesce(cast(customer_unique_id as VARCHAR(8000)), '_dbt_utils_surrogate_key_null_')), '')), 2))
 AS customer_key,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
FROM customer_orders
WHERE rn = 1