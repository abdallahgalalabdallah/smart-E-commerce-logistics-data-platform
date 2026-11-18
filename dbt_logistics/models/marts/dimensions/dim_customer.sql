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
    FROM {{ ref('stg_olist__customers') }} c
    LEFT JOIN {{ ref('stg_olist__orders') }} o
        ON c.customer_id = o.customer_id
)
SELECT
    {{ dbt_utils.generate_surrogate_key(['customer_unique_id']) }} AS customer_key,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
FROM customer_orders
WHERE rn = 1