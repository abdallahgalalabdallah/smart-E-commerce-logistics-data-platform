SELECT
    CAST(customer_id AS VARCHAR(50)) AS customer_id,
    CAST(customer_unique_id AS VARCHAR(50)) AS customer_unique_id,
    CAST(customer_zip_code_prefix AS VARCHAR(10)) AS customer_zip_code_prefix,
    TRIM(CAST(customer_city AS VARCHAR(100))) AS customer_city,
    UPPER(TRIM(CAST(customer_state AS VARCHAR(10)))) AS customer_state
FROM {{ source('silver', 'customers') }}