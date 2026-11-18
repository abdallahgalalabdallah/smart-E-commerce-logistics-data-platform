SELECT
    CAST(seller_id AS VARCHAR(50)) AS seller_id,
    CAST(seller_zip_code_prefix AS VARCHAR(10)) AS seller_zip_code_prefix,
    TRIM(CAST(seller_city AS VARCHAR(100))) AS seller_city,
    UPPER(TRIM(CAST(seller_state AS VARCHAR(10)))) AS seller_state
FROM {{ source('silver', 'sellers') }}