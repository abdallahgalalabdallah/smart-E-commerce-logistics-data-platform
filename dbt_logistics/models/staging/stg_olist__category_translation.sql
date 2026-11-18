SELECT
    LOWER(TRIM(CAST(product_category_name AS VARCHAR(100)))) AS product_category_name,
    LOWER(TRIM(CAST(product_category_name_english AS VARCHAR(100)))) AS product_category_name_english
FROM {{ source('silver', 'category_translation') }}