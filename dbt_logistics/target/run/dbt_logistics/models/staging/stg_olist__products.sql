USE [smart_logistics_dwh];
    
    

    

    USE [smart_logistics_dwh];
    EXEC('
        CREATE OR ALTER VIEW "staging"."stg_olist__products" AS SELECT
    CAST(product_id AS VARCHAR(50)) AS product_id,
    LOWER(TRIM(CAST(product_category_name AS VARCHAR(100)))) AS product_category_name,
    CAST(product_name_length AS INT) AS product_name_length,
    CAST(product_description_length AS INT) AS product_description_length,
    CAST(product_photos_qty AS INT) AS product_photos_qty,
    CAST(product_weight_g AS INT) AS product_weight_g,
    CAST(product_length_cm AS INT) AS product_length_cm,
    CAST(product_height_cm AS INT) AS product_height_cm,
    CAST(product_width_cm AS INT) AS product_width_cm
FROM "smart_logistics_dwh"."silver"."products";
    ')

