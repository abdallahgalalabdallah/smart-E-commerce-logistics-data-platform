SELECT
    
    lower(convert(varchar(50), hashbytes('md5', coalesce(convert(varchar(8000), coalesce(cast(p.product_id as VARCHAR(MAX)), '_dbt_utils_surrogate_key_null_')), '')), 2))
 AS product_key,
    p.product_id,
    COALESCE(ct.product_category_name_english, p.product_category_name, 'unknown') AS product_category_name_english,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm,
    p.product_photos_qty
FROM "smart_logistics_dwh"."staging"."stg_olist__products" p
LEFT JOIN "smart_logistics_dwh"."staging"."stg_olist__category_translation" ct
    ON p.product_category_name = ct.product_category_name