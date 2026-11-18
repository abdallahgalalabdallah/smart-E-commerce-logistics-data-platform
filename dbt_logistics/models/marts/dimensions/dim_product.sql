SELECT
    {{ dbt_utils.generate_surrogate_key(['p.product_id']) }} AS product_key,
    p.product_id,
    COALESCE(ct.product_category_name_english, p.product_category_name, 'unknown') AS product_category_name_english,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm,
    p.product_photos_qty
FROM {{ ref('stg_olist__products') }} p
LEFT JOIN {{ ref('stg_olist__category_translation') }} ct
    ON p.product_category_name = ct.product_category_name