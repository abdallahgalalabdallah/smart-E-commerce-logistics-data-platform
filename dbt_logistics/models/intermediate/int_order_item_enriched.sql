SELECT
    oi.order_id,
    oi.order_item_id,
    oi.product_id,
    oi.seller_id,
    oi.shipping_limit_date,
    oi.price,
    oi.freight_value,
    COALESCE(ct.product_category_name_english, p.product_category_name, 'unknown') AS product_category_name_english,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm,
    s.seller_zip_code_prefix,
    s.seller_city,
    s.seller_state
FROM {{ ref('stg_olist__order_items') }} oi
LEFT JOIN {{ ref('stg_olist__products') }} p
    ON oi.product_id = p.product_id
LEFT JOIN {{ ref('stg_olist__category_translation') }} ct
    ON p.product_category_name = ct.product_category_name
LEFT JOIN {{ ref('stg_olist__sellers') }} s
    ON oi.seller_id = s.seller_id
    