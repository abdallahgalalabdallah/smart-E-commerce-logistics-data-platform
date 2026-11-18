SELECT
    {{ dbt_utils.generate_surrogate_key(['i.order_id', 'i.order_item_id']) }} AS order_item_key,
    i.order_id,
    i.order_item_id,
    fo.order_key,
    dp.product_key,
    ds.seller_key,
    i.shipping_limit_date,
    i.price,
    i.freight_value
FROM {{ ref('int_order_item_enriched') }} i
INNER JOIN {{ ref('fact_orders') }} fo
    ON i.order_id = fo.order_id
LEFT JOIN {{ ref('dim_product') }} dp
    ON i.product_id = dp.product_id
LEFT JOIN {{ ref('dim_seller') }} ds
    ON i.seller_id = ds.seller_id