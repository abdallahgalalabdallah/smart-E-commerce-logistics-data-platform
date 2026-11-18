SELECT
    dd.full_date AS purchase_date,
    dg.state AS customer_state,
    COUNT(fo.order_id) AS total_orders,
    SUM(fo.item_count) AS total_items,
    SUM(fo.payment_total) AS total_revenue,
    SUM(fo.freight_total) AS total_freight,
    ROUND(AVG(CAST(fo.delivery_days AS FLOAT)), 2) AS avg_delivery_days,
    ROUND(AVG(CAST(fo.delay_days AS FLOAT)), 2) AS avg_delay_days,
    ROUND(AVG(fo.avg_review_score), 2) AS avg_review_score
FROM {{ ref('fact_orders') }} fo
LEFT JOIN {{ ref('dim_date') }} dd
    ON fo.purchase_date_key = dd.date_key
LEFT JOIN {{ ref('dim_geography') }} dg
    ON fo.geography_key = dg.geography_key
GROUP BY
    dd.full_date,
    dg.state