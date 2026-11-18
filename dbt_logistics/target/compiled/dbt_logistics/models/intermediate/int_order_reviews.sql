SELECT
    order_id,
    ROUND(AVG(CAST(review_score AS FLOAT)), 2) AS avg_review_score,
    COUNT(review_id) AS review_count
FROM "smart_logistics_dwh"."staging"."stg_olist__reviews"
GROUP BY order_id