SELECT
    CAST(review_id AS VARCHAR(50)) AS review_id,
    CAST(order_id AS VARCHAR(50)) AS order_id,
    CAST(review_score AS INT) AS review_score,
    CAST(review_comment_title AS NVARCHAR(255)) AS review_comment_title,
    CAST(review_comment_message AS NVARCHAR(MAX)) AS review_comment_message,
    CAST(review_creation_date AS DATETIME2) AS review_creation_date,
    CAST(review_answer_timestamp AS DATETIME2) AS review_answer_timestamp
FROM "smart_logistics_dwh"."silver"."order_reviews"