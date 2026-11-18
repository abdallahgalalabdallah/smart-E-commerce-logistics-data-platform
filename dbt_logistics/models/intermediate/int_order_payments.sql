WITH ranked_payments AS (
    SELECT
        order_id,
        payment_type,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY payment_value DESC, payment_sequential ASC
        ) AS rn
    FROM {{ ref('stg_olist__payments') }}
),
aggregated_payments AS (
    SELECT
        order_id,
        SUM(payment_value) AS payment_total,
        MAX(payment_installments) AS installment_count
    FROM {{ ref('stg_olist__payments') }}
    GROUP BY order_id
)
SELECT
    a.order_id,
    a.payment_total,
    a.installment_count,
    r.payment_type AS main_payment_type
FROM aggregated_payments a
LEFT JOIN ranked_payments r
    ON a.order_id = r.order_id
    AND r.rn = 1