-- Fails if any delivered order has a missing delivery timestamp

-- or was delivered earlier than its purchase timestamp.

SELECT
    order_id,
    order_purchase_timestamp,
    order_delivered_customer_date
FROM "smart_logistics_dwh"."staging"."stg_olist__orders"
WHERE order_status = 'delivered'
  AND (
      order_delivered_customer_date IS NULL
      OR order_delivered_customer_date < order_purchase_timestamp
  )