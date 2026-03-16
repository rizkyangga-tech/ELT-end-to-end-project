{{ config(materialized='table') }}

WITH base AS (

SELECT
    oi.order_item_id,
    oi.order_id,

    DATE(
        TIMESTAMP_SECONDS(
            CAST(SAFE_CAST(o.order_date AS INT64) / 1000000000 AS INT64)
        )
    ) AS order_date,

    SAFE_CAST(o.customer_id AS INT64) AS customer_id,
    SAFE_CAST(o.store_id AS INT64) AS store_id,
    SAFE_CAST(o.employee_id AS INT64) AS employee_id,
    SAFE_CAST(oi.product_id AS INT64) AS product_id,

    SAFE_CAST(oi.quantity AS INT64) AS quantity,
    SAFE_CAST(oi.unit_price AS FLOAT64) AS unit_price,
    SAFE_CAST(oi.discount_amount AS FLOAT64) AS discount_amount,

    SAFE_CAST(p.cost_price AS FLOAT64) AS cost_price,

    o.sales_channel,
    o.payment_method,
    o.order_status

FROM {{ ref('silver_orders') }} o

JOIN {{ ref('silver_order_items') }} oi
    ON SAFE_CAST(o.order_id AS INT64) = SAFE_CAST(oi.order_id AS INT64)

JOIN {{ ref('silver_products') }} p
    ON SAFE_CAST(oi.product_id AS INT64) = SAFE_CAST(p.product_id AS INT64)

WHERE o.order_status = 'COMPLETED'

),

customer_orders AS (

SELECT
    *,
    MIN(order_date) OVER (PARTITION BY customer_id) AS first_order_date
FROM base

)

SELECT

order_item_id,
order_id,
order_date,

customer_id,
store_id,
employee_id,
product_id,

quantity,
unit_price,
discount_amount,

quantity * unit_price AS gross_sales,
(quantity * unit_price) - discount_amount AS net_sales,

((quantity * unit_price) - discount_amount) - cost_price AS profit,

SAFE_DIVIDE(
    ((quantity * unit_price) - discount_amount) - cost_price,
    (quantity * unit_price) - discount_amount
) AS profit_margin,

sales_channel,

CASE
    WHEN sales_channel LIKE 'Online%' THEN 'Online'
    ELSE 'Offline'
END AS channel_type,

payment_method,

CASE
    WHEN order_date = first_order_date THEN 'New Customer'
    ELSE 'Returning Customer'
END AS customer_type,

EXTRACT(YEAR FROM order_date) AS order_year,
EXTRACT(MONTH FROM order_date) AS order_month,
EXTRACT(DAY FROM order_date) AS order_day

FROM customer_orders