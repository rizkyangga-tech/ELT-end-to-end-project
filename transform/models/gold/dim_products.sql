{{ config(materialized='table') }}

WITH base AS (

SELECT
    p.product_id,
    p.product_name,
    p.brand,
    p.cost_price,
    SAFE_CAST(p.category_id AS INT64) AS category_id,

    DATE(p.created_at) AS created_at,

    c.category_name,
    c.department

FROM {{ ref('silver_products') }} p

LEFT JOIN {{ ref('silver_categories') }} c
    ON SAFE_CAST(p.category_id AS INT64) = SAFE_CAST(c.category_id AS INT64)

)

SELECT
    product_id,
    product_name,
    brand,
    cost_price,
    category_name,
    department,
    created_at,
    DATE_DIFF(CURRENT_DATE(), created_at, DAY) AS product_age_days

FROM base