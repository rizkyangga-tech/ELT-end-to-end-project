{{ config(materialized='table') }}

SELECT
    customer_id,
    full_name,
    email,
    phone,
    city,
    DATE(birth_date) AS birth_date,
    DATE_DIFF(CURRENT_DATE(), DATE(birth_date), YEAR) AS customer_age,
    created_at

FROM {{ ref('silver_customers') }}