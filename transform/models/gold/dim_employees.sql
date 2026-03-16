{{ config(materialized='table') }}

SELECT
    employee_id,
    employee_name,
    job_title,
    CAST(salary AS FLOAT64) AS salary,
    status,
    store_id,
    SAFE_CAST(hire_date AS DATE) AS hire_date,

    DATE_DIFF(
        CURRENT_DATE(),
        SAFE_CAST(hire_date AS DATE),
        YEAR
    ) AS years_working

FROM {{ ref('silver_employees') }}