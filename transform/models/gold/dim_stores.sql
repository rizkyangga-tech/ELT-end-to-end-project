SELECT
    store_id,
    store_name,
    address,
    city,
    province,
    region,
    store_type,
    opened_date,
    DATE_DIFF(CURRENT_DATE(), DATE(opened_date), YEAR) AS store_age
FROM {{ ref('silver_stores') }}