{{ config(
    materialized='incremental',
    unique_key='order_id'
) }}

select
    order_id,
    customer_id,
    store_id,
    employee_id,
    order_date,
    sales_channel,
    order_status,
    payment_method
from {{ source('raw_ecommerce', 'orders') }}
where order_id is not null
and customer_id is not null
and store_id is not null
and employee_id is not null

{% if is_incremental() %}

and order_date > (
    select max(order_date) from {{ this }}
)

{% endif %}