{{ config(materialized='incremental', unique_key='created_at') }}
with ranked as(
select
    *,
    row_number() over(partition by customer_id order by created_at desc) as rn
from {{ source('raw_ecommerce', 'customers') }}
where customer_id is not null
)

select
    customer_id,
    concat(first_name, ' ', last_name) as full_name,
    email,
    phone,
    city,
    birth_date,
    created_at
from ranked
where rn = 1

{% if is_incremental() %}
and created_at > (
    select max(created_at) from {{ this }}
)
{% endif %}