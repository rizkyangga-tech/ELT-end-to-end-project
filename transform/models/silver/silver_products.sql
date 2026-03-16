with base as (

select
    cast(product_id as int64) as product_id,
    cast(category_id as int64) as category_id,
    product_name,
    brand,
    cost_price,
    created_at
from {{ source('raw_ecommerce', 'products') }}
where product_id is not null
and category_id is not null

),

ranked as (

select
    *,
    row_number() over(
        partition by product_id
        order by created_at desc
    ) as rn
from base

)

select
    product_id,
    category_id,
    product_name,
    brand,
    cost_price,
    created_at
from ranked
where rn = 1
