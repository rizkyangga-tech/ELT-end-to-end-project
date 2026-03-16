with ranked as (

select 
    *,
    row_number() over (
        partition by category_id
    ) as rn
from {{ source('raw_ecommerce', 'categories') }}
where category_id is not null 
and category_name is not null

)

select
    category_id,
    category_name,
    department,
from ranked
where rn = 1