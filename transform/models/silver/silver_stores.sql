select
    store_id,
    store_name,
    address,
    city,
    province,
    region,
    opened_date,
    store_type
from {{ source('raw_ecommerce', 'stores') }}
where store_id is not null
