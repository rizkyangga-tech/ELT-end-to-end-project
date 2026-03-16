select
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    discount_amount,
    total_amount
from {{ source('raw_ecommerce', 'order_items') }}
where order_item_id is not null
and order_id is not null