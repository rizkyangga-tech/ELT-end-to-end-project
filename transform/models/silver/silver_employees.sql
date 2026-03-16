{{ config(materialized='table') }}

with ranked as (
select 
    *,
    row_number() over(
        partition by employee_id 
        order by hire_date desc
    ) as rn
from {{ source('raw_ecommerce', 'employees') }}
where employee_id is not null
and store_id is not null
)

select
    employee_id,
    store_id,
    employee_name,
    job_title,
    hire_date,
    salary,
    status
from ranked
where rn = 1