{{ config(materialized = 'ephemeral') }}

with src as (
    select * from {{ source('ecomm', 'orders_au') }}
),

ranked as (
    select
    src.*,
    row_number() over(
        partition by id 
        order by coalesce(_synced_at, created_at) desc,
                 created_at desc
    ) as row_number
from src 
)
select *
from ranked 
where row_number = 1