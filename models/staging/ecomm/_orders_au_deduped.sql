{{ config(materialized='ephemeral') }}

select
    *
from {{ source('ecomm', 'orders_au') }}
qualify row_number() over (
    partition by id
    order by _synced_at desc
) = 1



/*
select
*
from raw.ecomm.orders_au
qualify count(*) OVER(partition BY id) > 1
order by id
*/