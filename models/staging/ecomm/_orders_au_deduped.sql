-- models/staging/ecomm/_orders_au_deduped.sql

{{ config(materialized='view') }}

select
    *
from {{ source('ecomm', 'orders_au') }}
qualify row_number() over (
    partition by id
    order by _synced_at desc
) = 1