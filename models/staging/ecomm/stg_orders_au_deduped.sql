-- models/staging/ecomm/_orders_au_deduped.sql

{{ config(materialized='ephemeral') }}

select
    *
from {{ source('ecomm', 'orders_au_duped') }}
qualify row_number() over (
    partition by id
    order by _synced_at desc
) = 1