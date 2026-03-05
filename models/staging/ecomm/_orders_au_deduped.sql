{{
    config(
        materialized='ephemeral'
    )
}}


select
    *
from {{ source('ecomm', 'orders_au_duped') }}
qualify row_number() over (
    partition by customer_id
    order by _synced_at  desc
) = 1