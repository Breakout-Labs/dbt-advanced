{{
    config(
        materialized='ephemeral'
    )
}}

select    
    *
from {{ ref('stg_ecomm__orders_au_duped') }}
qualify row_number() over (
    partition by ID
    order by _synced_at   desc
) = 1
order by ID