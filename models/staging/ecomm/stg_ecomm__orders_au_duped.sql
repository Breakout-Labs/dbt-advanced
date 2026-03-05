{{
    config(
        materialized='ephemeral'
    )
}}

with source as (
    select
    *
    from {{ source('ecomm', '_orders_au_duped') }}
)

duped as(
select 
    *
    from source
    qualify row_number() over (
            partition by ID
            order by _synced_at desc
    ) = 1
)

select
    *
from duped