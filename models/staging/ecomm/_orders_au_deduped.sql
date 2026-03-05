{{config (
    materialized='ephemeral'
)}}

with source as (
  
  select * from  {{
    source('ecomm','orders_au_duped')
}}

),

deduped AS (
    select * from source
qualify (count(*) over (partition by id )) > 1
)


select *
from deduped

