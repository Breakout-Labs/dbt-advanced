{{ config(materialized= 'view') }}

select * 
from {{ ref('orders_au_deduped') }}