{{ config(materialized='view') }}

with src as (
   select * 
   from {{ source('stripe', 'payments') }}
),

payments as (
    select
         json_data['id']::number              as payment_id,
         json_data['order_id']::text          as order_id,
         json_data['method']::text            as payment_type,
         json_data['amount']::number / 100.0  as payment_amount,
         json_data['created_at']::timestamp   as created_at
    from src
)

select * from payments