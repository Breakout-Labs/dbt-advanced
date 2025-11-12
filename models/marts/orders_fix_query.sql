{{ config(materialized='table',) }}

with orders as (
    select *
    from {{ ref('stg_ecomm__orders') }}
),

deliveries as (
    select *
    from {{ ref('stg_ecomm__deliveries') }}
),

customers as (
    select *
    from {{ ref('stg_ecomm__customers') }}
),

joined as (
    select
        orders.order_id,
        orders.store_id,
        orders.customer_id,
        orders.created_at,
        orders.order_status,
        orders.total_amount,
        deliveries.delivered_at
    from orders
    inner join deliveries on orders.order_id = deliveries.order_id
    inner join customers on orders.customer_id = customers.customer_id
),

final as (
    select *
    from joined
)

select *
from final
