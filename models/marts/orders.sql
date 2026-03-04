{{ config(materialized='table'
,snowflake_warehouse='TRANSFORMING_S') }}

with orders as (
    select *
    from {{ ref('stg_ecomm__orders') }}
),

deliveries as (
    select *
    from {{ ref('stg_ecomm__deliveries') }}
),

deliveries_filtered as (
    select *
    from deliveries
    where delivery_status = 'delivered'
),

joined as (
    select
        orders.order_id,
        orders.customer_id,
        orders.ordered_at,
        orders.order_status,
        orders.total_amount,
        datediff(
            'minutes', orders.ordered_at, deliveries_filtered.delivered_at
        ) as delivery_time_from_order,
        datediff(
            'minutes',
            deliveries_filtered.picked_up_at,
            deliveries_filtered.delivered_at
        ) as delivery_time_from_collection
    , orders.store_id
    from orders
    left join deliveries_filtered
        on orders.order_id = deliveries_filtered.order_id
),

addingStores as (
    select j.*, s.store_name from joined j left join {{ ref('stores') }} s ON j.store_id = s.store_id
),

final as (
    select *
    from addingStores 
)

select *
from final
