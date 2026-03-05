{{ config(
    materialized='table',
    snowflake_warehouse='TRANSFORMING_S') }}

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
        {{ dbt_utils.generate_surrogate_key(['orders.order_id']) }} as pk_orders,
        {{ dbt_utils.generate_surrogate_key(['orders.customer_id']) }} as hk_customer,
        orders.order_id,
        orders.customer_id,
        orders.store_name,
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
        ) as delivery_time_from_collection,
        greatest_ignore_nulls(orders._synced_at, deliveries_filtered._synced_at) as source_last_updated,
        current_timestamp() as last_updated
    from orders
    left join deliveries_filtered
        on orders.order_id = deliveries_filtered.order_id
),

final as (
    select 
        *,
        datediff(
        'day',
        lag(ordered_at) over (
            partition by customer_id
            order by ordered_at
        ),
        ordered_at
        ) as days_since_last_order
    from joined
)

select *
from final
