{{ config(materialized='table') }}


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
        orders.store_id,
        orders.ordered_at,
        orders.order_status,
        orders.total_amount,
        stores.store_name,
        orders._synced_at as orders_synced_at,
        deliveries_filtered._synced_at as deliveries_synced_at,
        datediff(
            'minutes', orders.ordered_at, deliveries_filtered.delivered_at
        ) as delivery_time_from_order,
        datediff(
            'minutes',
            deliveries_filtered.picked_up_at,
            deliveries_filtered.delivered_at
        ) as delivery_time_from_collection,
        datediff(
            'day',
            lag(ordered_at) over (partition by customer_id order by ordered_at),
            ordered_at
        ) as days_since_last_order
    from orders
    left join deliveries_filtered
        on orders.order_id = deliveries_filtered.order_id
    left join stores
        on orders.store_id = stores.store_id
),

final as (
    select *,
        {{ dbt_utils.generate_surrogate_key(['order_id']) }} as pk_orders,
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as hk_customer,
        current_timestamp() as last_updated,
        greatest_ignore_nulls(joined.orders_synced_at, joined.deliveries_synced_at) as source_last_updated
    from joined
)

select *
from final
