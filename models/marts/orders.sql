{{ config(materialized="table", snowflake_warehouse="TRANSFORMING_S") }}

with
    orders as (select * from {{ ref("stg_ecomm__orders") }}),

    deliveries as (select * from {{ ref("stg_ecomm__deliveries") }}),

    deliveries_filtered as (
        select * from deliveries where delivery_status = 'delivered'
    ),

    store_names as (select * from {{ ref("stores") }}),

    joined as (
        select
            orders.order_id,
            orders.customer_id,
            orders.ordered_at,
            orders.order_status,
            orders.total_amount,
            orders._synced_at,
            store_names.store_name,
            deliveries_filtered._synced_at as deliveries_synced_at,
            datediff(
                'minutes', orders.ordered_at, deliveries_filtered.delivered_at
            ) as delivery_time_from_order,
            datediff(
                'minutes',
                deliveries_filtered.picked_up_at,
                deliveries_filtered.delivered_at
            ) as delivery_time_from_collection
        from orders
        left join
            deliveries_filtered on (orders.order_id = deliveries_filtered.order_id)
        left join store_names on (orders.store_id = store_names.store_id)
    ),

    orders_with_lag as (
        select
            *,
            lag(ordered_at) over (
                partition by customer_id order by ordered_at
            ) as previous_order_date
        from joined
    ),

    final as (
        select
            {{ dbt_utils.generate_surrogate_key(["order_id"]) }} as pk_orders,
            {{ dbt_utils.generate_surrogate_key(["customer_id"]) }} as hk_customer,
            order_id,
            customer_id,
            ordered_at,
            order_status,
            total_amount,
            delivery_time_from_order,
            delivery_time_from_collection,
            store_name,
            datediff('day', previous_order_date, ordered_at) as days_since_last_order,
            greatest_ignore_nulls(
                _synced_at, deliveries_synced_at
            ) as source_last_updated,
            current_timestamp()::timestamp_ltz as last_updated
        from orders_with_lag
    )

select *
from final
