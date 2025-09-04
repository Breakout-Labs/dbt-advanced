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

store_names as (
    select *
    from {{ ref('stores') }}
),

joined as (
    select
        --pk added for lab 10 1.1
        {{ dbt_utils.generate_surrogate_key(['orders.order_id']) }} as pk_orders,

        --fk added for lab 10 1.2
        {{ dbt_utils.generate_surrogate_key(['orders.customer_id']) }} as hk_customer,

        orders.order_id,
        orders.customer_id,
        orders.ordered_at,
        orders.order_status,
        orders.total_amount,
        store_names.store_name,
        datediff(
            'minutes', orders.ordered_at, deliveries_filtered.delivered_at
        ) as delivery_time_from_order,
        datediff(
            'minutes',
            deliveries_filtered.picked_up_at,
            deliveries_filtered.delivered_at
        ) as delivery_time_from_collection,

        --source last updated timestamp for lab 10 1.3
        greatest_ignore_nulls(
            orders._synced_at, 
            deliveries_filtered._synced_at
        ) as source_last_updated,

        --timestamp refresh for lab 10 1.4
        current_timestamp() as last_updated

    from orders
    left join
        deliveries_filtered
        on (orders.order_id = deliveries_filtered.order_id)
    left join store_names on (orders.store_id = store_names.store_id)
),

final as (
    select *
    from joined
)

select *
from final