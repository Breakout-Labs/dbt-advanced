{{ config(
          materialized='incremental',
          unique_key='order_id',
          on_schema_change='append_new_columns'
          )
 }}

with orders as (

    select *
    from {{ ref('stg_ecomm__orders') }}

{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where ordered_at >= (select dateadd(day, -3, max(ordered_at))
     from {{ this }}
    )
{% endif %}

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

status_map as(
    select *
    from {{ ref('order_status') }}
),

joined as (
    select
        orders.order_id,
        orders.customer_id,
        orders.ordered_at,
        orders.order_status,
        coalesce(status_map.order_status_normalized, orders.order_status) as order_status_normalized,
        orders.total_amount,
        store_names.store_name,
        datediff(
            'minutes', orders.ordered_at, deliveries_filtered.delivered_at
        ) as delivery_time_from_order,
        datediff(
            'minutes',
            deliveries_filtered.picked_up_at,
            deliveries_filtered.delivered_at
        ) as delivery_time_from_collection
    from orders orders
    left join
        deliveries_filtered
        on (orders.order_id = deliveries_filtered.order_id)
    left join store_names on (orders.store_id = store_names.store_id)
    left join status_map status_map
        on lower(orders.order_status) = lower(status_map.order_status)
),

final as (
    select 
    j.*,
    current_timestamp() as last_updated
    from joined j
)

select *
from final
