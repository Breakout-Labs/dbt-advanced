{{
    config(
        materialized='incremental',
        unique_key = 'order_id',
        on_schema_change='append_new_columns'
    )
}}

with orders as (
    select *
    from {{ ref('stg_ecomm__orders') }}
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where ordered_at >= (select dateadd(day, -3, max(ordered_at)) from {{ this }})
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

joined as (
    select
        orders.order_id,
        orders.customer_id,
        orders.ordered_at,
        orders.order_status,
        orders.total_amount,
        orders.store_id, 
        current_timestamp() as last_updated,
        datediff(
            'minutes', orders.ordered_at, deliveries_filtered.delivered_at
        ) as delivery_time_from_order,
        datediff(
            'minutes',
            deliveries_filtered.picked_up_at,
            deliveries_filtered.delivered_at
        ) as delivery_time_from_collection
    from orders
    left join deliveries_filtered
        on orders.order_id = deliveries_filtered.order_id
),

j2 as (
    select 
        joined.*,
        stores.store_name
    from joined 
    left join {{ ref('stores') }} stores on stores.store_id = joined.store_id

 ),

final as (
    select *
    from j2
)

select *
from final

