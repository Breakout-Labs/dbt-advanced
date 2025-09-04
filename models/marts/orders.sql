{{
    config(
        materialized='incremental',
        unique_key='order_id'
    )
}}

with orders as (
    select *,
    {{ dbt_utils.generate_surrogate_key(['order_id']) }} as pk_orders,
    from {{ ref('stg_ecomm__orders') }}

{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where ordered_at > (select dateadd('day', -3, max(ordered_at)) from {{ this }}) 
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

stores as (
    select *
    from {{ ref('stores') }} -- added new CTE with seed reference
),

joined as (
    select
        orders.order_id,
        orders.customer_id,
        orders.ordered_at,
        orders.order_status,
        orders.total_amount,
        orders.store_id, -- added
        stores.store_name, --added
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
        on (orders.order_id = deliveries_filtered.order_id)
    -- join stores seed
    left join stores 
        on (orders.store_id = stores.store_id)
),

final as (
    select *
    from joined
)

select *
from final
