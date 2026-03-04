{{ config(materialized='incremental',
        unique_key='order_id' ,
        on_schema_change='append_new_columns',
        enabled=false
)
}}

with orders as (
    select *
    from {{ ref('stg_ecomm__orders') }}
    {% if is_incremental() %}
        where ordered_at >= (select dateadd('day', -3, max(ordered_at)) from {{ this }})
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
store_table as (
    select *
    from {{ ref('stores') }}
),

joined as (
    select
        orders.order_id,
        orders.customer_id,
        orders.store_id,
        st.store_name,
        orders.ordered_at,
        orders.order_status,
        orders.total_amount,
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
    left join store_table as st using (store_id)
),

final as (
    select *
    from joined
)

select *
from final

--select
--    datediff('day', created_at, _synced_at) as days_lag,
--    count(*)
--from raw.ecomm.orders_us
--group by 1