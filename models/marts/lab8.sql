--''' Version 1 without CTE
--select
--    orders.id as order_id,
--    orders.total_amount,
--    orders.status,
--    orders.store_id,
--    orders.created_at,
--    orders.customer_id,
--    deliveries.delivered_at
--from raw.ecomm.orders_us as orders
--inner join raw.ecomm.deliveries as deliveries on orders.id = deliveries.order_id
--inner join raw.ecomm.customers as customer on orders.customer_id = customer.id
--'''

--- Version 2 with CTE

with orders as (
    select
        *
    from {{ ref('orders') }}
),

customers as (
    select
        *
    from {{ ref('customers') }}
),

deliveries as (
    select
        *
    from {{ ref('stg_ecomm__deliveries') }}
),

final as (
    select
        orders.order_id,
        orders.total_amount,
        orders.order_status,
        orders.store_id,
        orders.ordered_at,
        orders.customer_id,
        deliveries.delivered_at
    from orders
    inner join deliveries using (order_id)
    inner join customers using (customer_id)
)

select * from final
