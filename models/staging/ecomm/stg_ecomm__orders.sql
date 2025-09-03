with source as (
    {{
    dbt_utils.union_relations(
        relations=[
            source('ecomm', 'orders_us'),
            source('ecomm', 'orders_de'),
            source('ecomm', 'orders_au')
        ],
    )
}}
),

renamed as (
    select
        * exclude (store_id),
        _dbt_source_relation as source_table,        
        id as order_id,
        created_at as ordered_at,
        status as order_status
    from source
),

normalize_order_status as (
    select
        *,
        -- quick & dirty, will fix later - Mike
        case 
            when order_status ilike any(
                'ordered', 'order_created') then 'Ordered'
            when lower(order_status) in ('shipped', 'sent')
                then 'Shipped'
            when lower(order_status) = 'pending' or lower(order_status) in ('waiting', 'processing', 'payment_pending') then 'Pending'
            when order_status = 'canceled' or 
            order_status = 'cancelled' then 'Canceled'
            when order_status = 'delivered' then 'Delivered'
            else
                'Unknown'
        end as order_status_normalized,
        case    
            when right(source_table,2) = 'de' then 2
            when right(source_table,2) = 'au' then 3
            else 1 --us country
        end as store_id
    from renamed
),

final as (
    select *
    from normalize_order_status
)

select *
from final
