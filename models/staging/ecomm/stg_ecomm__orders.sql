with source as (
    select *
    from {{ source('ecomm', 'orders') }}
),

renamed as (
    select
        *,
        id as order_id,
        created_at as ordered_at,
        status as order_status
    from source
),

order_status_seed as (
    select *
    from {{ ref('order_status') }}
),

normalize_order_status as (
    select
        renamed.* exclude(order_status),
        -- quick & dirty, will fix later - Mike
        order_status_seed.order_status_normalized
{#         case 
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
        end as order_status_normalized #}
    from renamed
    left join order_status_seed
    on renamed.order_status = order_status_seed.order_status
),

final as (
    select *
    from normalize_order_status
)

select *
from final
