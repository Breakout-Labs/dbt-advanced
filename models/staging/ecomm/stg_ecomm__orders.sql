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

    /* -- quick & dirty, will fix later - Mike
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
        end as order_status_normalized */

normalize_order_status as (
    select
        renamed.*,
        coalesce(order_status.order_status_normalized, 'Unknown') as order_status_normalized
    from renamed
    left join order_status on (
        lower(renamed.status) = order_status.order_status
    )
),

final as (
    select *
    from normalize_order_status
)

/*
select 
    order_id,
    max(datediff('day', created_at, _synced_at)) as datediff
from final 
group by 1
order by 2 desc
*/ 

select *
from final
