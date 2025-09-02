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
orders_status as(
    select *
    from {{ ref('Orders_status') }}
),

normalize_order_status as (
    select
        *,
       coalesce(orders_status.order_status_fixed, 'Unknown') as order_status
    from renamed
    left join orders_status on renamed.status = orders_status.order_status
),

final as (
    select *
    from normalize_order_status
)

select *
from final
