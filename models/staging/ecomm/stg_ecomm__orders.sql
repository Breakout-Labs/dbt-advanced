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

normalize_order_status as (
    select
        r.* exclude (order_status),
        coalesce(os.order_status_normalized, 'Unknown') as order_status,
    from renamed r
    LEFT JOIN {{ ref('order_status') }} os
    on lower(r.ORDER_STATUS) = os.ORDER_STATUS
),

final as (
    select 
        *,
        current_timestamp() as last_updated
    from normalize_order_status
)

select *
from final
