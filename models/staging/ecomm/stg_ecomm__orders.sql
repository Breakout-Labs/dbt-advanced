with source as (
    select *
    from {{ source('ecomm', 'orders') }}
),


order_status_1 as (
    select *
    from {{ ref('order_status') }}
),

renamed as (
    select
        *,
        id as order_id,
        created_at as ordered_at
    from source
),

normalize_order_status as (
    select
        renamed.*,
        coalesce(order_status_1.final_status, 'Unknown') as order_status
    from renamed
    left join order_status_1 on (
        lower(renamed.status) = order_status_1.raw_status
    )
),

final as (
    select *
    from normalize_order_status
)

select *
from final
