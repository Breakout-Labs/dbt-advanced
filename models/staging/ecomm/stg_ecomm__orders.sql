with source as (
    select *
    from {{ source('ecomm', 'orders') }}
),

stores as (
    select * from {{ ref("stores")}}
),

order_status as (
    select * from {{ ref("order_status")}}
),

renamed as (
    select
        source.*,
        id as order_id,
        created_at as ordered_at,
        status as order_status,
        stores.store_name
    from source
    left join stores on source.store_id = stores.store_id
),

normalize_order_status as (
    select
        renamed.*,
        -- quick & dirty, will fix later - Mike
        order_status.order_status_normalized
    from renamed
    left join order_status on lower(renamed.order_status) = lower(order_status.order_status)
),

final as (
    select *
    from normalize_order_status
)

select *
from final
