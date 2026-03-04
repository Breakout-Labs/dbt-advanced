with source as (
    select *
    from {{ source('ecomm', 'orders_us') }}
),

renamed as (
    select
        *,
        id as order_id,
        created_at as ordered_at,
        status as order_status
    from source
),

orders_mapping as (
    select *
    from {{ ref('orders_mapping') }}
),

normalize_order_status as (
    select
        r.*,
        coalesce(
            osm.normalized_status,
            'Unknown'
        ) as order_status_normalized
    from renamed r
    left join orders_mapping osm
        on lower(r.order_status) = lower(osm.raw_status)
),

final as (
    select *,
    current_timestamp as last_updated
    from normalize_order_status
)

select *
from final
