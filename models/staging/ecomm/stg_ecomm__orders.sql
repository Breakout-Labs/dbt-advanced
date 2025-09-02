with source as (
    select *
    from {{ source('ecomm', 'orders') }}
),

order_status_csv as (
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
        r.*,
        coalesce(order_status_new, 'Unknown') as order_status
    from renamed r
    left join order_status_csv o on lower(r.status) = o.order_status_old
),

final as (
    select *
    from normalize_order_status
)

select *
from final
