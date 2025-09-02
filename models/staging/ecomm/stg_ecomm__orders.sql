with source as (
    select *
    from {{ source('ecomm', 'orders') }}
),

order_status as (
    select *
    from {{ ref('order_status') }}
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
       renamed.*,
       coalesce(order_status.order_status_normalized, 'Unknown') as order_status
    from renamed
    left join {{ ref('order_status') }} on (lower(renamed.status) = order_status.order_status)
),

add_store as (
    select 
        nos.*,
        s.store_name,
    from normalize_order_status nos
    left outer join {{ ref('stores') }} s on s.store_id = nos.store_id
),

final as (
    select *
    from add_store
)

select *
from final
