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
        r.*,
        coalesce(s.order_status_normalized, 'Unknown') as order_status
    from renamed r
    left outer join {{ ref('order_status') }} s on s.order_status = r.order_status
),

add_store as (
    select 
        nos.* ,
        s.store_name
    from normalize_order_status nos
    left outer join {{ ref('stores') }} s on s.store_id = nos.store_id
),

final as (
    select *
    from add_store
)

select *
from final
