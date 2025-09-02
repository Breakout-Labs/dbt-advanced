with stores as (
    select * from {{ ref('stores') }}
),
source as (
    select *
    from {{ source('ecomm', 'orders') }}
),
order_status as (
    select * from {{ ref('order_status')}}
),

renamed as (
    select
        *,
        id as order_id,
        created_at as ordered_at,
        status as order_status
    from source
),

store_mapping as (
    select
        renamed.*,
        stores.store_name
    from renamed
    left join stores
    on stores.store_id = renamed.store_id
),

normalize_order_status as (
    select
        store_mapping.*,
        coalesce(order_status.order_status_normalized, 'Unknown') as order_status
    from store_mapping
    left join order_status
    on order_status.order_status = lower(store_mapping.order_status)
),

final as (
    select *
    from normalize_order_status
)

select *
from final
