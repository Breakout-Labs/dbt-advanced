with source as (
    select *
    from {{ source('ecomm', 'orders') }}
),

/*

lag_calculation as (
    select
    --_synced_at,
    --created_at,
    datediff(day,created_at,_synced_at) as order_lag,
    count(*) as amount_lag
    from {{source('ecomm', 'orders')}}
    group by 1
),

*/

renamed as (
    select
        *,
        id as order_id,
        created_at as ordered_at
    from source
),

order_status as (
    select * from {{ref('order_status')}}
),

normalize_order_status as (
    select
        renamed.*,
        coalesce(order_status.order_status_normalized, 'Unknown') as order_status
    from renamed
    left join order_status on order_status.order_status = lower(renamed.status)
),

final as (
    select *
    from normalize_order_status
)

select *
from final
