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
        r.* exclude (order_status)
        , IFNULL(os.order_status_normalized,'unknown') AS order_status 
    from renamed r
    LEFT JOIN {{ ref('order_status') }} os
    ON lower(r.ORDER_STATUS) = lower(os.ORDER_STATUS)
),


final as (
    select *
    from normalize_order_status
)

select *
from final
