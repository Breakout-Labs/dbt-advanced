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

order_status as (select * from {{ ref('order_status') }} ),

normalize_order_status as (
    select
        r.*,
        -- case
        -- when r.order_status is null then 'Unkown'
        -- else o.final_status
        -- end as final_status
        coalesce(o.final_status, 'Unknown') as final_status
    from renamed r
    left join order_status o on o.order_status = r.order_status
),

final as (
    select *
    from normalize_order_status
)

select *
from final