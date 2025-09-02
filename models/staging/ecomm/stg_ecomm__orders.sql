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

normalized_statusses as (
    select 
        raw_status, 
        final_status
    from {{ ref('statusses') }}
),

normalize_order_status as (
    select
        renamed.*,
        case 
        when normalized_statusses.final_status is null then 'Unknown'
        else normalized_statusses.final_status
        end as final_status
    from renamed
    left join normalized_statusses on renamed.order_status = normalized_statusses.raw_status
),

final as (
    select *
    from normalize_order_status
)

select *
from final
