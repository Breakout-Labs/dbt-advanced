with source as (
    {{
    dbt_utils.union_relations(
        relations=[
            source('ecomm', 'orders_us'),
            source('ecomm', 'orders_de'),
            source('ecomm', 'orders_au')
        ],
    )
}}
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

deduplicated as (
    {{
        dbt_utils.deduplicate(
            relation='normalize_order_status',
            partition_by='order_id',
            order_by='_synced_at desc'
        )
    }}
),

final as (
    select *
    from deduplicated
)

select *
from final
