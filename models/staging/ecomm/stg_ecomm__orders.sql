with source as ( {{
    dbt_utils.union_relations(
        relations=[
            source('ecomm', 'orders_us'),
            source('ecomm', 'orders_de'),
            source('ecomm', 'orders_au')
        ],
    )

}}),


order_status_1 as (
    select *
    from {{ ref('order_status') }}
),

add_store_id as (
    select
        * exclude (store_id),   
        case
            when _dbt_source_relation ilike '%orders_us' then 1
            when _dbt_source_relation ilike '%orders_de' then 2
            when _dbt_source_relation ilike '%orders_au' then 3
        end as store_id            
    from source
),

renamed as (
    select
        *,
        id as order_id,
        created_at as ordered_at
    from add_store_id
),

normalize_order_status as (
    select
        renamed.*,
        coalesce(order_status_1.final_status, 'Unknown') as order_status
    from renamed
    left join order_status_1 on (
        lower(renamed.status) = order_status_1.raw_status
    )
),

final as (
    select *, current_timestamp as last_updated
    from normalize_order_status
),

deduplicated as (
    {{
        dbt_utils.deduplicate(
            relation='final',
            partition_by='order_id',
            order_by='_synced_at desc'
        )
    }}
)

select *
from deduplicated
