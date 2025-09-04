with sources as (
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



stores as (
    select *
    from {{ ref('stores') }}
),

order_status as (
    select *
    from {{ ref('order_status') }}
),

add_store_id as (
    select
        * exclude (store_id),    -- Omit original store_id column
        case
            when _dbt_source_relation ilike '%orders_us' then 1
            when _dbt_source_relation ilike '%orders_de' then 2
            when _dbt_source_relation ilike '%orders_au' then 3
        end as store_id            -- Add calculated store_id
    from sources
),


renamed as (
    select
        *,
        id as order_id,
        created_at as ordered_at,
        status as order_status,

    from add_store_id
),

deduplicated as (
    {{
        dbt_utils.deduplicate(
            relation='renamed',
            partition_by='order_id',
            order_by='_synced_at desc'
        )
    }}
),


store_mapping as (
    select 
    deduplicated.*,
    stores.store_name
    from deduplicated
    left join stores on stores.store_id = deduplicated.store_id
),



normalize_order_status as (
    select
        store_mapping.*,
        order_status.order_status_normalized
    from store_mapping
    left join order_status on order_status.order_status = store_mapping.order_status
),

final as (
    select *
    from normalize_order_status
)

select *
from final
