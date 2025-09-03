with stores as (
    select * from {{ ref('stores') }}
),
source as (
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
order_status as (
    select * from {{ ref('order_status')}}
),

store_id as (
    select
        * exclude (store_id),
        case
            when _dbt_source_relation ilike '%orders_us%' then 1
            when _dbt_source_relation ilike '%orders_de%' then 2
            when _dbt_source_relation ilike '%orders_au%' then 3
        end as store_id
    from source
),

renamed as (
    select
        *,
        id as order_id,
        created_at as ordered_at,
        status as order_status
    from store_id
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
        coalesce(order_status.order_status_normalized, 'Unknown')
    from store_mapping
    left join order_status
    on order_status.order_status = lower(store_mapping.order_status)
),

final as (
    select *
    from normalize_order_status
)


{{
    dbt_utils.deduplicate(
        relation='final',
        partition_by='order_id',
        order_by='_synced_at desc'
    )
}}
