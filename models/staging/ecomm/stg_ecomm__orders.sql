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

add_store_id as (
    select
        * exclude (store_id),
        case
            when _dbt_source_relation ilike '%orders_us' then 1
            when _dbt_source_relation ilike '%orders_de' then 2
            when _dbt_source_relation ilike '%orders_au' then 3
        end as store_id
    from sources
),

renamed as (
    select
        *,
        id as order_id,
        created_at as ordered_at,
        status as order_status
    from add_store_id
),

normalize_order_status as (
    select
        renamed.* exclude (order_status),
        coalesce(order_status.order_status_normalized, 'Unknown') as order_status
    from renamed
    left outer join {{ ref('order_status') }} on order_status.order_status = renamed.order_status
),

add_store as (
    select 
        nos.* ,
        s.store_name
    from normalize_order_status nos
    left outer join {{ ref('stores') }} s on s.store_id = nos.store_id
),

deduplicated as (
    {{
        dbt_utils.deduplicate(
            relation='add_store',
            partition_by='order_id',
            order_by='_synced_at desc'
        )
    }}
)

select
    *
from deduplicated

/*
or just 
    {{
        dbt_utils.deduplicate(
            relation='add_store',
            partition_by='order_id',
            order_by='_synced_at desc'
        )
    }}
*/

