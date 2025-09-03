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

store_id as (
    select * exclude (store_id),
    case 
        when _dbt_source_relation like '%orders_us%' then 1
        when _dbt_source_relation like '%orders_de%' then 2
        when _dbt_source_relation like '%orders_au%' then 3
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

order_status as (
    select * from {{ ref('order_status') }}
),

normalize_order_status as (
    select
        renamed.*,
        coalesce(order_status.order_status_normalized, 'unknown') as order_status_fixed
    from renamed
    left join order_status on
        order_status.order_status = renamed.order_status
),

final as (
    select *
    from normalize_order_status
)

{{ dbt_utils.deduplicate(
    relation='final',
    partition_by='order_id',
    order_by='_synced_at desc',
   )
}}