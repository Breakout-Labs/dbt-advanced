with source as (
    {{
    dbt_utils.union_relations(
        relations=[
            source('ecomm', 'orders_us'),
            source('ecomm', 'orders_de'),
            ref('_orders_au_deduped')
        ],
    )
}}
),

renamed as (
    select
        * exclude (store_id),
        id as order_id,
        created_at as ordered_at,
        status as order_status,
        case
            when _dbt_source_relation like '%orders_us' then 1
            when _dbt_source_relation like '%orders_de' then 2
            when _dbt_source_relation like '%orders_au' then 3
        end as store_id
    from source
),

order_statuses as (
    select *
    from {{ ref('order_status') }}
),

normalize_order_status as (
    select
        renamed.*,
        order_statuses.order_status_normalized
    from renamed
    left join order_statuses on lower(renamed.order_status) = order_statuses.order_status
),

final as (
    select *
    from normalize_order_status
)

select *
from final
