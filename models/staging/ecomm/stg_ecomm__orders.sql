-- models/staging/stg_ecomm__orders.sql
with sources as (
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


order_status as (
    select *
    from {{ ref('order_status') }}
),

renamed as (
    select
        *,
        id as order_id,
        created_at as ordered_at
    from sources
),

normalize_order_status as (
    select
        renamed.*,
        coalesce(order_status.order_status_normalized, 'Unknown') as order_status
    from renamed
    left join order_status on (
        lower(renamed.status) = order_status.order_status
    )
),

store_id as (
    select * exclude (store_id),
    case
        when _dbt_source_relation ilike '%orders_us' then 1
        when _dbt_source_relation ilike '%orders_de' then 2
        when _dbt_source_relation ilike '%_orders_au_deduped' then 3
    end as store_id
    from normalize_order_status
),

final as (
    select *
    from store_id
)

select *
from final
