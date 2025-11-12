with source as (
    select *
    from {{
                dbt_utils.union_relations(
                    relations=[
                        source('ecomm', 'orders_us'),
                        source('ecomm', 'orders_de'),
                        ref('_orders_au_deduped')
                    ],
                )
            }}
),

store_id_added as (
    select
        * exclude(store_id),
        case 
            when _dbt_source_relation like '%orders_us' then 1
            when _dbt_source_relation like '%orders_de' then 2
            when _dbt_source_relation like '%_orders_au_deduped' then 3
        end as store_id
    from source
),

renamed as (
    select
        *,
        id as order_id,
        created_at as ordered_at
    from store_id_added
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

final as (
    select *
    from normalize_order_status
)

select *
from final
