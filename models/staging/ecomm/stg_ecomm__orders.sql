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

renamed as (
    select
        *,
        id as order_id,
        created_at as ordered_at,
        status as order_status
    from sources
),

redefined_store_id as (
    select * exclude(store_id),
        case 
            when _dbt_source_relation ilike '%orders_us' then 1
            when _dbt_source_relation ilike '%orders_de' then 2
            when _dbt_source_relation ilike '%orders_au' then 3
        end as store_id
    from renamed
),

deduplicated_cte as (
  {{ dbt_utils.deduplicate(
      relation='redefined_store_id',
      partition_by='customer_id',
      order_by='customer_id desc',
     )
  }}
),

order_status_lookup as (
    select *
    from {{ ref('order_status') }}
),

normalize_order_status as (
    select
        deduplicated_cte.*,
        coalesce(order_status_lookup.order_status_normalized, 'Unknown') as order_status_normalized
    from deduplicated_cte
    left join order_status_lookup
    on lower(deduplicated_cte.order_status) = order_status_lookup.order_status
),

final as (
    select *
    from normalize_order_status
)

select *
from final
