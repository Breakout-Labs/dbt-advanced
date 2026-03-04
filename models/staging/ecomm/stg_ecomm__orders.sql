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

order_status as (
    select *
    from {{ ref('order_status') }}
),

renamed as (
    select
        *,
        id as order_id,
        created_at as ordered_at
    from source
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
    select 
    * exclude(store_id),
    current_timestamp() as last_updated,
    case 
        when right(_dbt_source_relation, 2) = 'us' then 1
        when right(_dbt_source_relation, 2) = 'de' then 2
        when right(_dbt_source_relation, 2) = 'au' then 3
    end as store_id
    from normalize_order_status
)

select *
from final
