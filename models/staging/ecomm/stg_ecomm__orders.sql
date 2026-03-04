with source as ({{
    dbt_utils.union_relations(
        relations=[
            source('ecomm', 'orders_us'),
            source('ecomm', 'orders_de'),
            source('ecomm', 'orders_au')
        ],
    )
}}),
renamed as (
    select
        *,
        id as order_id,
        created_at as ordered_at,
        status as order_status
    from source
),

normalize_order_status as (
    select
        r.* exclude (order_status),
        coalesce(os.order_status_normalized, 'Unknown') as order_status,
    from renamed r
    LEFT JOIN {{ ref('order_status') }} os
    on lower(r.ORDER_STATUS) = os.ORDER_STATUS
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
