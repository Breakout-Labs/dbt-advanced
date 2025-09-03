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

source_with_store_id as (
    select 
    * exclude (store_id), 
    case
        when _dbt_source_relation = 'raw.ecomm.orders_de' then 2
        when _dbt_source_relation = 'raw.ecomm.orders_us' then 3
        else store_id
    end as store_id
    from source
),

renamed as (
    select
        *,
        id as order_id,
        created_at as ordered_at
    from source_with_store_id
),

order_status as (
    select * from {{ref('order_status')}}
),

normalize_order_status as (
    select
        renamed.*,
        coalesce(order_status.order_status_normalized, 'Unknown') as order_status
    from renamed
    left join order_status on order_status.order_status = lower(renamed.status)
),

final as (
    select *
    from normalize_order_status
)

select *
from final
