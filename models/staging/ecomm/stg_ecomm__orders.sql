{{ config(
  snowflake_warehouse='TRANSFORMING_S'
) }}

with source as ( {{
    dbt_utils.union_relations(
        relations=[
            source('ecomm', 'orders_us'),
            source('ecomm', 'orders_de'),
            source('ecomm', 'orders_au')
        ],
    )
}} ),

store_id as (
    select
        * exclude(store_id),
        case
        when
        _dbt_source_relation ilike '%orders_us%'
        then 1
        when
        _dbt_source_relation ilike '%orders_de%'

        then 2
        when
        _dbt_source_relation ilike '%orders_au%'
        then 3
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

order_status as (select * from {{ ref('order_status') }} ),

normalize_order_status as (
    select
        r.*,
        -- case
        -- when r.order_status is null then 'Unkown'
        -- else o.final_status
        -- end as final_status
        coalesce(o.final_status, 'Unknown') as final_status
    from renamed r
    left join order_status o on o.order_status = r.order_status
),

final as (
    select *
    from normalize_order_status
)

select *
from final