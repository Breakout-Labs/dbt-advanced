-- models/staging/stg_ecomm__orders.sql
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
        source.* exclude(store_id),
        id as order_id,
        created_at as ordered_at,
        right(_DBT_SOURCE_RELATION, 2) AS country_code,
        stores.store_id
    from source
    left join {{ ref('stores') }} AS stores ON stores.country_code = upper(right(_DBT_SOURCE_RELATION, 2))
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