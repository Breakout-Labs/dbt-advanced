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

renamed as (
    select
        *
    from source
),

final as (
    select
        id as order_id,
        *,
        created_at as ordered_at,
        status as order_status
    from renamed
)

select
  *
from final