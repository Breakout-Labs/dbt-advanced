

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

store_id_map as (
    select _dbt_source_relation as source_name,
        case _dbt_source_relation
            when 'raw.ecomm.orders_us' then 1
            when 'raw.ecomm.orders_de' then 2
            when 'BREAKOUT_LABS.dbt_kjoshi._orders_au_deduped' then 3
        end as store_id
    from sources
),

renamed as (
    select
        sources.*
            exclude(store_id),
        id as order_id,
        created_at as ordered_at,
        status as order_status,
        store_id_map.store_id as store_id
    from sources
    left join store_id_map
    on sources._dbt_source_relation = store_id_map.source_name
),

order_status_seed as (
    select *
    from {{ ref('order_status') }}
),

normalize_order_status as (
    select
        renamed.* exclude(order_status),
        -- quick & dirty, will fix later - Mike
        order_status_seed.order_status_normalized
{#         case 
            when order_status ilike any(
                'ordered', 'order_created') then 'Ordered'
            when lower(order_status) in ('shipped', 'sent')
                then 'Shipped'
            when lower(order_status) = 'pending' or lower(order_status) in ('waiting', 'processing', 'payment_pending') then 'Pending'
            when order_status = 'canceled' or 
            order_status = 'cancelled' then 'Canceled'
            when order_status = 'delivered' then 'Delivered'
            else
                'Unknown'
        end as order_status_normalized #}
    from renamed
    left join order_status_seed
    on renamed.order_status = order_status_seed.order_status
),

final as (
    select *
    from normalize_order_status
)

select *
from final
