with source as (
    -- select *
    -- from {{ source('ecomm', 'orders_us') }}
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
        lower(status) as order_status
    from source
),

order_status as (
    select *
    from {{ ref('order_status') }}
),

normalize_order_status as (
    select
        renamed.* exclude(order_status, store_id),
        -- quick & dirty, will fix later - Mike
        -- case 
        --     when order_status ilike any(
        --         'ordered', 'order_created') then 'Ordered'
        --     when lower(order_status) in ('shipped', 'sent')
        --         then 'Shipped'
        --     when lower(order_status) = 'pending' or lower(order_status) in ('waiting', 'processing', 'payment_pending') then 'Pending'
        --     when order_status = 'canceled' or 
        --     order_status = 'cancelled' then 'Canceled'
        --     when order_status = 'delivered' then 'Delivered'
        --     else
        --         'Unknown'
        -- end as order_status_normalized
        coalesce(order_status.order_status_normalized, 'Unknown') as order_status,
        case
            when _dbt_source_relation ilike '%orders_us' then 1
            when _dbt_source_relation ilike '%orders_de' then 2
            when _dbt_source_relation ilike '%orders_au' then 3
        end as store_id
    from renamed
    left join order_status on (
        renamed.order_status = order_status.order_status
    )
),

final as (
    select *
    from normalize_order_status
)

select *
from final
