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

storeid as (
    select
        * exclude (store_id),
        case 
            when split_part(_dbt_source_relation, '_', 2) = 'us' then '1'
            when split_part(_dbt_source_relation, '_', 2) = 'de' then '2'
            else '3'
        end as store_id
    from source
),

renamed as (
    select
        *,
        id as order_id,
        created_at as ordered_at,
        status as order_status
    from storeid
),

stores as (
    select
        *
    from {{ ref('stores') }}
),


join_table as (
    select
        re.*,
        st.store_name
    from renamed re
    left join stores st on re.store_id = st.store_id
),


normalize_order_status as (
    select
        *,
        -- quick & dirty, will fix later - Mike
        case 
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
        end as order_status_normalized
    from join_table
),

final as (
    select *
    from normalize_order_status
)

select *
from final
