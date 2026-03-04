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
        *,
        id as order_id,
        created_at as ordered_at,
        status as order_status
    from source
),

normalize_order_status as (
    select
        r.* exclude (order_status, store_id)
        , IFNULL(os.order_status_normalized,'unknown') AS order_status 
        , CASE 
            WHEN RIGHT(r._DBT_SOURCE_RELATION,2) = 'us' THEN 1
            WHEN RIGHT(r._DBT_SOURCE_RELATION,2) = 'de' THEN 2
            WHEN RIGHT(r._DBT_SOURCE_RELATION,2) = 'au' THEN 3
        END AS store_id
    from renamed r
    LEFT JOIN {{ ref('order_status') }} os
    ON lower(r.ORDER_STATUS) = lower(os.ORDER_STATUS)
),


final as (
    select *
    from normalize_order_status
)

select *
from final

