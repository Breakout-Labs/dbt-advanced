with source as (
    select * from {{ source('ecomm', 'orders_us') }} us
    union by name 
    select *, '2' as store_id from {{ source('ecomm', 'orders_de')}} de 
    union by name 
    select *, '3' as store_id from {{ ref('_orders_au_deduped')}} au 

),



{#

{{
    dbt_utils.union_relations(
        relations=[
            source('ecomm', 'orders_us'),
            source('ecomm', 'orders_de'),
            ref('_orders_au_deduped')
        ],
    )
}}
#}



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
        renamed.*,
        coalesce(order_status.order_status_normalized, 'Unknown') as order_status_normalized
    from renamed
    left join order_status on (
        lower(renamed.status) = order_status.order_status
    )
),

final as (
    select *
    from normalize_order_status
)

/*
select 
    order_id,
    max(datediff('day', created_at, _synced_at)) as datediff
from final 
group by 1
order by 2 desc
*/ 

select *
from final
