{{ config(
    materialized='table',
    snowflake_warehouse='TRANSFORMING_S'
) }}

with orders as (
    select *
    from {{ ref('stg_ecomm__orders') }}
),

deliveries as (
    select *
    from {{ ref('stg_ecomm__deliveries') }}
),

deliveries_filtered as (
    select *
    from deliveries
    where delivery_status = 'delivered'
),

joined as (
    select
        -- 1) PRIMARY KEY: siempre la PRIMERA columna
        {{ dbt_utils.generate_surrogate_key(['order_id']) }} as pk_orders,

        -- 2) FOREIGN KEY hacia customer (surrogate key)
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as hk_customer,

        -- 3) columnas de negocio
        orders.order_id,
        orders.customer_id,
        orders.ordered_at,
        orders.order_status,
        orders.total_amount,

        datediff(
            'minutes', orders.ordered_at, deliveries_filtered.delivered_at
        ) as delivery_time_from_order,

        datediff(
            'minutes',
            deliveries_filtered.picked_up_at,
            deliveries_filtered.delivered_at
        ) as delivery_time_from_collection,

        -- 4) source_last_updated: mayor de los timestamps de las fuentes
        -- Versión robusta que maneja NULLs (devuelve NULL si ambas son NULL)
        case
          when orders._synced_at is null and deliveries_filtered._synced_at is null then null
          when orders._synced_at is null then deliveries_filtered._synced_at
          when deliveries_filtered._synced_at is null then orders._synced_at
          else greatest(orders._synced_at, deliveries_filtered._synced_at)
        end as source_last_updated,

        -- 5) metadato: cuándo se refrescó esta tabla
        current_timestamp() as last_updated

    from orders
    left join deliveries_filtered
        on orders.order_id = deliveries_filtered.order_id
),

final as (
    select *
    from joined
)

select *
from final
;
