{{
    config(
        snowflake_warehouse='TRANSFORMING_S'
    )
}}

with unioned as (
    {{ dbt_utils.union_relations(
        relations=[
            ref('stg_ecomm__orders_au'),
            ref('stg_ecomm__orders_de'),
            ref('stg_ecomm__orders_us')
        ]
    ) }}
),

final as (
    select
        * exclude (store_id),
        case _dbt_source_relation
            when '{{ ref("stg_ecomm__orders_us") }}' then 1
            when '{{ ref("stg_ecomm__orders_de") }}' then 2
            when '{{ ref("stg_ecomm__orders_au") }}' then 3
        end as store_id
    from unioned
)


select * from final