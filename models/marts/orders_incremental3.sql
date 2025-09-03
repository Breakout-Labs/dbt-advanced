{{
    config(
        materialized='incremental',
        unique_key='order_id'
    )
}}

with orders as (
    select *
    from {{ ref('stg_ecomm__orders') }}
    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        where ordered_at > (select dateadd('day',-3,max(ordered_at)) from {{ this }}) 
    {% endif %}
)

select * from orders