with 

source as (

    select * from {{ source('ecomm', 'orders_us') }}

),

renamed as (

    select
        id,
        total_amount,
        status,
        created_at,
        customer_id,
        store_id,
        _synced_at

    from source

)

select * from renamed