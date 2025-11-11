{{
    config(
        materialized='ephemeral'
    )
}}

with 

source as (

    select * from {{ source('ecomm', 'orders_au') }}

),

renamed as (

    select
        id,
        total_amount,
        currency,
        status,
        created_at,
        customer_id,
        _synced_at

    from source

),


deduplicated as (

    select * from renamed
    qualify row_number() over (partition by id order by _synced_at desc ) = 1 

)

--select count(*) from renamed 

select * from deduplicated

/*
select count(*) as count from deduplicated 
group by id 
order by count desc 
*/