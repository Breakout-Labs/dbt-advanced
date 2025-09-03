with 

source as (

    select * from {{ source('stripe', 'payments') }}

),

renamed as (

    select
        json_data

    from source

)

select * from renamed
