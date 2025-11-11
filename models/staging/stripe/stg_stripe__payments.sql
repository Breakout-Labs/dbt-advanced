with source as (
    select *,

    from {{ source('stripe', 'payments') }}
),

renamed as (
    select
        json_data['order_id']::text as order_id,
        json_data['payment_id']::number as payment_id,
        json_data['payment_type']::text as payment_type,
        json_data['payment_amount']::decimal as payment_amount,
        json_data['created_at']::timestamp as created_at
    from source

),

final as (
    select *
    from renamed
)

select *
from final