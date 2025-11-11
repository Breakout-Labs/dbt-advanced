{{ config(materialized='ephemeral') }}

select
    *
from {{ source('ecomm', 'orders_au') }}
qualify row_number() over (
    partition by id
    order by _synced_at desc
) = 1

/*with ranked_order AS(
    SELECT *, row_number()OVER (
            partition by id 
            ORDER BY updated_at DESC,created_at DESC) as rn 
    FROM {{ source('ecomm', 'orders_au') }}
    
    )
SELECT * EXCLUDE(rn)
FROM ranked_orders
WHERE rn=1*/

