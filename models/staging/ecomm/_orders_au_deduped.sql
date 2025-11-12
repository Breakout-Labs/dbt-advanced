{{ source('ecomm', 'orders_au') }}

select * 
from {{source("ecomm","orders_au")}}
qualify row_number() over (
        partition by id
        order by _synced_at desc
) = 1
