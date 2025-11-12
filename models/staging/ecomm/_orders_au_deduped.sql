select    
    *
from orders_au
qualify row_number() over (
    partition by customer_id
    order by created_at  desc
) = 1