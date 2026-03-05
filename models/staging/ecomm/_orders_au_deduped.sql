select    
    *
from raw.ecomm.orders_au_duped
qualify count(*) over (partition by id) > 1
order by id