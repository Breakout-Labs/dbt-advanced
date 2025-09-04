select *
from raw.ecomm.orders_au oau
where oau.id in 
(select
    id
from raw.ecomm.orders_au
group by 1
having count(*) > 1)
order by oau.id