select
DATEDIFF( day, created_at, _synced_at) as DIFF
, COUNT(*)
from 
{{ ref('stg_ecomm__orders') }}
group by DIFF