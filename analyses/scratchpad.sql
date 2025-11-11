select
DATEDIFF( day, created_at, _synced_at) as DIFF
, count(*)
from 
{{ ref('stg_ecomm__orders') }}
group by DIFF
