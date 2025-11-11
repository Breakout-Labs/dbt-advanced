select
    datediff('day', created_at, _synced_at) as days_lag,
    count(*)
from {{ ref('stg_ecomm__orders') }}
group by days_lag
order by days_lag desc