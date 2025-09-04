select * from raw.geo.countries


select
    country,
    state.value['state']::text as state,
    zip_code.value['city']::text as city,
    zip_code.value['timezone']::text as timezone,
    zip_code.value['zipcode']::text as zipcode
from raw.geo.countries
left join lateral flatten (input => states) as state
left join lateral flatten (input => state.value['zipcodes']) as zip_code