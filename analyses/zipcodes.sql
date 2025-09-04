select
    country,
    states.value:state as state,
    zip_codes.value:city::text as city,
    zip_codes.value:zipcode::text as zipcode
from raw.geo.countries
left join lateral flatten (input => states) as states
left join lateral flatten (input => states.value:zipcodes) as zip_codes