select
    country,
    states.value:state::text as state,
    zipcodes.value:city::text as city,
    zipcodes.value:timezone::text as timezone,
    zipcodes.value:zipcode::number as zipcode
from raw.geo.countries,
    lateral flatten (input => states) as states,
    lateral flatten (input => states.value:zipcodes) as zipcodes
order by state asc