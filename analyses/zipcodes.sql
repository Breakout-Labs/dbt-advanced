select country,
    states.value:state::text as state,
    zip_codes.value:city::text as city,
    zip_codes.value:zipcode::number(5,0) as zipcode
from raw.geo.countries,
    lateral flatten (input => states) as states,
    lateral flatten (input => value:zipcodes) as zip_codes