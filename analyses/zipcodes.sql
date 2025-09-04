select
    country,
    state.value['state']::varchar as states,
    zip_code.value['city']::varchar as city,
    zip_code.value['zipcode']::varchar as zipcode_per_city
from raw.geo.countries,
    lateral flatten(input => states) as state,
    lateral flatten(input => state.value['zipcodes']) as zip_code
