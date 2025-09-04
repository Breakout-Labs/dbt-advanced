select
    country,
    state.value['state'] as states,
    zip_code.value['city'] as city,
    zip_code.value['zipcode'] as zipcode_per_city
from raw.geo.countries,
    lateral flatten(input => states) as state,
    lateral flatten(input => state.value['zipcodes']) as zip_code
