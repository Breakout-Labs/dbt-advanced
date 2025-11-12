select
country,
state.value['state']::text as state,
zipcodes.value['city']::text as city, 
zipcodes.value['timezone']::text as timezone, 
zipcodes.value['zipcode']::number as zipcode 
from raw.geo.countries
left join lateral flatten(input=>states) as state
left join lateral flatten(input=>state.value['zipcodes']) as zipcodes