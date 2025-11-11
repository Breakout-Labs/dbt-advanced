select
    country,
    state.value:state::varchar as state, 
    state.value:zipcodes::varchar as zipcodes, 
    zipcode.value:city::varchar as city,
    zipcode.value:zipcode::varchar as zipcode
from raw.geo.countries
left join lateral flatten (input => states) as state   
left join lateral flatten (input =>  state.value:zipcodes) as zipcode