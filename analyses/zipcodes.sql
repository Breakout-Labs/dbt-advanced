select 
c.country, 
s.value:name::string        as state_name,
z.value:zipcode::string     as zipcode,
z.value:city::string        as city,
z.value:timezone::string    as timezone
from raw.geo.countries c,
      lateral flatten(input => c.states) as s,
      lateral flatten(input => s.value:zipcodes) as z