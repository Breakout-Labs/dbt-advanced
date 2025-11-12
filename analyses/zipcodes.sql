SELECT my_country.country,
state_names.value:state::text as state_name,
zips.value:zipcode::number as zipcode,
FROM RAW.GEO.COUNTRIES my_country,
    lateral flatten(input => states) as state_names,
    lateral flatten(input => state_names.value:zipcodes) as zips