  select
      country,
      state.value:state as state,
      zip_code.value:zipcode as zip_code,
      zip_code.value:city as city
  from raw.geo.countries
  left join lateral flatten (input => states) as state
  left join lateral flatten (input => state.value:zipcodes) as zip_code