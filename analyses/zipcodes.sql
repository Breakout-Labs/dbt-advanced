  /*select
      country,
      state.value:state as state,
      state.value:zipcodes as zip_codes
  from raw.geo.countries
  left join lateral flatten (input => states) as st*/


    select
      country,
      st.value:state::varchar as state,
      zip_code.value:zipcode::varchar as zip_code,
      zip_code.value:city::varchar as city
  from raw.geo.countries
  left join lateral flatten (input => states) as st
  left join lateral flatten (input => st.value:zipcodes) as zip_code