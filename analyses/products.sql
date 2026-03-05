-- select * from raw.ecomm.products
select
    product_variants.value:title::text as title                     -- Using dot notation
from raw.ecomm.products
left join lateral flatten (input => variants) as product_variants