select
    *,
    variant.value:title::string as title,                  
from raw.ecomm.products
left join lateral flatten (input => products.variants) as variant 