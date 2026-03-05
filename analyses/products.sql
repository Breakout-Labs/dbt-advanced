select 
*,
variant.value:title::string as product_title 
from raw.ecomm.products,
    lateral flatten(input => products.variants) as variant