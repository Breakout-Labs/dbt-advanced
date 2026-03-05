select
    * exclude(variants),
    product_variant.value['title']::string as title
from raw.ecomm.products,
    lateral flatten (input => variants) as product_variant