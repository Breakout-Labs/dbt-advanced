select 
    products.* exclude(variants),
    products_details.value:title::text as product_title
from raw.ecomm.products as products
left join lateral flatten (input => variants) as products_details