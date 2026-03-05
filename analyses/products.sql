--select * from raw.ecomm.products

select
    id,
    name,
    category,
    subcategory,
    unit_price,
    is_active,
    created_at,
    _synced_at,
    variant.value:title::text as product_title
from raw.ecomm.products,
    lateral flatten(input => products.variants) as variant