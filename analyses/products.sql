-- select * from raw.ecomm.products

select
    id,
    name,
    category,
    subcategory,
    unit_price,
    is_active,
    created_at,
    _synced_at,
    product_variant.value:title::text as title                     -- Using dot notation
from raw.ecomm.products
left join lateral flatten (input => variants) as product_variant