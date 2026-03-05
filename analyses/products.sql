-- analyses/products.sql
-- Analysis: Unpacking JSON product variants
-- Purpose: Transform nested JSON variants into relational rows for easier analysis
-- Unpack variants using LATERAL FLATTEN
select
    id,
    name,
    category,
    subcategory,
    unit_price,
    is_active,
    created_at,
    _synced_at,
    variant.value:title::string as product_title
from raw.ecomm.products,
    lateral flatten(input => products.variants) as variant