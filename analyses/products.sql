select
    id,
    name,
    variants
from raw.ecomm.products

SELECT
    p.*,
    v.value:title::string AS variant_title
FROM raw.ecomm.products p,
LATERAL FLATTEN(input => p.variants) v;


