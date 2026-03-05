SELECT
    product.ID,
    product.Name,
    variant.value:title::string AS variant_title,
    product.UNIT_PRICE
FROM raw.ecomm.products product,
LATERAL FLATTEN(input => product.variants) variant
