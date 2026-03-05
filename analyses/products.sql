with source as (
    select * from raw.ecomm.products
),

title_variants as (
    select
        id,
        variant.value:title::string as product_title,
        variant.value:variant_id::string as variant_id
    from raw.ecomm.products,
        lateral flatten(input => products.variants) as variant
),

final as (
    select * exclude (variants)
    from source
    left join title_variants using (id)
)

select * from final