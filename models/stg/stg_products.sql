with source as (

    select *
    from {{ source('my_source', 'PRODUCTS') }}

),

base as (

SELECT p.doc:active::boolean as active,
p.doc:attributes.color as color,
p.doc:attributes.warrantyMonths as warrantyMonths,
TO_TIMESTAMP(p.doc:audit.updatedAt."$date"::string) AS updated_at,
p.doc:category.l1::string as l1,
p.doc:category.l2::string as l2,
p.doc:name::string as name,
p.doc:productId::string as productId,
p.doc:sku::string as sku
FROM source p
)

SELECT b.active,
b.color,
b.warrantyMonths,
b.updated_at,
b.l1,
b.l2,
b.name,
b.productId,
b.sku
FROM base b





