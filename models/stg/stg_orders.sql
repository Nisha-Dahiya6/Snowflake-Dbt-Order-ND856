with source as (

    select *
    from {{ source('my_source', 'ORDERS') }}

),

base as (

    SELECT 
    TO_TIMESTAMP(o.doc:audit.updatedAt::string) AS updated_at,
    TO_TIMESTAMP(o.doc:audit.createdAt::string) AS created_at,
      o.doc:customer.customerId::string AS customerId,
      o.doc:customer.name::string AS name,
      o.doc:isDeleted::boolean AS isDeleted,
      o.doc:items as array_items,
      o.doc:statusHistory as array_statusHistory,
      o.doc:orderId::string AS orderId,
      o.doc:region::string AS region
    from source o

),

items as (

select b.customerId,
      i.value:itemId::string as itemId,
      i.value:pricing.currency::string as currency,
      i.value:pricing.unitPrice::string as unitPrice,
      i.value:product.productId::string as productId,
      i.value:qty::string as qty,
      i.value:discounts as array_discounts
      from base b
      left join LATERAL FLATTEN (input => b.array_items) i

),

discounts as (
    select i.customerId,
           d.value:type::string as type,
           d.value:value::string as value        
    from items i
    left join lateral flatten(input => i.array_discounts) d
)
,


statusHistory as
( 
    select b.customerId,
        s.value:status::string as status,
        to_timestamp(s.value:ts::string) as status_time
        from base b
        left join LATERAL FLATTEN (input => b.array_statusHistory) s
        
)

select
        b.updated_at,
    b.created_at,
      b.customerId,
      b.name,
      b.isDeleted,
      d.type,
      d.value,
      i.itemId,
      i.currency,
      i.unitPrice,
      i.productId,
      i.qty,
      s.status as status,
      s.status_time as status_time,
      b.orderId,
      b.region
from base b
left join items i using (customerId)
left join discounts d using (customerId)
left join statusHistory s using (customerId)