-- created_at: 2026-02-23T06:04:29.875531+00:00
-- finished_at: 2026-02-23T06:04:30.024504+00:00
-- elapsed: 148ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2994c-3202-5d57-0000-00140f72cf35
-- desc: list_relations_in_parallel
SHOW OBJECTS IN SCHEMA "MY_DB"."MY_SCHEMA" LIMIT 10000;
-- created_at: 2026-02-23T06:04:30.872163+00:00
-- finished_at: 2026-02-23T06:04:31.000540+00:00
-- elapsed: 128ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2994c-3202-5d57-0000-00140f72cf39
-- desc: Get table schema
describe table "MY_DB"."MY_SCHEMA"."DIM_CUSTOMERS";
-- created_at: 2026-02-23T06:04:31.008491+00:00
-- finished_at: 2026-02-23T06:04:31.358734+00:00
-- elapsed: 350ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2994c-3202-5d57-0000-00140f72cf3d
-- desc: Get table schema
describe table "MY_DB"."MY_SCHEMA"."DIM_PRODUCTS";
-- created_at: 2026-02-23T06:04:31.359466+00:00
-- finished_at: 2026-02-23T06:04:31.485598+00:00
-- elapsed: 126ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2994c-3202-5d57-0000-00140f72cf41
-- desc: Get table schema
describe table "MY_DB"."MY_SCHEMA"."FCT_ORDER_ITEMS";
-- created_at: 2026-02-23T06:04:31.486249+00:00
-- finished_at: 2026-02-23T06:04:31.613679+00:00
-- elapsed: 127ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2994c-3202-5d57-0000-00140f72cf4d
-- desc: Get table schema
describe table "MY_DB"."MY_SCHEMA"."FCT_ORDERS";
-- created_at: 2026-02-23T06:04:32.623214+00:00
-- finished_at: 2026-02-23T06:04:32.767792+00:00
-- elapsed: 144ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2994c-3202-5d57-0000-00140f72cf51
-- desc: execute adapter call
show terse schemas in database MY_DB
    limit 10000
/* {"app": "dbt", "connection_name": "", "dbt_version": "2.0.0", "profile_name": "my_project", "target_name": "MY_SCHEMA"} */;
-- created_at: 2026-02-23T06:04:33.363401+00:00
-- finished_at: 2026-02-23T06:04:34.093649+00:00
-- elapsed: 730ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.my_project.stg_products
-- query_id: 01c2994c-3202-5d57-0000-00140f72cf59
-- desc: execute adapter call
create or replace transient  table MY_DB.MY_SCHEMA.stg_products
    
    
    
    as (with source as (

    select *
    from MY_DB.MY_SCHEMA.PRODUCTS

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





    )

/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.my_project.stg_products", "profile_name": "my_project", "target_name": "MY_SCHEMA"} */;
-- created_at: 2026-02-23T06:04:33.351963+00:00
-- finished_at: 2026-02-23T06:04:34.187975+00:00
-- elapsed: 836ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.my_project.stg_customers
-- query_id: 01c2994c-3202-5d57-0000-00140f72cf55
-- desc: execute adapter call
create or replace transient  table MY_DB.MY_SCHEMA.stg_customers
    
    
    
    as (with source as (

    select *
    from MY_DB.MY_SCHEMA.CUSTOMERS

),

base as (

    select
        to_date(doc:audit.createdAt."$date"::string)      as created_at,
        to_date(doc:audit.updatedAt."$date"::string)      as updated_at,
        doc:customerId::string                            as customer_id,
        doc:isDeleted::boolean                            as is_deleted,
        doc:loyalty.points::number                        as loyalty_points,
        doc:loyalty.tier::string                          as loyalty_tier,
        doc:name.first::string                            as first_name,
        doc:name.last::string                             as last_name,
        doc:region::string                                as region,
        doc:contacts.emails                               as emails_array,
        doc:contacts.phones                               as phones_array
    from source

),

emails as (

    select
        b.customer_id,
        e.value::string as email
    from base b
    left join lateral flatten(input => b.emails_array) e

),

phones as (

    select
        b.customer_id,
        p.value:type::string  as phone_type,
        p.value:value::string as phone_number
    from base b
    left join lateral flatten(input => b.phones_array) p

)

select
    b.created_at,
    b.updated_at,
    b.customer_id,
    b.is_deleted,
    b.loyalty_points,
    b.loyalty_tier,
    b.first_name,
    b.last_name,
    b.region,
    e.email,
    p.phone_type,
    p.phone_number
from base b
left join emails e using (customer_id)
left join phones p using (customer_id)
    )

/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.my_project.stg_customers", "profile_name": "my_project", "target_name": "MY_SCHEMA"} */;
-- created_at: 2026-02-23T06:04:34.736191+00:00
-- finished_at: 2026-02-23T06:04:35.028293+00:00
-- elapsed: 292ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.my_project.dim_customers
-- query_id: 01c2994c-3202-5d57-0000-00140f72cf61
-- desc: execute adapter call
create or replace  temporary view MY_DB.MY_SCHEMA.dim_customers__dbt_tmp
  
  
  
  
  as (
    

with source as (

    select *
    from MY_DB.MY_SCHEMA.stg_customers

),

filtered as (

    select *
    from source

    
        where updated_at > (
            select coalesce(max(updated_at), '1900-01-01')
            from MY_DB.MY_SCHEMA.dim_customers
        )
    

)

select
    customer_id,
    first_name,
    last_name,
    email,
    loyalty_tier,
    loyalty_points,
    region,
    is_deleted,
    created_at,
    updated_at
from filtered
  )
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.my_project.dim_customers", "profile_name": "my_project", "target_name": "MY_SCHEMA"} */;
-- created_at: 2026-02-23T06:04:35.030126+00:00
-- finished_at: 2026-02-23T06:04:35.393825+00:00
-- elapsed: 363ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.my_project.dim_customers
-- query_id: 01c2994c-3202-5d57-0000-00140f72cf65
-- desc: execute adapter call
describe table MY_DB.MY_SCHEMA.dim_customers__dbt_tmp
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.my_project.dim_customers", "profile_name": "my_project", "target_name": "MY_SCHEMA"} */;
-- created_at: 2026-02-23T06:04:34.577404+00:00
-- finished_at: 2026-02-23T06:04:35.553937+00:00
-- elapsed: 976ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.my_project.stg_orders
-- query_id: 01c2994c-3202-5d57-0000-00140f72cf5d
-- desc: execute adapter call
create or replace transient  table MY_DB.MY_SCHEMA.stg_orders
    
    
    
    as (with source as (

    select *
    from MY_DB.MY_SCHEMA.ORDERS

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
    )

/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.my_project.stg_orders", "profile_name": "my_project", "target_name": "MY_SCHEMA"} */;
-- created_at: 2026-02-23T06:04:35.396298+00:00
-- finished_at: 2026-02-23T06:04:35.749810+00:00
-- elapsed: 353ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.my_project.dim_customers
-- query_id: 01c2994c-3202-5d57-0000-00140f72cf6d
-- desc: execute adapter call
describe table MY_DB.MY_SCHEMA.dim_customers
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.my_project.dim_customers", "profile_name": "my_project", "target_name": "MY_SCHEMA"} */;
-- created_at: 2026-02-23T06:04:35.559154+00:00
-- finished_at: 2026-02-23T06:04:35.877694+00:00
-- elapsed: 318ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.my_project.fct_order_items
-- query_id: 01c2994c-3202-5d57-0000-00140f72cf69
-- desc: execute adapter call
create or replace  temporary view MY_DB.MY_SCHEMA.fct_order_items__dbt_tmp
  
  
  
  
  as (
    

with source as (

    select *
    from MY_DB.MY_SCHEMA.stg_orders

),

base as (

    select *
    from source

    
        where updated_at > (
            select coalesce(max(order_updated_at), '1900-01-01'::timestamp)
            from MY_DB.MY_SCHEMA.fct_order_items
        )
    
),

discount as (

        select
        orderid,
        itemid,
        productid,
        qty,
        unitprice,
        currency,
        (unitprice * qty) as line_gross,
        case
            when type = 'PCT'
                then (qty * unitprice) * (value / 100)
            when type = 'AMT'
                then value
            else 0
        end as line_discount,
        updated_at
    from base
    where type in ('PCT', 'AMT')

),

final as (

    select
        orderid as order_id,
        itemid as item_id,
        productid as product_id,
        qty,
        unitprice,
        currency,
        line_gross as gross_amount,
        line_discount as discount_amount,
        (line_gross - line_discount) as net_amount,
        updated_at as order_updated_at
    from discount
    

)

select
    order_id,
    item_id,
    product_id,
    qty,
    unitprice,
    currency,
    gross_amount,
    discount_amount,
    net_amount,
    order_updated_at
from final
 


  )
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.my_project.fct_order_items", "profile_name": "my_project", "target_name": "MY_SCHEMA"} */;
-- created_at: 2026-02-23T06:04:35.751997+00:00
-- finished_at: 2026-02-23T06:04:35.889419+00:00
-- elapsed: 137ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.my_project.dim_customers
-- query_id: 01c2994c-3202-5d57-0000-00140f72cf71
-- desc: execute adapter call
describe table "MY_DB"."MY_SCHEMA"."DIM_CUSTOMERS"
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.my_project.dim_customers", "profile_name": "my_project", "target_name": "MY_SCHEMA"} */;
-- created_at: 2026-02-23T06:04:35.894979+00:00
-- finished_at: 2026-02-23T06:04:36.063290+00:00
-- elapsed: 168ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.my_project.dim_customers
-- query_id: 01c2994c-3202-5d57-0000-00140f72cf75
-- desc: execute adapter call
-- back compat for old kwarg name
  
  begin
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.my_project.dim_customers", "profile_name": "my_project", "target_name": "MY_SCHEMA"} */;
-- created_at: 2026-02-23T06:04:35.879461+00:00
-- finished_at: 2026-02-23T06:04:36.252414+00:00
-- elapsed: 372ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.my_project.fct_order_items
-- query_id: 01c2994c-3202-5d57-0000-00140f72cf81
-- desc: execute adapter call
describe table MY_DB.MY_SCHEMA.fct_order_items__dbt_tmp
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.my_project.fct_order_items", "profile_name": "my_project", "target_name": "MY_SCHEMA"} */;
-- created_at: 2026-02-23T06:04:36.086760+00:00
-- finished_at: 2026-02-23T06:04:36.357830+00:00
-- elapsed: 271ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.my_project.fct_orders
-- query_id: 01c2994c-3202-5d57-0000-00140f72cf7d
-- desc: execute adapter call
create or replace  temporary view MY_DB.MY_SCHEMA.fct_orders__dbt_tmp
  
  
  
  
  as (
    

with source as (

    select *
    from MY_DB.MY_SCHEMA.stg_orders

),

base as (

    select *
    from source

    
        where updated_at > (
            select coalesce(max(order_updated_at), '1900-01-01'::timestamp)
            from MY_DB.MY_SCHEMA.fct_orders
        )
    
),

discount as (

    select
        orderid,
        updated_at,
        created_at,
        status,
        region,
        customerid,
        isdeleted,
        (unitprice * qty) as gross_amount,
        case
            when type = 'PCT'
                then (qty * unitprice) * (value / 100)
            when type = 'AMT'
                then value
            else 0
        end as discount_amount
    from base
    where type in ('PCT', 'AMT')

),

final as (

    select
        orderid as order_id,
        max(updated_at) as order_updated_at,
        max(created_at) as order_created_at,
        status as current_status,
        region,
        customerid as customer_id,
        isdeleted as is_deleted,
        sum(gross_amount) as gross_amount,
        sum(discount_amount) as discount_amount,
        sum(gross_amount - discount_amount) as net_amount
    from discount
    group by orderid,status,region,customerid,isdeleted

)

select
    order_id,
    region,
    customer_id,
    order_created_at,
    order_updated_at,
    current_status,
    gross_amount,
    discount_amount,
    net_amount,
    is_deleted
from final



  )
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.my_project.fct_orders", "profile_name": "my_project", "target_name": "MY_SCHEMA"} */;
-- created_at: 2026-02-23T06:04:36.255476+00:00
-- finished_at: 2026-02-23T06:04:36.403326+00:00
-- elapsed: 147ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.my_project.fct_order_items
-- query_id: 01c2994c-3202-5d57-0000-00140f72cf85
-- desc: execute adapter call
describe table MY_DB.MY_SCHEMA.fct_order_items
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.my_project.fct_order_items", "profile_name": "my_project", "target_name": "MY_SCHEMA"} */;
-- created_at: 2026-02-23T06:04:36.360480+00:00
-- finished_at: 2026-02-23T06:04:36.524219+00:00
-- elapsed: 163ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.my_project.fct_orders
-- query_id: 01c2994c-3202-5d57-0000-00140f72cf89
-- desc: execute adapter call
describe table MY_DB.MY_SCHEMA.fct_orders__dbt_tmp
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.my_project.fct_orders", "profile_name": "my_project", "target_name": "MY_SCHEMA"} */;
-- created_at: 2026-02-23T06:04:36.405566+00:00
-- finished_at: 2026-02-23T06:04:36.542720+00:00
-- elapsed: 137ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.my_project.fct_order_items
-- query_id: 01c2994c-3202-5d57-0000-00140f72cf8d
-- desc: execute adapter call
describe table "MY_DB"."MY_SCHEMA"."FCT_ORDER_ITEMS"
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.my_project.fct_order_items", "profile_name": "my_project", "target_name": "MY_SCHEMA"} */;
-- created_at: 2026-02-23T06:04:36.550107+00:00
-- finished_at: 2026-02-23T06:04:37.101331+00:00
-- elapsed: 551ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.my_project.fct_order_items
-- query_id: 01c2994c-3202-5d57-0000-00140f72cf95
-- desc: execute adapter call
-- back compat for old kwarg name
  
  begin
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.my_project.fct_order_items", "profile_name": "my_project", "target_name": "MY_SCHEMA"} */;
-- created_at: 2026-02-23T06:04:36.526592+00:00
-- finished_at: 2026-02-23T06:04:37.102085+00:00
-- elapsed: 575ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.my_project.fct_orders
-- query_id: 01c2994c-3202-5d57-0000-00140f72cf91
-- desc: execute adapter call
describe table MY_DB.MY_SCHEMA.fct_orders
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.my_project.fct_orders", "profile_name": "my_project", "target_name": "MY_SCHEMA"} */;
-- created_at: 2026-02-23T06:04:36.063549+00:00
-- finished_at: 2026-02-23T06:04:37.102083+00:00
-- elapsed: 1.0s
-- outcome: success
-- dialect: snowflake
-- node_id: model.my_project.dim_customers
-- query_id: 01c2994c-3202-5d57-0000-00140f72cf79
-- desc: execute adapter call

    
        
            
	    
	    
            
        
    

    

    merge into MY_DB.MY_SCHEMA.dim_customers as DBT_INTERNAL_DEST
        using MY_DB.MY_SCHEMA.dim_customers__dbt_tmp as DBT_INTERNAL_SOURCE
        on ((DBT_INTERNAL_SOURCE.customer_id = DBT_INTERNAL_DEST.customer_id))

    
    when matched then update set
        "CUSTOMER_ID" = DBT_INTERNAL_SOURCE."CUSTOMER_ID","FIRST_NAME" = DBT_INTERNAL_SOURCE."FIRST_NAME","LAST_NAME" = DBT_INTERNAL_SOURCE."LAST_NAME","EMAIL" = DBT_INTERNAL_SOURCE."EMAIL","LOYALTY_TIER" = DBT_INTERNAL_SOURCE."LOYALTY_TIER","LOYALTY_POINTS" = DBT_INTERNAL_SOURCE."LOYALTY_POINTS","REGION" = DBT_INTERNAL_SOURCE."REGION","IS_DELETED" = DBT_INTERNAL_SOURCE."IS_DELETED","CREATED_AT" = DBT_INTERNAL_SOURCE."CREATED_AT","UPDATED_AT" = DBT_INTERNAL_SOURCE."UPDATED_AT"
    

    when not matched then insert
        ("CUSTOMER_ID", "FIRST_NAME", "LAST_NAME", "EMAIL", "LOYALTY_TIER", "LOYALTY_POINTS", "REGION", "IS_DELETED", "CREATED_AT", "UPDATED_AT")
    values
        ("CUSTOMER_ID", "FIRST_NAME", "LAST_NAME", "EMAIL", "LOYALTY_TIER", "LOYALTY_POINTS", "REGION", "IS_DELETED", "CREATED_AT", "UPDATED_AT")


/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.my_project.dim_customers", "profile_name": "my_project", "target_name": "MY_SCHEMA"} */;
-- created_at: 2026-02-23T06:04:37.104192+00:00
-- finished_at: 2026-02-23T06:04:37.242202+00:00
-- elapsed: 138ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.my_project.fct_orders
-- query_id: 01c2994c-3202-5d57-0000-00140f72cf99
-- desc: execute adapter call
describe table "MY_DB"."MY_SCHEMA"."FCT_ORDERS"
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.my_project.fct_orders", "profile_name": "my_project", "target_name": "MY_SCHEMA"} */;
-- created_at: 2026-02-23T06:04:37.249213+00:00
-- finished_at: 2026-02-23T06:04:37.393456+00:00
-- elapsed: 144ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.my_project.fct_orders
-- query_id: 01c2994c-3202-5d57-0000-00140f72cfa5
-- desc: execute adapter call
-- back compat for old kwarg name
  
  begin
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.my_project.fct_orders", "profile_name": "my_project", "target_name": "MY_SCHEMA"} */;
-- created_at: 2026-02-23T06:04:37.102218+00:00
-- finished_at: 2026-02-23T06:04:37.394288+00:00
-- elapsed: 292ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.my_project.dim_customers
-- query_id: 01c2994c-3202-5d57-0000-00140f72cf9d
-- desc: execute adapter call

    commit
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.my_project.dim_customers", "profile_name": "my_project", "target_name": "MY_SCHEMA"} */;
-- created_at: 2026-02-23T06:04:37.396367+00:00
-- finished_at: 2026-02-23T06:04:37.578640+00:00
-- elapsed: 182ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.my_project.dim_customers
-- query_id: 01c2994c-3202-5d57-0000-00140f72cfad
-- desc: execute adapter call
drop view if exists MY_DB.MY_SCHEMA.dim_customers__dbt_tmp cascade
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.my_project.dim_customers", "profile_name": "my_project", "target_name": "MY_SCHEMA"} */;
-- created_at: 2026-02-23T06:04:37.101533+00:00
-- finished_at: 2026-02-23T06:04:37.681732+00:00
-- elapsed: 580ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.my_project.fct_order_items
-- query_id: 01c2994c-3202-5d57-0000-00140f72cfa1
-- desc: execute adapter call

    
        
            
                
                
            
                
                
            
        
    

    

    merge into MY_DB.MY_SCHEMA.fct_order_items as DBT_INTERNAL_DEST
        using MY_DB.MY_SCHEMA.fct_order_items__dbt_tmp as DBT_INTERNAL_SOURCE
        on (
                    DBT_INTERNAL_SOURCE.order_id = DBT_INTERNAL_DEST.order_id
                ) and (
                    DBT_INTERNAL_SOURCE.item_id = DBT_INTERNAL_DEST.item_id
                )

    
    when matched then update set
        "ORDER_ID" = DBT_INTERNAL_SOURCE."ORDER_ID","ITEM_ID" = DBT_INTERNAL_SOURCE."ITEM_ID","PRODUCT_ID" = DBT_INTERNAL_SOURCE."PRODUCT_ID","QTY" = DBT_INTERNAL_SOURCE."QTY","UNITPRICE" = DBT_INTERNAL_SOURCE."UNITPRICE","CURRENCY" = DBT_INTERNAL_SOURCE."CURRENCY","GROSS_AMOUNT" = DBT_INTERNAL_SOURCE."GROSS_AMOUNT","DISCOUNT_AMOUNT" = DBT_INTERNAL_SOURCE."DISCOUNT_AMOUNT","NET_AMOUNT" = DBT_INTERNAL_SOURCE."NET_AMOUNT","ORDER_UPDATED_AT" = DBT_INTERNAL_SOURCE."ORDER_UPDATED_AT"
    

    when not matched then insert
        ("ORDER_ID", "ITEM_ID", "PRODUCT_ID", "QTY", "UNITPRICE", "CURRENCY", "GROSS_AMOUNT", "DISCOUNT_AMOUNT", "NET_AMOUNT", "ORDER_UPDATED_AT")
    values
        ("ORDER_ID", "ITEM_ID", "PRODUCT_ID", "QTY", "UNITPRICE", "CURRENCY", "GROSS_AMOUNT", "DISCOUNT_AMOUNT", "NET_AMOUNT", "ORDER_UPDATED_AT")


/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.my_project.fct_order_items", "profile_name": "my_project", "target_name": "MY_SCHEMA"} */;
-- created_at: 2026-02-23T06:04:37.393653+00:00
-- finished_at: 2026-02-23T06:04:37.719098+00:00
-- elapsed: 325ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.my_project.fct_orders
-- query_id: 01c2994c-3202-5d57-0000-00140f72cfa9
-- desc: execute adapter call

    
        
            
	    
	    
            
        
    

    

    merge into MY_DB.MY_SCHEMA.fct_orders as DBT_INTERNAL_DEST
        using MY_DB.MY_SCHEMA.fct_orders__dbt_tmp as DBT_INTERNAL_SOURCE
        on ((DBT_INTERNAL_SOURCE.order_id = DBT_INTERNAL_DEST.order_id))

    
    when matched then update set
        "ORDER_ID" = DBT_INTERNAL_SOURCE."ORDER_ID","REGION" = DBT_INTERNAL_SOURCE."REGION","CUSTOMER_ID" = DBT_INTERNAL_SOURCE."CUSTOMER_ID","ORDER_CREATED_AT" = DBT_INTERNAL_SOURCE."ORDER_CREATED_AT","ORDER_UPDATED_AT" = DBT_INTERNAL_SOURCE."ORDER_UPDATED_AT","CURRENT_STATUS" = DBT_INTERNAL_SOURCE."CURRENT_STATUS","GROSS_AMOUNT" = DBT_INTERNAL_SOURCE."GROSS_AMOUNT","DISCOUNT_AMOUNT" = DBT_INTERNAL_SOURCE."DISCOUNT_AMOUNT","NET_AMOUNT" = DBT_INTERNAL_SOURCE."NET_AMOUNT","IS_DELETED" = DBT_INTERNAL_SOURCE."IS_DELETED"
    

    when not matched then insert
        ("ORDER_ID", "REGION", "CUSTOMER_ID", "ORDER_CREATED_AT", "ORDER_UPDATED_AT", "CURRENT_STATUS", "GROSS_AMOUNT", "DISCOUNT_AMOUNT", "NET_AMOUNT", "IS_DELETED")
    values
        ("ORDER_ID", "REGION", "CUSTOMER_ID", "ORDER_CREATED_AT", "ORDER_UPDATED_AT", "CURRENT_STATUS", "GROSS_AMOUNT", "DISCOUNT_AMOUNT", "NET_AMOUNT", "IS_DELETED")


/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.my_project.fct_orders", "profile_name": "my_project", "target_name": "MY_SCHEMA"} */;
-- created_at: 2026-02-23T06:04:37.719351+00:00
-- finished_at: 2026-02-23T06:04:37.934967+00:00
-- elapsed: 215ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.my_project.fct_orders
-- query_id: 01c2994c-3202-5d57-0000-00140f72cfb5
-- desc: execute adapter call

    commit
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.my_project.fct_orders", "profile_name": "my_project", "target_name": "MY_SCHEMA"} */;
-- created_at: 2026-02-23T06:04:37.682003+00:00
-- finished_at: 2026-02-23T06:04:37.980497+00:00
-- elapsed: 298ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.my_project.fct_order_items
-- query_id: 01c2994c-3202-5d57-0000-00140f72cfb1
-- desc: execute adapter call

    commit
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.my_project.fct_order_items", "profile_name": "my_project", "target_name": "MY_SCHEMA"} */;
-- created_at: 2026-02-23T06:04:37.937918+00:00
-- finished_at: 2026-02-23T06:04:38.130679+00:00
-- elapsed: 192ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.my_project.fct_orders
-- query_id: 01c2994c-3202-5d57-0000-00140f72cfb9
-- desc: execute adapter call
drop view if exists MY_DB.MY_SCHEMA.fct_orders__dbt_tmp cascade
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.my_project.fct_orders", "profile_name": "my_project", "target_name": "MY_SCHEMA"} */;
-- created_at: 2026-02-23T06:04:37.982945+00:00
-- finished_at: 2026-02-23T06:04:38.205369+00:00
-- elapsed: 222ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.my_project.fct_order_items
-- query_id: 01c2994c-3202-5d57-0000-00140f72cfbd
-- desc: execute adapter call
drop view if exists MY_DB.MY_SCHEMA.fct_order_items__dbt_tmp cascade
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.my_project.fct_order_items", "profile_name": "my_project", "target_name": "MY_SCHEMA"} */;
-- created_at: 2026-02-23T06:04:42.288224+00:00
-- finished_at: 2026-02-23T06:04:42.598486+00:00
-- elapsed: 310ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.my_project.dim_products
-- query_id: 01c2994c-3202-5d57-0000-00140f72cfc1
-- desc: execute adapter call
create or replace  temporary view MY_DB.MY_SCHEMA.dim_products__dbt_tmp
  
  
  
  
  as (
    

with source as (

    select *
    from MY_DB.MY_SCHEMA.stg_products

),

filtered as (

    select *
    from source

    
        where updated_at > (
            select coalesce(max(updated_at), '1900-01-01')
            from MY_DB.MY_SCHEMA.dim_products
        )
    

)

select
    productid,
    name,
    sku,
    l1,
    l2,
    warrantymonths,
    color,
    active,
    updated_at
from filtered
  )
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.my_project.dim_products", "profile_name": "my_project", "target_name": "MY_SCHEMA"} */;
-- created_at: 2026-02-23T06:04:42.600292+00:00
-- finished_at: 2026-02-23T06:04:42.745117+00:00
-- elapsed: 144ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.my_project.dim_products
-- query_id: 01c2994c-3202-5d57-0000-00140f72cfc5
-- desc: execute adapter call
describe table MY_DB.MY_SCHEMA.dim_products__dbt_tmp
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.my_project.dim_products", "profile_name": "my_project", "target_name": "MY_SCHEMA"} */;
-- created_at: 2026-02-23T06:04:42.746979+00:00
-- finished_at: 2026-02-23T06:04:42.881733+00:00
-- elapsed: 134ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.my_project.dim_products
-- query_id: 01c2994c-3202-5d57-0000-00140f72cfc9
-- desc: execute adapter call
describe table MY_DB.MY_SCHEMA.dim_products
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.my_project.dim_products", "profile_name": "my_project", "target_name": "MY_SCHEMA"} */;
-- created_at: 2026-02-23T06:04:42.883983+00:00
-- finished_at: 2026-02-23T06:04:43.248859+00:00
-- elapsed: 364ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.my_project.dim_products
-- query_id: 01c2994c-3202-5d57-0000-00140f72cfcd
-- desc: execute adapter call
describe table "MY_DB"."MY_SCHEMA"."DIM_PRODUCTS"
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.my_project.dim_products", "profile_name": "my_project", "target_name": "MY_SCHEMA"} */;
-- created_at: 2026-02-23T06:04:43.253556+00:00
-- finished_at: 2026-02-23T06:04:43.415031+00:00
-- elapsed: 161ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.my_project.dim_products
-- query_id: 01c2994c-3202-5d57-0000-00140f72cfd1
-- desc: execute adapter call
-- back compat for old kwarg name
  
  begin
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.my_project.dim_products", "profile_name": "my_project", "target_name": "MY_SCHEMA"} */;
-- created_at: 2026-02-23T06:04:43.415161+00:00
-- finished_at: 2026-02-23T06:04:44.063108+00:00
-- elapsed: 647ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.my_project.dim_products
-- query_id: 01c2994c-3202-5d57-0000-00140f72cfd5
-- desc: execute adapter call

    
        
            
	    
	    
            
        
    

    

    merge into MY_DB.MY_SCHEMA.dim_products as DBT_INTERNAL_DEST
        using MY_DB.MY_SCHEMA.dim_products__dbt_tmp as DBT_INTERNAL_SOURCE
        on ((DBT_INTERNAL_SOURCE.productid = DBT_INTERNAL_DEST.productid))

    
    when matched then update set
        "PRODUCTID" = DBT_INTERNAL_SOURCE."PRODUCTID","NAME" = DBT_INTERNAL_SOURCE."NAME","SKU" = DBT_INTERNAL_SOURCE."SKU","L1" = DBT_INTERNAL_SOURCE."L1","L2" = DBT_INTERNAL_SOURCE."L2","WARRANTYMONTHS" = DBT_INTERNAL_SOURCE."WARRANTYMONTHS","COLOR" = DBT_INTERNAL_SOURCE."COLOR","ACTIVE" = DBT_INTERNAL_SOURCE."ACTIVE","UPDATED_AT" = DBT_INTERNAL_SOURCE."UPDATED_AT"
    

    when not matched then insert
        ("PRODUCTID", "NAME", "SKU", "L1", "L2", "WARRANTYMONTHS", "COLOR", "ACTIVE", "UPDATED_AT")
    values
        ("PRODUCTID", "NAME", "SKU", "L1", "L2", "WARRANTYMONTHS", "COLOR", "ACTIVE", "UPDATED_AT")


/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.my_project.dim_products", "profile_name": "my_project", "target_name": "MY_SCHEMA"} */;
-- created_at: 2026-02-23T06:04:44.063423+00:00
-- finished_at: 2026-02-23T06:04:44.884111+00:00
-- elapsed: 820ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.my_project.dim_products
-- query_id: 01c2994c-3202-5d57-0000-00140f72cfd9
-- desc: execute adapter call

    commit
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.my_project.dim_products", "profile_name": "my_project", "target_name": "MY_SCHEMA"} */;
-- created_at: 2026-02-23T06:04:44.886563+00:00
-- finished_at: 2026-02-23T06:04:45.077302+00:00
-- elapsed: 190ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.my_project.dim_products
-- query_id: 01c2994c-3202-5d57-0000-00140f72cfdd
-- desc: execute adapter call
drop view if exists MY_DB.MY_SCHEMA.dim_products__dbt_tmp cascade
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.my_project.dim_products", "profile_name": "my_project", "target_name": "MY_SCHEMA"} */;
