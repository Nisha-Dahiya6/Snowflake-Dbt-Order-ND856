
Snowflake-Dbt Integration using Key Pair

**Database Objects created in Snowflake**
Database: MY_DB
Schema: MY_SCHEMA
RAW TABES: MY_SCHEMA.ORDERS, MY_SCHEMA.PRODUCTS, MY_SCHEMA.CUSTOMERS
FACT TABLES: MY_SCHEMA.FCT_ORDERS, MY_SCHEMA.FCT_ORDER_ITEMS
DIM TABLES: MY_SCHEMA.DIM_CUSTOMERS, MY_SCHEMA.DIM_PRODUCTS

WAREHOUSE: MY_COMPUTE

KEY PAIR:
- USER : DBT with public key
- ROLE : DBT_TRANSFORM (PROVIDE ACCESS TO ABOVE DB OBJECTS AND WAREHOUSE) AND ASSIGN THE ROLE TO USER DBT


**RAW SQLs**:
create table products (doc variant);
create table customers (doc variant);
create table orders (doc variant);

insert into products (doc)
SELECT PARSE_JSON('{
  "productId": "P-101",
  "sku": "SKU-101",
  "name": "Widget Pro",
  "category": {"l1": "Widgets", "l2": "Premium"},
  "active": true,
  "attributes": {"color": "black", "warrantyMonths": 12},
  "audit": {"updatedAt": {"$date": "2025-11-01T00:00:00Z"}}
}');

insert into customers (doc) 
select parse_json ('{"customerId":"C-9001",
  "region":"IN_WEST",
  "name":{"first":"Asha","last":"Mehta"},
  "contacts":{
    "emails":["asha@acme.com","asha.personal@gmail.com"],
    "phones":[{"type":"mobile","value":"+91-99999-11111"}]
  },
  "loyalty":{"tier":"Gold","points":1500},
  "audit":{"createdAt":{"$date":"2025-01-10T00:00:00Z"},"updatedAt":{"$date":"2025-12-01T00:00:00Z"}},
  "isDeleted": false
}');
  
insert into orders (doc) 
select parse_json('{
  "orderId": "OA-10001",
  "region": "IN_WEST",
  "customer": {
    "customerId": "C-9001",
    "name": "Asha Mehta"
  },
  "statusHistory": [
    {"status": "CREATED", "ts": "2025-12-10T10:00:00Z"},
    {"status": "PAID", "ts": "2025-12-10T10:05:00Z"}
  ],
  "items": [
    {
      "itemId": "I-1",
      "product": {"productId": "P-101"},
      "qty": 2,
      "pricing": {"unitPrice": 249.99, "currency": "INR"},
      "discounts": [{"type": "PCT", "value": 10}]
    }
  ],
  "audit": {
    "createdAt": "2025-12-10T10:00:00Z",
    "updatedAt": "2025-12-12T09:00:00Z"
  },
  "isDeleted": false
}');


