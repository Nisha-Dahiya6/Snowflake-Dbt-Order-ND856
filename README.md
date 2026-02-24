**Snowflake-Dbt-Order-ND856**
Case Study for Snowflake-Dbt Order, Customer, Products Analytics Warehouse with Incremental Processing and Data Quality Controls.

<img width="895" height="479" alt="Screenshot 2026-02-23 at 4 52 57 PM" src="https://github.com/user-attachments/assets/17dba154-baa5-47b9-b665-ac32cb0a826d" />

Context:
While working with operational data stored in MongoDB, each region has its own MongoDB database. Data is landed into Snowflake as raw JSON (VARIANT) tables and must be transformed using dbt for reporting in Power BI.

Key challenges:
- Nested and complex JSON structures
- Schema drift over time
- Late-arriving updates
- Soft deletes (isDeleted flag)

**Ingestion Layer** : RAW in Snowflake

Raw JSON data loaded into Snowflake as VARIANT columns
**See Path**: Snowflake/README.md

**Transformation Layer (Modelling in dbt)**

Bronze: Raw JSON ingestion - Model: STG
**See Path**: models/stg

Silver: Flattened and normalized relational - models: FCT, DIM
**See Path**: models/fct, models/dim

Gold: Business-ready reporting tables - models: PUB

**Consumption Layer**
Power BI dashboards built on curated Gold models


**🧪 Testing**

- order_id uniqueness in fct_orders
  🚀 in fct_orders configured unique key.
  
- Referential integrity between fct_orders and dimensions
  🚀 in fct_orders.yml file implemented tests - not_null and relationships.
    1. not_null: in fct_order table - column order_id, cusstomer_id, product_id
    2. relationships:
         => fct_order.customer_id references to dim_customers.customer_id
         => fct_order.product_id references to dim_products.product_id
       
  
- Freshness of order_updated_at
  🚀 in fct_orders and fct_order_items used is_incremental() function for full load for first time and delta load for subsequent

1. How would you handle deletes from MongoDB?
   
   ✅ Data Ingestion from MongoDB to Snowflake lands in Raw layer, so it is full load and snapshot of source. When deletes receive from source, isDeleteFlag can be updated to True in Silver layer.
   
2. How would this design scale for very large volumes?
   
   ✅ Materailize the fct and dim models as incremental. Full load is very expensive for large tables.
   ✅ Implement cluster key by Region
   ✅ Avoid Row Explosion while processing Semi-structure data.
   
3. Where would you implement Power BI RLS and why?
   ✅ In Snowflake we can create Secure views, Row access policies and setup RBAC.


**🧪 Cost, Performance & Scalability**

•	How do you manage Snowflake cost and performance?

🚀 Micro-partioning and Cluster key in fct_order
🚀 incremental_strategy as merge, and for very large tables insert_overwrite

•	How does the architecture scale with increased data volume or users?

🚀 As MongoDB is region based, therefore can ingest data parallely region wise
🚀 FLATTEN in silver layer and Avoid repeated LATERAL FLATTEN for semi structure data.
🚀 In Snowflake, Computation and Storage are isolated, warehouse performance optimization for concurrent users using multi-cluster warehouse.
🚀 Incremental processing and cluster key - region, created_at


•	What monitoring or alerting would you put in place?

🚀 DBT: dbt tests (not_null, unique, referential integrity)
🚀 Snowflake: Can setup Resource Monitor if exceed thresold limit

**🧪 Changes Scenario (Design Evolution)**

Scenario: Six months after launch: - Data volume has tripled - A new data source containing sensitive customer data is added - Business users are requesting near-real-time dashboards
How would you adapt or evolve your architecture to support these changes?

Answer: In DBT: in schema.yml file, apply governance tags.

              columns:
                - name: customer_name
                  tests:
                    - not_null
                  meta:
                    contains_pii: true
                    sensitivity: high


        In Snowflake: DYnamic data masking for sensitive customer data.


For data volume triple requirement, we can move from batch to continuous data processing and use snowpipe or Dynamic tables.

**TECHNOLOGIES/TOOLS USED:**

1. SNOWFLAKE
2. DBT FUSION
3. VSCODE
4. GITHUB

