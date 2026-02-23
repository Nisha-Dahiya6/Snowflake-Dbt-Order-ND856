<img width="468" height="54" alt="image" src="https://github.com/user-attachments/assets/a0eb9f77-5f14-4819-9c59-f9ddb5e993a4" /><img width="468" height="54" alt="image" src="https://github.com/user-attachments/assets/3e6ef771-6b15-4b1f-9203-e63450272d08" /># Snowflake-Dbt-Order-ND856
Case Study for Snowflake-Dbt Order, Customer, Products Analytics Warehouse with Incremental Processing and Data Quality Controls.

<img width="895" height="479" alt="Screenshot 2026-02-23 at 4 52 57 PM" src="https://github.com/user-attachments/assets/17dba154-baa5-47b9-b665-ac32cb0a826d" />

Context:
You are working with operational data stored in MongoDB. Each region has its own MongoDB database. Data is landed into Snowflake as raw JSON (VARIANT) tables and must be transformed using dbt for reporting in Power BI.

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


🧪 Testing

- order_id uniqueness in fct_orders
- Referential integrity between fct_orders and dimensions
- Freshness of order_updated_at

1. How would you handle deletes from MongoDB?
2. How would this design scale for very large volumes?
3. Where would you implement Power BI RLS and why?




🧪 Cost, Performance & Scalability

•	How do you manage Snowflake cost and performance?
•	How does the architecture scale with increased data volume or users?
•	What monitoring or alerting would you put in place?





🧪 Changes Scenario (Design Evolution)

Scenario: Six months after launch: - Data volume has tripled - A new data source containing sensitive customer data is added - Business users are requesting near-real-time dashboards
How would you adapt or evolve your architecture to support these changes?






