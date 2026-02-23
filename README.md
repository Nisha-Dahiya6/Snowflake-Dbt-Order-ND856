# Snowflake-Dbt-Order-ND856
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

**Transformation Layer (Modelling in dbt)**
Bronze: Raw JSON ingestion - Model: STG
Silver: Flattened and normalized relational - models: FCT, DIM
Gold: Business-ready reporting tables - models: PUB

**Consumption Layer**
Power BI dashboards built on curated Gold models






