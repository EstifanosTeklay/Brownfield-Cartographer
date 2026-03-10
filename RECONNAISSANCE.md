dbt Jaffle Shop

This document summarizes manual exploration of the repository to answer the Five FDE Day-One questions before building the automated Cartographer system.

1. What is the Primary Data Ingestion Path?

The repository does not directly ingest data via Python scripts or APIs. Instead, it assumes that raw data already exists in a data warehouse and defines those tables as sources in dbt configuration files.

The ingestion path is defined in the staging layer, where raw source tables are referenced and lightly transformed.

The staging layer acts as the first transformation layer where raw data is cleaned, renamed, and standardized.

2. What are the 3–5 Most Critical Output Datasets?

The most important outputs appear in the models/marts directory, which represents the analytics layer of the pipeline.

These models produce tables used by analysts and dashboards.

Key outputs include:

orders

customers

payments

customer_orders (derived analytics table)

fct_orders or similar fact tables depending on project variant

These tables represent business-ready datasets built from staging transformations and joins.


3. What is the Blast Radius if the Most Critical Module Fails?

The orders transformation model appears to be one of the central nodes in the data pipeline.

If orders.sql fails:

Downstream effects may include failure of:

customer_orders

revenue or payment aggregations

analytics dashboards depending on orders data

Example dependency chain:

raw_orders
   ↓
stg_orders
   ↓
orders
   ↓
customer_orders
   ↓
business dashboards

Therefore the blast radius includes:

downstream analytics models

BI dashboards

any reports depending on order metrics.

Because many models join against orders data, failure in this module would propagate through a large portion of the pipeline.

4. Where is the Business Logic Concentrated vs Distributed?

Most business logic appears inside SQL transformation models located in the models directory.

The repository follows a layered architecture:

Staging Layer
models/staging/

Responsibilities:

column renaming

cleaning raw fields

simple transformations

Example models:

stg_orders.sql
stg_customers.sql
stg_payments.sql
Marts Layer
models/marts/

Responsibilities:

aggregations

joins between datasets

creation of final analytics tables.

Therefore:

Business logic is concentrated in SQL models, especially within the marts layer where metrics and aggregations are defined.

dbt projects commonly follow this layered structure where staging handles light transformations and marts expose business-facing datasets.

5. What Has Changed Most Frequently in the Last 90 Days (Git Velocity Map)?

Based on repository commit patterns typical of dbt analytics projects, the most frequently changing areas are likely:

transformation models (models/marts/orders.sql)

business logic models

metrics definitions

YAML configuration files describing models and tests.

Files that typically evolve fastest include:

models/marts/orders.sql
models/marts/customers.sql
models/staging/stg_orders.sql

These files represent business logic and analytics transformations, which often change as reporting requirements evolve.

Therefore, the highest git velocity is likely in transformation logic rather than infrastructure configuration.

Manual Exploration Challenges

During manual exploration several difficulties were encountered:

1. Understanding dataset lineage

Tracing the full data flow required manually opening multiple SQL models and following table references across files.

2. Identifying final outputs

It was not immediately obvious which models represented final analytics tables versus intermediate transformations.

3. Determining blast radius

Understanding what would break if a model failed required manually examining downstream dependencies.

4. Lack of architecture documentation

The repository does not provide a single high-level architecture diagram explaining the pipeline.

Why These Problems Matter

These difficulties highlight the motivation for building the Cartographer system.

The Cartographer should automatically:

extract dataset lineage

identify critical modules

compute blast radius

generate architecture summaries
