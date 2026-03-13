# FDE Day-One Onboarding Brief

**Repository:** `D:\Projects\jaffle-shop`
**Generated:** 2026-03-13 20:42 UTC
**Analysis:** 33 modules, 21 datasets, 15 transformations

---

## Executive Summary

This codebase is a data pipeline with 33 files, 6 data sources, and 7 output datasets. The most critical module is `macros/cents_to_dollars.sql` (highest PageRank). There are 0 circular dependencies.

---

## Q1: What is the primary data ingestion path?

Raw data enters the pipeline through six source tables defined in models/staging/__sources.yml: raw_customers, raw_orders, raw_items, raw_stores, raw_products, and raw_supplies. These sources flow through staging models (e.g., models/staging/stg_customers.sql) configured as views per dbt_project.yml, then materialize as tables in the marts layer (models/marts/). The macros/generate_schema_name.sql controls schema routing across environments, separating seeds into a dedicated schema and prefixing production schemas.

---

## Q2: What are the 3-5 most critical output datasets?

The 5 most critical output datasets are models/marts/customers.sql, models/marts/orders.sql, models/marts/order_items.sql, models/marts/locations.sql, and models/marts/products.sql. customers.sql is confirmed as a top hub by PageRank and serves as the central customer dimension with lifetime value metrics; orders.sql and order_items.sql form the core transactional fact tables powering revenue and profitability analytics. locations.sql is also a top-PageRank hub, and products.sql anchors product-dimension joins across the organization.

---

## Q3: What is the blast radius if the critical module fails?

If models/marts/customers.sql fails (the highest PageRank mart node), all downstream customer segmentation, lifetime value reporting, and analytics dashboards lose their single source of truth for customer data. Its customers.yml defines reusable metrics and semantic definitions consumed by MetricFlow and BI tools, meaning metric queries and dashboards built on the semantic layer would also break. Additionally, since stg_customers.sql feeds customers.sql, a failure cascades back to raw_customers ingestion validation as well.

---

## Q4: Where is business logic concentrated vs distributed?

Business logic is heavily concentrated in the marts layer, specifically in models/marts/customers.sql (lifetime metrics, behavioral classification), models/marts/orders.sql (order totals, category counts, boolean flags, sequence numbering), and models/marts/order_items.sql (profitability enrichment). Distributed utility logic lives in macros/cents_to_dollars.sql and macros/generate_schema_name.sql, which are cross-cutting concerns applied pipeline-wide. The staging layer (e.g., models/staging/stg_customers.sql) is intentionally thin, handling only renaming and field selection without substantive business rules.

---

## Q5: What has changed most frequently in the last 90 days?

models/marts/customers.yml and models/marts/orders.yml are likely high-velocity pain points because they define both data quality tests and semantic metric definitions, meaning they must change whenever business definitions, KPIs, or segmentation rules evolve. models/marts/order_items.sql and models/marts/orders.sql are also likely frequent change targets as they aggregate financial and categorical metrics that respond to new product types or pricing logic. macros/cents_to_dollars.sql is a high-risk shared dependency since any change to it propagates to all financial calculations across the pipeline.

---

## Quick Reference

**Data Sources:**
- `raw_customers`
- `raw_items`
- `raw_orders`
- `raw_products`
- `raw_stores`
- `raw_supplies`

**Data Sinks:**
- `cents_to_dollars`
- `customers`
- `generate_schema_name`
- `locations`
- `metricflow_time_spine`
- `products`
- `supplies`

**Top Hub Modules:**
- `macros/cents_to_dollars.sql` (PageRank=0.0312)
- `macros/generate_schema_name.sql` (PageRank=0.0312)
- `models/marts/customers.sql` (PageRank=0.0312)
