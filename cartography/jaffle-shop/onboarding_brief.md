# FDE Day-One Onboarding Brief

**Repository:** `D:\Projects\jaffle-shop`
**Generated:** 2026-03-15 22:30 UTC
**Analysis:** 33 modules, 21 datasets, 15 transformations

---

## Executive Summary

This codebase is a data pipeline with 33 files, 6 data sources, and 7 output datasets. The most critical module is `models/staging/stg_customers.yml` (highest PageRank). There are 0 circular dependencies.

---

## Q1: What is the primary data ingestion path?

Raw data enters through six source tables defined in 'models/staging/__sources.yml': raw_customers, raw_orders, raw_items, raw_stores, raw_products, and raw_supplies. These feed into five staging models (stg_customers.sql, stg_orders.sql, stg_order_items.sql, stg_locations.sql, stg_products.sql) which are the top hub modules by PageRank, indicating they are the highest-traffic transformation nodes. The staging layer applies standardization and naming conventions before passing data downstream to the marts layer.

---

## Q2: What are the 3-5 most critical output datasets?

The most critical output datasets are 'customers', 'orders', and 'order_items' based on their complexity and described downstream dependencies. 'customers' (models/marts/customers.sql) aggregates lifetime metrics and segmentation used for BI and marketing; 'orders' (models/marts/orders.sql) is described as the central fact table for order analytics; and 'order_items' (models/marts/order_items.yml) is described as 'the single source of truth for order item analytics.' Supporting dimensions 'locations' and 'products' round out the critical outputs as reusable dimension tables.

---

## Q3: What is the blast radius if the critical module fails?

The most critical module is 'models/staging/stg_orders.yml' (highest PageRank hub), and its failure would cascade to at minimum 'models/marts/orders.sql', 'models/marts/order_items.sql', and 'models/marts/customers.sql', which all depend on order data. Since 'customers.sql' derives lifetime purchase metrics from orders and 'order_items.sql' enriches order line items, a failure in stg_orders would effectively break the entire transactional analytics surface. This represents 3 of the 7 mart outputs and would disable customer segmentation, order analytics, and product performance reporting simultaneously.

---

## Q4: Where is business logic concentrated vs distributed?

Business logic is heavily concentrated in the marts layer, particularly in 'models/marts/orders.sql' (aggregates item-level costs, derives food/drink flags), 'models/marts/customers.sql' (calculates LTV and behavioral classifications), and 'models/marts/order_items.sql' (enriches with supply cost and product details). The 'macros/cents_to_dollars.sql' represents distributed reusable logic for monetary conversion used across multiple models. The staging layer (stg_*.sql files) is intentionally thin, handling only renaming and basic cleaning, meaning complex joins and metric derivation are deferred to and concentrated in the marts.

---

## Q5: What has changed most frequently in the last 90 days?

The mart definition YAML files ('models/marts/orders.yml', 'models/marts/customers.yml', 'models/marts/order_items.yml') are likely the highest-velocity pain points because they simultaneously own data quality tests, semantic model definitions, and metric declarations—three concerns that evolve at different rates. 'models/staging/__sources.yml' is also likely frequently changed as new raw sources or fields are onboarded, since it is the single registration point for all upstream data assets. 'macros/cents_to_dollars.sql' may be a pain point given its multi-platform database-specific implementations, which require updates whenever a new target platform is added.

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
- `models/staging/stg_customers.yml` (PageRank=0.0442)
- `models/staging/stg_locations.yml` (PageRank=0.0442)
- `models/staging/stg_order_items.yml` (PageRank=0.0442)
