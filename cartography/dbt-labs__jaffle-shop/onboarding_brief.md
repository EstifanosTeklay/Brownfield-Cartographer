# FDE Day-One Onboarding Brief

**Repository:** `C:\Users\MOON\AppData\Local\Temp\brownfield_cartographer_repos\dbt-labs__jaffle-shop`
**Generated:** 2026-03-13 20:52 UTC
**Analysis:** 33 modules, 21 datasets, 15 transformations

---

## Executive Summary

This codebase is a data pipeline with 33 files, 6 data sources, and 7 output datasets. The most critical module is `macros/cents_to_dollars.sql` (highest PageRank). There are 0 circular dependencies.

---

*Semantic analysis not available. Run with Semanticist agent for full answers.*

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
