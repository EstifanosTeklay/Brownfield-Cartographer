# FDE Day-One Onboarding Brief

**Repository:** `C:\Users\MOON\AppData\Local\Temp\brownfield_cartographer_repos\DataTalksClub__data-engineering-zoomcamp`
**Generated:** 2026-03-13 19:28 UTC
**Analysis:** 146 modules, 41 datasets, 30 transformations

---

## Executive Summary

This codebase is a data pipeline with 146 files, 19 data sources, and 15 output datasets. The most critical module is `04-analytics-engineering/taxi_rides_ny/models/marts/reporting/fct_monthly_zone_revenue.sql` (highest PageRank). There are 0 circular dependencies.

---

*Semantic analysis not available. Run with Semanticist agent for full answers.*

## Quick Reference

**Data Sources:**
- `./.dlt/secrets.toml`
- `<dynamic:16>`
- `<dynamic:18>`
- `<spark_dynamic:16>`
- `<spark_dynamic:22>`
- `<spark_dynamic:28>`
- `<spark_dynamic:30>`
- `<spark_dynamic:34>`
- `<spark_dynamic:36>`
- ```

**Data Sinks:**
- `<spark_write_dynamic:110>`
- `append`
- `bigquery_external_table_task`
- `dim_vendors`
- `fct_monthly_zone_revenue`
- `format_to_parquet_task`
- `get_trip_duration_minutes`
- `get_vendor_data`
- `ingest_task`
- `overwrite`

**Top Hub Modules:**
- `04-analytics-engineering/taxi_rides_ny/models/marts/reporting/fct_monthly_zone_revenue.sql` (PageRank=0.0126)
- `04-analytics-engineering/taxi_rides_ny/models/staging/stg_green_tripdata.sql` (PageRank=0.0126)
- `04-analytics-engineering/taxi_rides_ny/models/staging/stg_yellow_tripdata.sql` (PageRank=0.0126)
