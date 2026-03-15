# FDE Day-One Onboarding Brief

**Repository:** `C:\Users\MOON\AppData\Local\Temp\brownfield_cartographer_repos\mitodl__ol-data-platform`
**Generated:** 2026-03-13 22:21 UTC
**Analysis:** 1126 modules, 981 datasets, 616 transformations

---

## Executive Summary

This codebase is a data pipeline with 1126 files, 371 data sources, and 269 output datasets. The most critical module is `src/ol_dbt/macros/apply_deduplication_query.sql` (highest PageRank). There are 0 circular dependencies.

---

*Semantic analysis not available. Run with Semanticist agent for full answers.*

## Quick Reference

**Data Sources:**
- `<spark_dynamic:147>`
- `<spark_dynamic:231>`
- `<spark_dynamic:321>`
- `<spark_dynamic:33>`
- `<spark_dynamic:393>`
- `<spark_dynamic:443>`
- `<spark_dynamic:44>`
- `<spark_dynamic:73>`
- `legacy_edx_certificate_revision_mapping`
- `platforms`

**Data Sinks:**
- `afact_video_engagement`
- `apply_deduplication_query`
- `apply_grants_macro_override`
- `cast_date_to_iso8601`
- `cast_timestamp_to_iso8601`
- `chatbot_usage_report`
- `check_cross_column_duplicates`
- `combined_enrollments_with_gender_and_date`
- `combined_video_engagements_counts_report`
- `cross_db_functions`

**Top Hub Modules:**
- `src/ol_dbt/macros/apply_deduplication_query.sql` (PageRank=0.0009)
- `src/ol_dbt/macros/apply_grants_macro_override.sql` (PageRank=0.0009)
- `src/ol_dbt/macros/cast_date_to_iso8601.sql` (PageRank=0.0009)
