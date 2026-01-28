# Architecture Overview

## Goal
Build an end-to-end insurance (Guidewire-like) data pipeline on Azure using Databricks and Snowflake with orchestration, IaC, and CI/CD.

## Data Flow (High Level)
1. Source extracts (CSV/JSON) arrive from enterprise systems (simulated Guidewire exports).
2. Files are landed in Azure Blob Storage / ADLS Gen2 in a date-partitioned folder structure.
3. Databricks ingests raw files into **Bronze** Delta tables (as-is + metadata).
4. Databricks transforms into **Silver** Delta tables (cleaned, typed, deduped).
5. Databricks creates **Gold** tables (facts/dimensions for analytics).
6. Gold outputs are loaded into **Snowflake** curated schemas and marts for reporting/BI.

## Layers
- **Raw**: Files in Blob/ADLS (immutable landing).
- **Bronze**: Raw Delta tables (minimal changes + ingestion metadata).
- **Silver**: Cleaned, standardized, deduplicated tables (business-ready).
- **Gold**: Analytics-ready facts and dimensions (star schema style).
- **Snowflake Marts**: Serving layer for BI/reporting and downstream consumers.

## Orchestration (ADF)
Azure Data Factory orchestrates:
- File arriva checks
- Databricks job execution (Bronze → Silver → Gold)
- Snowflake load steps (COPY/MERGE or task execution)
- Notifications / logging

## Infrastructure as Code (Terraform)
Terraform provisions and manages:
- Storage (Blob/ADLS containers)
- ADF resources (pipelines, triggers - if managed as code)
- Key Vault / secrets (recommended)
- Optional Databricks workspace resources

## CI/CD (GitHub Actions / Azure DevOps)
CI/CD pipeline performs:
- Code validation (lint/format)
- Terraform validation/plan checks
- Deploy notebooks/pipeline definitions
- Run smoke tests (optional)
- Promote changes via PR workflow to main branch

## Non-Functional Requirements (Targets)
- Idempotent reruns (safe to rerun for a gven date)
- Incremental loads (MERGE/upserts)
- Basic data quality checks (null/dup/referential)
- Monitoring/audit logging (run status, row counts, duration)
