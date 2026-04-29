# ADR-001: Raw vs. staging — naming and schema separation

- **Status:** Accepted
- **Date:** 2026-04-28
- **Deciders:** Abdelali Amassaghrou
- **Related:** ADR-002 (surrogate keys)

## Context

In v1, the Snowflake schema that received CSV files from S3 was called `STAGING`, and its tables were prefixed `STG_` (`STG_USERS`, `STG_ORDERS`, ...). The dbt staging models were also prefixed `stg_` (`stg_orders.sql`).

This created a conflict: the dbt `source('staging', 'stg_orders')` lookup pointed at a table named `stg_orders` while the dbt model was *also* called `stg_orders`. Two different artifacts shared one identifier across two layers. The downstream symptom was that `dim_customers`, `dim_products`, and `fact_orders` all reached past the staging models and read directly from `source(...)`, because the staging layer wasn't materially distinguishable from raw.

## Decision

Two changes:

1. **Rename the landing schema** `STAGING` → `RAW`, and **rename the landing tables** `STG_*` → `RAW_*`.
2. **Reserve the `staging` schema and `stg_` prefix exclusively for dbt models.**

The pipeline now reads:

```
PostgreSQL  →  Azure Blob  →  RAW.RAW_*  →  STAGING.STG_*  →  CORE.{DIM,FACT}_*  →  MARTS.MART_*
                       (table)        (view)              (table)              (table)
```

Each layer has a distinct purpose and a distinct naming convention. There is no overload.

## Consequences

**Positive**

- A reader of any model can tell instantly which layer they're in, because the schema name and the prefix agree.
- The dbt staging layer regains its purpose: cleaning, typing, renaming. Core models are now forced to consume staging models (via `ref()`), not raw sources, which is the convention dbt assumes throughout.
- Source-level dbt tests (in `_sources.yml`) live alongside the raw tables they protect, while model-level tests live alongside the staging views they protect — the two test suites stop overlapping.

**Negative**

- One-time migration cost: every consumer of the warehouse (Power BI semantic model, ad-hoc queries, the verify scripts) needs schema names updated. Mitigated by doing this before any production user is onboarded.
- A new contributor coming from a project where "staging" means "raw" has to learn the convention. This is the standard dbt convention; the friction is one-time.

## Alternatives considered

- **Keep `STAGING` for raw, rename dbt models from `stg_` to something else (e.g., `cln_`, `clean_`).** Rejected: every dbt project on the planet uses `stg_` for staging models; deviating from convention costs more than the rename.
- **Keep both names and document the overload.** Rejected: documentation is not a substitute for unambiguous identifiers.
