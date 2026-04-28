# Changelog

## v2.0.0 — 2026-04-28

Audit-driven cleanup. Same architecture, materially better engineering.

### Architecture

- Schema `STAGING` renamed to `RAW`; landing tables prefixed `RAW_` (was `STG_`). The dbt staging layer now reserves the `STG_` prefix for itself. See [ADR-001](docs/adr/0001-raw-vs-staging-naming.md).
- All staging models now exist (`stg_users`, `stg_products`, `stg_orders`, `stg_order_items`); v1 only had `stg_orders`. Core models now read from staging via `ref()`, not directly from sources.
- Surrogate keys on every dimension and fact via `dbt_utils.generate_surrogate_key`. Natural keys preserved for traceability. See [ADR-002](docs/adr/0002-surrogate-keys.md).
- Per-category gross margin rates moved from a hard-coded `* 0.2` in `fact_orders` to a versioned seed (`seeds/product_margins.csv`). See [ADR-003](docs/adr/0003-margin-via-seed.md).
- Custom `generate_schema_name` macro so models land in `STAGING` / `CORE` / `MARTS` directly, not `<target>_<custom>`.

### Tests

- Source-level dbt tests on every raw table (unique, not_null, relationships, accepted_values where applicable).
- Model-level dbt tests on every staging, core, and mart model.
- Source freshness checks (`warn_after: 26h`, `error_after: 48h`) on the raw layer.
- `dbt_expectations` package added for richer tests in v2.1.

### Pipeline

- Airflow DAG: `dbt run` + `dbt test` consolidated into `dbt build --fail-fast`. A failing test now halts the DAG (was previously swallowed with a warning).
- DAG: `dbt deps` and `dbt seed` tasks added; `verify_snowflake_data` now raises on empty tables; SLAs added per task; `on_failure_callback` placeholder for Slack.
- COPY INTO: `ON_ERROR` changed from `CONTINUE` to `ABORT_STATEMENT` so malformed rows surface immediately.

### Scope

- Removed `events` and `crm_contacts` from v2.0 across PostgreSQL, Azure Blob export, Snowflake, and dbt sources. They were landed but never modeled in v1. Reintroducing them with proper VARIANT-flattening / attribution modeling is on the v2.1 backlog.

### Code quality

- All comments, log messages, and identifiers translated to English (was bilingual French/English in v1).
- Whitespace bugs fixed throughout (`df. to_csv`, `o. country`, etc.).
- Type hints added to Python files; deprecated `pd.read_sql` replaced with `pd.read_sql_query`.
- README claim "production-ready" replaced with accurate scope statement.

### Removed / cleaned

- Four unused empty dbt directories with `.gitkeep` markers retained where dbt requires them.
- Default dbt-init starter README replaced with project-specific docs.
- Hard-coded date `2025-12-16` in `snowflake_copy_into.sql` replaced with a `<YYYY-MM-DD>` placeholder.

## v1.0.0 — initial

Initial pipeline: PostgreSQL → Azure Blob → Snowflake → dbt → Power BI. Tutorial-grade scaffolding suitable for academic submission.
