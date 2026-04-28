# `shopstream_dbt`

dbt project for the ShopStream data warehouse on Snowflake.

## Layers

```
sources (raw)  ->  staging  ->  core (dim_*, fact_*)  ->  marts (mart_*)
```

| Layer | Schema | Materialization | Purpose |
|---|---|---|---|
| Sources | `RAW` | tables (loaded by COPY INTO) | Landing zone. Mirrors PostgreSQL 1:1 + `_loaded_at`. |
| Staging | `STAGING` | view | Cleaning, typing, renaming. No business logic. |
| Core | `CORE` | table | Conformed dimensions and facts. Surrogate keys via `dbt_utils.generate_surrogate_key`. |
| Marts | `MARTS` | table | Business-ready: sales overview, customer LTV / RFM, product ABC. |

## Quick start

```bash
cd dbt_part/shopstream_dbt

# Install package dependencies (dbt_utils, dbt_expectations).
dbt deps

# Load seeds (product_margins.csv -> CORE.PRODUCT_MARGINS).
dbt seed

# Build everything: runs models then their tests in dependency order,
# fails fast if any test fails. This is the canonical pipeline command.
dbt build

# Or run / test separately during development:
dbt run --select staging
dbt test --select source:raw

# Generate and serve docs locally.
dbt docs generate
dbt docs serve
```

## Conventions

- **Naming.** Sources are `raw_<entity>`. Staging models are `stg_<entity>`. Dims are `dim_<entity>`. Facts are `fact_<grain>`. Marts are `mart_<purpose>`.
- **One YAML per folder.** `_sources.yml` declares external sources; `_models.yml` declares models, columns, descriptions, and tests.
- **Surrogate keys.** Built with `{{ dbt_utils.generate_surrogate_key([...]) }}`. The natural key is preserved on each dimension as a debug aid.
- **Schema names.** The default dbt behavior of prefixing schema names with the target schema is overridden via `macros/generate_schema_name.sql`, so models land in `STAGING`, `CORE`, `MARTS` directly.
- **Variables.** `start_date` (cutoff for fact_orders) and `premium_plans` (list of plan types treated as Premium) live in `dbt_project.yml`.

## Tests

Built-in dbt tests cover:
- `unique` and `not_null` on every primary key
- `relationships` for every FK between layers (sources → staging → core → marts)
- `accepted_values` on enums (order_status, plan_type, segments, abc_class)

`dbt build` fails the pipeline on any test failure. To run only tests:

```bash
dbt test
```

## ADRs

Architecture Decision Records live one level up in [`/docs/adr/`](../../docs/adr/). Read them in order; the first three explain the v2 layout.
