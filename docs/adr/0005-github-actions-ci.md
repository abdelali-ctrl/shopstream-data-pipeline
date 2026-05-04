# ADR-005: GitHub Actions CI pipeline

- **Status:** Accepted
- **Date:** 2026-05-04
- **Deciders:** Abdelali Amassaghrou

## Context

ShopStream v2.0 ships with a solid local development story (`dbt build --fail-fast`,
comprehensive dbt tests, Docker Compose) but has no automated checks on pull requests.
This means:

- A merge could introduce Python style regressions that aren't caught until the next
  local run.
- A merge could introduce a SQL model that fails dbt tests, breaking the daily Airflow
  DAG silently until the next morning.
- There is no machine-readable record of whether the pipeline was green at a given
  commit.

The immediate trigger is the Phase 1 roadmap goal: *production-grade Shopstream, where
a hiring manager reads the README and thinks "I'd hire this person."* A project without
CI is a portfolio project. A project with CI is a production system.

## Decision

Introduce a two-job GitHub Actions workflow (`.github/workflows/ci.yml`):

**Job 1 — `lint`** (no credentials, runs in ~60 seconds):
- `ruff check` on all Python files: enforces style, catches unused imports and common
  errors without running the code.
- `sqlfluff lint` on all dbt SQL models: enforces capitalisation policy, trailing comma
  style, and indentation — the same rules already followed manually in the models.

**Job 2 — `dbt-ci`** (runs after lint passes):
- `dbt deps` → `dbt seed` → `dbt build --fail-fast` against a dedicated
  `SHOPSTREAM_CI` database in the same Snowflake account.
- Credentials are stored as GitHub Actions secrets, never hardcoded.
- CI schemas (`STAGING`, `CORE`, `MARTS`) are dropped at the end of every run
  so each PR starts with a clean slate.

### CI database isolation strategy

The existing `generate_schema_name` macro (ADR note in `macros/`) uses the
`custom_schema_name` directly, producing identical schema names in all environments.
Isolation is therefore achieved at the **database** level:

| Environment | Snowflake database | Schemas |
|---|---|---|
| Production | `SHOPSTREAM_DWH` | `STAGING`, `CORE`, `MARTS` |
| CI (GitHub Actions) | `SHOPSTREAM_CI` | `STAGING`, `CORE`, `MARTS` |
| Local dev | `SHOPSTREAM_DWH` (dev profile) | same |

`SHOPSTREAM_CI` is created once:
```sql
CREATE DATABASE IF NOT EXISTS SHOPSTREAM_CI;
GRANT ALL ON DATABASE SHOPSTREAM_CI TO ROLE TRANSFORMER;
```

The `ci/profiles.yml` is checked into the repo (credentials come from env vars)
and referenced with `--profiles-dir ../../ci` in the workflow.

### What sqlfluff checks

Rules enabled: keyword capitalisation (UPPER), identifier capitalisation (lower),
function capitalisation (UPPER), trailing commas, line length ≤ 120, indentation
4 spaces. These match the existing model style and will enforce consistency on
any future model additions.

The dbt templater is used so `{{ ref() }}`, `{{ config() }}`, and Jinja blocks are
resolved before linting. Without this, every Jinja expression is a syntax error.

## Consequences

**Positive**

- Every PR gets automated feedback within ~3 minutes (lint) or ~8 minutes (full dbt
  build) — no manual `dbt build` required before merge.
- The green checkmark on commits is a credibility signal in the GitHub repo.
- sqlfluff gradually enforces consistent SQL style across future model additions
  without requiring manual review comments.
- The `ci/profiles.yml` pattern documents exactly how to wire dbt to Snowflake in
  any environment, which is useful for onboarding.

**Negative**

- The `dbt-ci` job requires a live Snowflake connection. If Snowflake is down or the
  CI credentials expire, the pipeline is blocked. Mitigation: monitor credential
  expiry; consider a separate low-privilege CI service account.
- Running `dbt build` in CI consumes Snowflake credits. At demo data volumes this is
  negligible (< $0.05/run on an X-Small warehouse), but worth tracking if the dataset
  grows.
- sqlfluff with the dbt templater is slower than plain lint tools (~30s for this
  model count) and can produce false positives on complex Jinja. The `ignore_templated_areas`
  flag is set to reduce noise.

## Alternatives considered

**lint-only CI (no dbt build):** Fast and free, but misses the most important failure
mode — a model that lints clean but produces wrong or empty output. Rejected because
the whole point of CI on a data pipeline is to catch data-layer failures, not just
formatting.

**dbt Cloud CI:** The managed equivalent. Cleaner UX, but requires a dbt Cloud account
and monthly cost. GitHub Actions is free for public repos and more transparent for a
portfolio project.

**Per-PR schema isolation (CI_<run_id>):** Creates a fresh schema per run, eliminates
any risk of concurrent run conflicts. Adds complexity (schema name must be dynamic,
which conflicts with the current macro). Deferred to a future version if concurrent
PR runs become a problem.
