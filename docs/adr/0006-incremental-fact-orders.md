# ADR-006: Incremental materialization for fact_orders

- **Status:** Accepted
- **Date:** 2026-05-04
- **Deciders:** Abdelali Amassaghrou
- **Related:** ADR-002 (surrogate keys), ADR-003 (margin via seed)

## Context

`fact_orders` was materialized as a `table` in v2.0. On each `dbt build` run,
dbt drops and recreates the entire table from scratch. This is the simplest
possible materialization and correct for small datasets.

The question is whether to keep it as a table or switch to `incremental`.

At demo scale (~10k–50k rows), the difference in runtime is negligible. The
decision is therefore driven by two other factors:

1. **Signal value.** Senior data engineers are expected to know when and why
   to use incremental models, and to have implemented them in a real project.
   A project where every model is `table` or `view` does not demonstrate this.

2. **Correctness at scale.** As the dataset grows (100k+ rows, 1–2 years of
   daily data), a full table rebuild becomes progressively more expensive:
   it processes rows that haven't changed and consumes Snowflake credits
   proportional to total row count rather than incremental row count.

## Decision

Change `fact_orders` materialization from `table` to `incremental`
with `unique_key='order_line_key'`.

### Incremental strategy

```sql
{% if is_incremental() %}
where o.order_at > (select max(order_timestamp) from {{ this }})
{% else %}
where o.order_at >= '{{ var("start_date") }}'
{% endif %}
```

On the first run (full refresh): loads all rows from `start_date` onward.
On subsequent runs: loads only rows with `order_at` later than the most
recent `order_timestamp` already in the table.

Snowflake uses `MERGE` under the hood when `unique_key` is specified:
existing rows with a matching key are updated; new rows are inserted.

### Why `order_timestamp` as the watermark

`order_at` (sourced as `order_timestamp` in the fact) is an immutable
event timestamp — it represents when the order was placed and does not
change after creation. Using it as the watermark means:

- We only process orders placed since the last run.
- Late-arriving rows (order placed before the watermark but loaded late
  to RAW) will be missed. This is an accepted tradeoff at this scale.
  If late arrivals become a problem, switch the watermark to
  `_loaded_at` from the RAW layer.

### `on_schema_change = 'fail'`

Set to `'fail'` so that adding or removing columns to the model produces
a CI/build failure rather than silent schema drift. Forces an explicit
`dbt run --full-refresh` when the schema changes.

## Consequences

**Positive**

- At scale, incremental builds are substantially cheaper and faster than
  full table rebuilds (processes ~1 day of data instead of all history).
- Demonstrates incremental modeling competency in the portfolio project.
- The `_dbt_updated_at` audit column (already present in v2.0) records when
  each row was last processed, enabling auditability.
- `dbt_expectations.expect_column_values_to_be_between` tests on
  `line_revenue` and `quantity_sold` remain valid for incremental runs.

**Negative**

- A full refresh is required if: (a) the model schema changes, (b) upstream
  logic changes in a way that affects historical rows, (c) the watermark
  value is incorrect. This adds a manual step on schema migrations.
- Late-arriving orders (placed before the watermark, loaded after) are
  silently dropped. Acceptable for this dataset because source data is
  synthetic and append-only.
- Slightly more complex to reason about than `table` materialization.
  Documented here so future contributors understand the watermark logic.

## Alternatives considered

**Keep `table`:** Simple, always correct, no late-arrival problem.
Rejected for the signal and cost-at-scale reasons above.

**Incremental with `_loaded_at` watermark:** Catches late-arriving rows by
filtering on when the row landed in RAW rather than when the order was placed.
More robust but requires `_loaded_at` to be reliably populated in the RAW layer.
It is currently populated (`_loaded_at` is set by COPY INTO), so this is a viable
upgrade path if late arrivals become an issue.

**Snapshots (SCD Type 2) on dim_customers:** A separate question from
fact_orders materialization. Noted here because the v2.0 dimensions are
Type 1 (overwrite). SCD Type 2 on `dim_customers` is a Phase 2 enhancement
if historical customer state tracking is needed.
