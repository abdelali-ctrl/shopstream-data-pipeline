# ADR-003: Margin estimation via a versioned seed file

- **Status:** Accepted
- **Date:** 2026-04-28
- **Deciders:** Abdelali Amassaghrou

## Context

The `mart_sales_overview` and `mart_product_performance` marts both expose a `total_margin` and `margin_rate`. In v1, this came from a hard-coded multiplier inside `fact_orders.sql`:

```sql
oi.line_total * 0.2 AS estimated_margin
```

This silently produced a "margin" number that:

- Was identical for every product, regardless of category.
- Was undocumented — no reader of the mart would know it was 20% by fiat.
- Could not be revised without a code change and a PR review.
- Carried no audit trail of who chose 0.2 or when.

Three real options exist for handling margin in a warehouse:

1. **Pull a real margin field from the source system.** Best when the source has it.
2. **Maintain the rate in the warehouse as a versioned reference table.**
3. **Compute it analytically** (e.g., revenue minus a derived COGS pulled from a procurement system).

ShopStream has no procurement system and no margin field in PostgreSQL. Option 1 and 3 require source-system changes outside the scope of v2. So the question is: where does the rate live?

## Decision

Maintain a **per-category gross margin rate** in a dbt seed file:

```
seeds/product_margins.csv
```

| product_category | gross_margin_rate |
|---|---|
| electronics | 0.18 |
| clothing | 0.45 |
| books | 0.30 |
| ... | ... |

`dim_products` joins on this seed and exposes `gross_margin_rate` as a dimension column. `fact_orders` computes `estimated_margin = line_revenue * gross_margin_rate` per line, using the *product's* category rate, not a flat 20%.

When a category is missing from the seed, the model falls back to `0.20` and surfaces no error — this is deliberate: we want the pipeline to keep running, but the wrong-but-flagged number should be noticeable in the mart QA.

## Consequences

**Positive**

- The number is reviewable: `git log seeds/product_margins.csv` answers "who set books at 30%, and when?"
- Finance can update margin rates by editing a CSV and opening a PR — no SQL skills required.
- Per-category granularity makes the mart numbers directionally meaningful, not just uniform.
- Tests on the seed (`accepted_range: 0..1`, `unique` on category) prevent typos from breaking the warehouse.

**Negative**

- The rate is still an estimate. It does not replace a real COGS feed.
- Two systems of record now exist for "what's the margin on category X?" — the seed and whatever finance calls the truth. Mitigation: name the column `gross_margin_rate` (specific) and document the seed as `Sourced manually from finance estimates`.

## Migration path

When ShopStream onboards a real procurement / COGS feed, the move is:

1. Land the feed in `RAW.RAW_PRODUCT_COSTS`.
2. Build `stg_product_costs.sql` to clean and rename.
3. Replace the seed join in `dim_products` with a join to `stg_product_costs`, calculating margin per product (not per category).
4. Delete the seed and this ADR's "deferred" status, recording the migration in a successor ADR.

This is a one-day change with the seed pattern in place.
