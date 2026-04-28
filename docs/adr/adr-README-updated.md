# Architecture Decision Records

Short, dated documents recording non-obvious choices in the ShopStream codebase. New ADRs are numbered sequentially. Status values: `Proposed`, `Accepted`, `Deprecated`, `Superseded by ADR-NNNN`.

## Index

| # | Title | Status | Date |
|---|---|---|---|
| [0001](./0001-raw-vs-staging-naming.md) | Raw vs. staging — naming and schema separation | Accepted | 2026-04-28 |
| [0002](./0002-surrogate-keys.md) | Surrogate keys on dimensions and facts | Accepted | 2026-04-28 |
| [0003](./0003-margin-via-seed.md) | Margin estimation via a versioned seed file | Accepted | 2026-04-28 |
| [0004](./0004-azure-blob-instead-of-s3.md) | Azure Blob Storage instead of AWS S3 for the data lake layer | Accepted | 2026-04-28 |

## Planned (not yet written)

- 0004: Incremental materialization for `fact_orders`
- 0005: CSV vs. Parquet for the Azure Blob landing layer
- 0006: Two warehouses (LOADING_WH + TRANSFORM_WH) — when does separation pay off?
- 0007: Observability — Slack on failure, cost dashboard from ACCOUNT_USAGE
- 0008: Re-introducing `events` (VARIANT) and `crm_contacts` (attribution) in v2.1

## Template

```markdown
# ADR-NNNN: <short imperative title>

- **Status:** Proposed | Accepted | Deprecated | Superseded by ADR-NNNN
- **Date:** YYYY-MM-DD
- **Deciders:** <name(s)>
- **Related:** ADR-NNNN, ADR-NNNN

## Context
What problem are we solving? What constraints exist?

## Decision
What we are doing, in one or two sentences.

## Consequences
Positive, negative, and neutral effects.

## Alternatives considered
What else we looked at, and why we rejected each.
```
