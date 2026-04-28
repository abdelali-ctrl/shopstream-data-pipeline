# ADR-004: Azure Blob Storage instead of AWS S3 for the data lake layer

- **Status:** Accepted
- **Date:** 2026-04-28
- **Deciders:** Abdelali Amassaghrou
- **Related:** ADR-001 (raw vs. staging naming)

## Context

The original ShopStream design used AWS S3 as the object-store landing zone
between PostgreSQL and Snowflake. The pipeline was:

```
PostgreSQL → export_to_s3.py (boto3) → S3 → COPY INTO (S3 stage) → Snowflake RAW
```

The S3 approach works, but it has two primary friction points in this setup —
one practical, one architectural — and a third secondary one:

1. **No AWS account.** AWS requires a valid account, a billing method, and
   confirmed identity. The author did not have an active AWS account at the
   time of v2 development.

2. **IAM complexity is disproportionate to the use case.** Connecting
   Snowflake to S3 requires: create a storage integration object → describe
   it to retrieve the Snowflake-managed IAM user ARN → switch to AWS IAM to
   configure a trust relationship on a role → grant that role the correct
   bucket permissions → return to Snowflake to finalise. That is a
   four-step, cross-console setup with non-trivial debugging when any step
   fails and no local equivalent for testing. For a pipeline where the only
   consumer of the object store is a single Snowflake account, this overhead
   is not justified. Azure Blob's SAS token collapses the entire auth story
   to: create a token in the portal, put it in an environment variable,
   reference it in one `CREDENTIALS` clause. This was a deliberate reason to
   prefer Azure, independent of the account availability issue.

3. **Cross-cloud overhead.** This project targets Snowflake on Azure (the
   account is on Azure), so routing data through AWS S3 and then into Azure
   adds an unnecessary cross-cloud hop with no architectural benefit at this
   scale.

Three alternatives were evaluated:

| Option | Auth mechanism | Snowflake support | Account required |
|---|---|---|---|
| AWS S3 | IAM role + trust policy | Native (storage integration) | AWS account |
| Azure Blob Storage | SAS token or connection string | Native (external stage) | Azure account (free tier) |
| Snowflake internal stage | Snowflake credentials only | Native (PUT command) | None beyond Snowflake |
| Cloudflare R2 | S3-compatible API + HMAC key | Via S3-compat. layer | Cloudflare account |

## Decision

Replace AWS S3 with **Azure Blob Storage** as the object-store landing zone.

The pipeline becomes:

```
PostgreSQL → export_to_azure_blob.py (azure-storage-blob) → Azure Blob → COPY INTO (Azure stage) → Snowflake RAW
```

Authentication uses a **container-scoped SAS token** with read, write, list,
and create permissions. The token is loaded from the environment at runtime;
no credentials are hardcoded.

The Snowflake external stage is defined as:

```sql
CREATE OR REPLACE STAGE RAW.azure_raw_stage
    URL = 'azure://<account>.blob.core.windows.net/<container>/raw/'
    CREDENTIALS = (AZURE_SAS_TOKEN = '<sas_token>');
```

The `run_snowflake_copy_into.py` script reads `snowflake_copy_into.sql`,
substitutes the `{{ partition_date }}` placeholder, and executes the COPY INTO
statements via `snowflake.connector` — replacing the previous manual
"run this SQL in a worksheet" step.

## Consequences

**Positive**

- No AWS account needed. Azure free tier (5 GB LRS storage) is more than
  sufficient for the demo dataset and months of daily partitions.
- SAS token auth is significantly simpler than the Snowflake–S3 IAM trust
  setup: one token, one environment variable, one stage definition.
- Snowflake on Azure → Azure Blob is same-cloud, removing the cross-cloud
  hop. Latency and potential egress costs are both reduced.
- `azure-storage-blob` SDK and `boto3` are equivalent in API ergonomics; the
  migration was a near-direct substitution with no logic changes.
- COPY INTO automation via `run_snowflake_copy_into.py` eliminates the manual
  worksheet step that existed in v1, making the load stage fully orchestrable
  by Airflow.

**Negative**

- S3 is the dominant object store in data engineering job descriptions and
  tooling. A reviewer assessing cloud breadth sees Azure instead. Mitigated
  by documenting the decision explicitly here and by the fact that the
  pipeline pattern (object store → COPY INTO → warehouse) is identical
  regardless of vendor.
- SAS tokens expire. A token that expires silently breaks the pipeline with a
  confusing auth error. Mitigation: token expiry date must be documented in
  the stage definition comment and in the `.env.example` header; rotate before
  expiry.
- No managed identity / service principal auth in this version. SAS token is
  appropriate for a personal demo project; a production deployment would use
  a service principal or managed identity with narrower permissions.

## Alternatives not chosen

**Snowflake internal stage.** This would eliminate the external cloud
dependency entirely — files are staged inside Snowflake itself via a `PUT`
command. Rejected for v2 because it removes the data lake layer from the
architecture, making the project less representative of real-world pipelines
where the object store is a durable, independently-queryable landing zone. It
remains the right choice for a single-warehouse setup with no downstream
consumers of the raw files. Documented as a v3.0 option.

**Cloudflare R2.** S3-compatible API, zero egress fees, free tier available.
Attractive, but Snowflake does not have a native R2 stage type — it requires
using the S3-compatible endpoint with HMAC keys, which adds a workaround layer.
The native Azure stage in Snowflake is cleaner. R2 would be the right call
if AWS compatibility mattered more than native Snowflake integration.

**Keep S3, get a free AWS account.** AWS Free Tier is available. Rejected
because the IAM role setup for Snowflake integration still requires effort, the
Snowflake account is on Azure anyway, and the engineering time was better spent
on the pipeline itself.

## Migration notes

Files changed in v2.0 (Azure migration):

- `scripts/export_to_s3.py` → deleted
- `scripts/export_to_azure_blob.py` → new
- `scripts/run_snowflake_copy_into.py` → new (load automation)
- `scripts/snowflake_copy_into.sql` → updated to Azure stage + `{{ partition_date }}` templating
- `scripts/snowflake_setup.sql` → Azure external stage definition added
- `airflow/dags/shopstream_pipeline_dag.py` → `extract_postgres_to_azure_blob` and `copy_azure_blob_to_snowflake` tasks
- `requirements.txt` → `boto3` replaced by `azure-storage-blob`; `snowflake-connector-python` added
