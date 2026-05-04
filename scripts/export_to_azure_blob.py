"""
export_to_azure_blob.py

Extract every transactional table from PostgreSQL and upload it to Azure Blob
Storage as a date-partitioned CSV. Designed to be invoked by Airflow once per day.

Output layout:
    azure://<account>.blob.core.windows.net/<container>/raw/postgres/<table>/<YYYY-MM-DD>/<table>_<YYYYMMDD>.csv

Notes:
    - Idempotent: a re-run for the same day overwrites that day's files.
    - Empty tables raise an error instead of silently uploading empty files.
    - Authentication supports either AZURE_STORAGE_CONNECTION_STRING or
      AZURE_STORAGE_ACCOUNT + AZURE_STORAGE_SAS_TOKEN.
"""

from __future__ import annotations

import logging
import os
from datetime import UTC, datetime
from io import StringIO

import pandas as pd
import psycopg2
from azure.storage.blob import BlobServiceClient, ContentSettings
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("ShopStream.ExportAzureBlob")

load_dotenv()

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DB_CONFIG = {
    "host": os.environ.get("POSTGRES_HOST", "localhost"),
    "port": int(os.environ.get("POSTGRES_PORT", 5432)),
    "database": os.environ.get("POSTGRES_DB", "shopstream"),
    "user": os.environ.get("POSTGRES_USER", "postgres"),
    "password": os.environ.get("POSTGRES_PASSWORD", ""),
}

AZURE_CONFIG = {
    "account": os.environ.get("AZURE_STORAGE_ACCOUNT", ""),
    "container": os.environ.get("AZURE_STORAGE_CONTAINER", "shopstream-datalake"),
    "sas_token": os.environ.get("AZURE_STORAGE_SAS_TOKEN", ""),
    "connection_string": os.environ.get("AZURE_STORAGE_CONNECTION_STRING", ""),
}

TABLES = ("users", "products", "orders", "order_items")


# ---------------------------------------------------------------------------
# Clients
# ---------------------------------------------------------------------------
def open_pg() -> psycopg2.extensions.connection:
    logger.info(
        "Connecting to PostgreSQL %s:%s/%s",
        DB_CONFIG["host"],
        DB_CONFIG["port"],
        DB_CONFIG["database"],
    )
    return psycopg2.connect(**DB_CONFIG)


def open_blob_container():
    container_name = AZURE_CONFIG["container"]
    if not container_name:
        raise RuntimeError("AZURE_STORAGE_CONTAINER is not set")

    if AZURE_CONFIG["connection_string"]:
        logger.info(
            "Connecting to Azure Blob using connection string, container=%s", container_name
        )
        service = BlobServiceClient.from_connection_string(AZURE_CONFIG["connection_string"])
    else:
        account = AZURE_CONFIG["account"]
        sas_token = AZURE_CONFIG["sas_token"]
        if not account:
            raise RuntimeError("AZURE_STORAGE_ACCOUNT is not set")
        if not sas_token:
            raise RuntimeError("AZURE_STORAGE_SAS_TOKEN is not set")

        account_url = f"https://{account}.blob.core.windows.net"
        logger.info("Connecting to Azure Blob account=%s, container=%s", account, container_name)
        service = BlobServiceClient(account_url=account_url, credential=sas_token)

    container = service.get_container_client(container_name)
    container.get_container_properties()  # smoke-test auth and container existence
    return container


# ---------------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------------
def export_table(
    pg_conn: psycopg2.extensions.connection,
    container_client,
    table: str,
    partition_date: str,
) -> int:
    """Export one table to Azure Blob as CSV. Returns row count exported."""
    logger.info("Exporting %s ...", table)
    df = pd.read_sql_query(f"SELECT * FROM {table}", pg_conn)

    if df.empty:
        raise RuntimeError(f"Source table '{table}' is empty; refusing to upload an empty file")

    buf = StringIO()
    df.to_csv(buf, index=False)

    blob_name = (
        f"raw/postgres/{table}/{partition_date}/{table}_{partition_date.replace('-', '')}.csv"
    )
    container_client.upload_blob(
        name=blob_name,
        data=buf.getvalue(),
        overwrite=True,
        content_settings=ContentSettings(content_type="text/csv"),
    )
    logger.info(
        "Uploaded azure blob %s/%s (%d rows)", AZURE_CONFIG["container"], blob_name, len(df)
    )
    return len(df)


def main() -> None:
    partition_date = os.environ.get("EXECUTION_DATE") or datetime.now(UTC).strftime("%Y-%m-%d")
    logger.info("=== Export PostgreSQL -> Azure Blob, partition=%s ===", partition_date)

    pg = open_pg()
    container = open_blob_container()
    try:
        total = 0
        for table in TABLES:
            total += export_table(pg, container, table, partition_date)
        logger.info("=== Done. %d rows total uploaded for %s ===", total, partition_date)
    finally:
        pg.close()


if __name__ == "__main__":
    main()
