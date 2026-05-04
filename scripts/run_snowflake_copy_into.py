"""
run_snowflake_copy_into.py

Executes the Azure Blob -> Snowflake RAW COPY INTO SQL for the Airflow execution date.
"""

from __future__ import annotations

import logging
import os
import re
from datetime import UTC, datetime
from pathlib import Path

import snowflake.connector
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("ShopStream.SnowflakeCopyInto")

load_dotenv()

SCRIPT_DIR = Path(__file__).resolve().parent
SQL_FILE = SCRIPT_DIR / "snowflake_copy_into.sql"


def split_sql_statements(sql: str) -> list[str]:
    """Split simple SQL scripts on semicolons while removing comments."""
    lines = []
    for line in sql.splitlines():
        stripped = line.strip()
        if stripped.startswith("--") or not stripped:
            continue
        lines.append(line)
    cleaned = "\n".join(lines)
    return [stmt.strip() for stmt in cleaned.split(";") if stmt.strip()]


def main() -> None:
    partition_date = os.environ.get("EXECUTION_DATE") or datetime.now(UTC).strftime("%Y-%m-%d")
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", partition_date):
        raise RuntimeError(f"Invalid EXECUTION_DATE: {partition_date}. Expected YYYY-MM-DD.")

    sql = SQL_FILE.read_text(encoding="utf-8").replace("{{ partition_date }}", partition_date)
    statements = split_sql_statements(sql)

    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ.get("SNOWFLAKE_ROLE"),
        warehouse=os.environ.get("SNOWFLAKE_LOADING_WAREHOUSE", "LOADING_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "SHOPSTREAM_DWH"),
        schema="RAW",
    )

    logger.info(
        "Executing %d Snowflake statements for partition=%s", len(statements), partition_date
    )
    try:
        cursor = conn.cursor()
        try:
            for statement in statements:
                logger.info("Executing: %s", statement[:120].replace("\n", " "))
                cursor.execute(statement)
                if cursor.description:
                    rows = cursor.fetchall()
                    for row in rows:
                        logger.info("Result: %s", row)
        finally:
            cursor.close()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
