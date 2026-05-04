"""
shopstream_pipeline_dag.py

Daily orchestration for ShopStream:
    1. Extract PostgreSQL -> Azure Blob (CSV per table, partitioned by date)
    2. Load Azure Blob -> Snowflake RAW (COPY INTO, run by an external operator below)
    3. Build dbt: staging -> core -> marts, with tests inline (`dbt build`)
    4. Smoke-test the warehouse
    5. Notify on success / failure

Conventions:
    - Tasks fail loudly (no swallowed errors). Slack alerting is wired via
      `on_failure_callback` once the webhook is configured (see TODO).
    - dbt is invoked with `dbt build` so models and their tests run in
      dependency order, and a single test failure stops the pipeline.
    - SLA: 90 minutes end-to-end at the demo data volume.

v2 changes vs v1:
    - `dbt run` + `dbt test` consolidated into `dbt build` (was previously
      two tasks, with test failures silently swallowed).
    - Schema names fixed (CORE_marts -> MARTS, etc.).
    - All comments and log messages in English.
    - SLAs added per task; Slack callback hook wired (placeholder).
"""

from __future__ import annotations

import logging
import os
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# ---------------------------------------------------------------------------
# Paths and config
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path("/opt/airflow")
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt_part" / "shopstream_dbt"

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Callbacks
# ---------------------------------------------------------------------------

def notify_failure(context: dict[str, Any]) -> None:
    """
    Failure callback. Logs the failure and (TODO) posts to Slack.

    Wire SLACK_WEBHOOK_URL via Airflow Variables / env, then replace the
    log line below with a `requests.post(SLACK_WEBHOOK_URL, json=payload)`
    call. Kept as a no-op for v2.0 so the DAG runs without external deps.
    """
    task = context["task_instance"]
    logger.error(
        "Pipeline failure: dag=%s task=%s execution_date=%s log_url=%s",
        task.dag_id,
        task.task_id,
        context.get("ds"),
        task.log_url,
    )
    # TODO(v2.1): Slack webhook integration.


# ---------------------------------------------------------------------------
# Default args
# ---------------------------------------------------------------------------

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email": ["alerts@shopstream.example"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
    "sla": timedelta(minutes=30),
    "on_failure_callback": notify_failure,
}


dag = DAG(
    dag_id="shopstream_daily_pipeline",
    default_args=default_args,
    description="Daily ShopStream pipeline: PostgreSQL -> Azure Blob -> Snowflake -> dbt -> BI",
    schedule_interval="0 2 * * *",  # 02:00 UTC daily
    catchup=False,
    max_active_runs=1,
    tags=["production", "daily", "shopstream"],
)


# ---------------------------------------------------------------------------
# Task functions
# ---------------------------------------------------------------------------

def _run(cmd: list[str], cwd: Path | None = None, env_updates: dict[str, str] | None = None) -> None:
    """Run a subprocess and raise on non-zero exit, surfacing stdout / stderr."""
    env = os.environ.copy()
    if env_updates:
        env.update(env_updates)
    logger.info("Running: %s (cwd=%s)", " ".join(cmd), cwd or os.getcwd())
    result = subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        capture_output=True,
        text=True,
        env=env,
    )
    if result.stdout:
        logger.info("stdout:\n%s", result.stdout)
    if result.stderr:
        logger.warning("stderr:\n%s", result.stderr)
    if result.returncode != 0:
        raise RuntimeError(f"Command failed with code {result.returncode}: {' '.join(cmd)}")


def extract_postgres_to_azure_blob(**context: Any) -> None:
    """Extract relational tables from PostgreSQL and upload CSVs to Azure Blob."""
    execution_date = context["ds"]
    logger.info("PostgreSQL -> Azure Blob extraction for %s", execution_date)
    _run(
        ["python", str(SCRIPTS_DIR / "export_to_azure_blob.py")],
        cwd=SCRIPTS_DIR,
        env_updates={"EXECUTION_DATE": execution_date},
    )


def copy_azure_blob_to_snowflake(**context: Any) -> None:
    """Load the current Azure Blob partition into Snowflake RAW with COPY INTO."""
    execution_date = context["ds"]
    logger.info("Azure Blob -> Snowflake RAW load for %s", execution_date)
    _run(
        ["python", str(SCRIPTS_DIR / "run_snowflake_copy_into.py")],
        cwd=SCRIPTS_DIR,
        env_updates={"EXECUTION_DATE": execution_date},
    )


def verify_snowflake_data(**_: Any) -> list[tuple[str, int]]:
    """
    Smoke-test: confirm every layer has rows after the build.

    Raises if any table is empty (loud failure beats silent data drift).
    """
    try:
        import snowflake.connector  # type: ignore[import-not-found]
    except ImportError as exc:
        raise RuntimeError(
            "snowflake-connector-python is required in the Airflow image."
        ) from exc

    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "TRANSFORM_WH"),
        database="SHOPSTREAM_DWH",
    )

    targets = [
        ("RAW.RAW_USERS",                  "select count(*) from RAW.RAW_USERS"),
        ("RAW.RAW_ORDERS",                 "select count(*) from RAW.RAW_ORDERS"),
        ("STAGING.STG_ORDERS",             "select count(*) from STAGING.STG_ORDERS"),
        ("CORE.DIM_CUSTOMERS",             "select count(*) from CORE.DIM_CUSTOMERS"),
        ("CORE.FACT_ORDERS",               "select count(*) from CORE.FACT_ORDERS"),
        ("MARTS.MART_SALES_OVERVIEW",      "select count(*) from MARTS.MART_SALES_OVERVIEW"),
        ("MARTS.MART_CUSTOMER_LTV",        "select count(*) from MARTS.MART_CUSTOMER_LTV"),
        ("MARTS.MART_PRODUCT_PERFORMANCE", "select count(*) from MARTS.MART_PRODUCT_PERFORMANCE"),
    ]

    results: list[tuple[str, int]] = []
    empty: list[str] = []

    try:
        cursor = conn.cursor()
        try:
            for name, query in targets:
                cursor.execute(query)
                count = int(cursor.fetchone()[0])
                results.append((name, count))
                logger.info("%s: %d rows", name, count)
                if count == 0:
                    empty.append(name)
        finally:
            cursor.close()
    finally:
        conn.close()

    if empty:
        raise RuntimeError(f"Empty tables after build: {', '.join(empty)}")

    return results


def send_success_notification(**context: Any) -> None:
    """Success callback. TODO(v2.1): Slack webhook."""
    logger.info("Pipeline completed successfully for %s", context["ds"])


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

start = EmptyOperator(task_id="start", dag=dag)

# Optional in production; useful in the demo so the DB stays populated.
task_generate_data = BashOperator(
    task_id="generate_sample_data",
    bash_command=f'cd "{SCRIPTS_DIR}" && python generate_data.py',
    dag=dag,
)

task_extract = PythonOperator(
    task_id="extract_postgres_to_azure_blob",
    python_callable=extract_postgres_to_azure_blob,
    dag=dag,
)

task_copy_to_snowflake = PythonOperator(
    task_id="copy_azure_blob_to_snowflake",
    python_callable=copy_azure_blob_to_snowflake,
    dag=dag,
)

# `dbt deps` is idempotent and cheap; ensures dbt_utils / dbt_expectations
# are present even on a fresh container.
task_dbt_deps = BashOperator(
    task_id="dbt_deps",
    bash_command=f'cd "{DBT_PROJECT_DIR}" && dbt deps',
    dag=dag,
)

task_dbt_seed = BashOperator(
    task_id="dbt_seed",
    bash_command=f'cd "{DBT_PROJECT_DIR}" && dbt seed',
    dag=dag,
)

# `dbt build` runs models in dependency order AND runs each model's tests
# immediately after it builds. A failing test halts the DAG -- which is
# exactly what we want a quality gate to do.
task_dbt_build = BashOperator(
    task_id="dbt_build",
    bash_command=f'cd "{DBT_PROJECT_DIR}" && dbt build --fail-fast',
    dag=dag,
)

task_dbt_docs = BashOperator(
    task_id="dbt_generate_docs",
    bash_command=f'cd "{DBT_PROJECT_DIR}" && dbt docs generate',
    dag=dag,
)

task_verify = PythonOperator(
    task_id="verify_snowflake_data",
    python_callable=verify_snowflake_data,
    dag=dag,
)

task_success = PythonOperator(
    task_id="send_success_notification",
    python_callable=send_success_notification,
    dag=dag,
)

end = EmptyOperator(task_id="end", dag=dag)

# ---------------------------------------------------------------------------
# Wiring
# ---------------------------------------------------------------------------

(
    start
    >> task_generate_data
    >> task_extract
    >> task_copy_to_snowflake
    >> task_dbt_deps
    >> task_dbt_seed
    >> task_dbt_build
    >> task_dbt_docs
    >> task_verify
    >> task_success
    >> end
)
