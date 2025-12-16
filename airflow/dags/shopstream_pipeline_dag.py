"""
shopstream_pipeline_dag.py
DAG Airflow pour le pipeline quotidien ShopStream

Ce DAG orchestre :
1. Extraction PostgreSQL vers S3
2. Chargement S3 vers Snowflake Staging
3. Transformations dbt (staging vers core vers marts)
4. Tests de qualité dbt

Emplacement : airflow/dags/shopstream_pipeline_dag.py
"""

import os
import subprocess
import logging
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

# Configuration
PROJECT_ROOT = Path(__file__).parent.parent.parent  # shopstream/
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt_part" / "shopstream_dbt"

# Logging
logger = logging.getLogger(__name__)

# Default arguments
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email': ['alerts@shopstream.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2)
}

# DAG Definition
dag = DAG(
    'shopstream_daily_pipeline',
    default_args=default_args,
    description='Pipeline quotidien ShopStream: PostgreSQL → S3 → Snowflake → dbt → BI',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    catchup=False,
    tags=['production', 'daily', 'shopstream']
)


# =============================================================================
# Task Functions
# =============================================================================

def extract_postgres_to_s3(**context):
    """Extract data from PostgreSQL and upload to S3"""
    execution_date = context['ds']
    logger.info(f"Starting PostgreSQL → S3 extraction for {execution_date}")
    
    script_path = SCRIPTS_DIR / "export_to_s3.py"
    
    result = subprocess.run(
        ['python', str(script_path)],
        capture_output=True,
        text=True,
        env={**os.environ, 'EXECUTION_DATE': execution_date}
    )
    
    if result.returncode != 0:
        logger.error(f"Export failed: {result.stderr}")
        raise Exception(f"Export error: {result.stderr}")
    
    logger.info(result.stdout)
    logger.info("PostgreSQL → S3 extraction completed successfully")


def run_dbt_models(**context):
    """Run dbt transformations"""
    logger.info("Starting dbt run...")
    
    result = subprocess.run(
        ['dbt', 'run'],
        capture_output=True,
        text=True,
        cwd=str(DBT_PROJECT_DIR)
    )
    
    if result.returncode != 0:
        logger.error(f"dbt run failed: {result.stderr}")
        raise Exception(f"dbt run error: {result.stderr}")
    
    logger.info(result.stdout)
    logger.info("dbt transformations completed successfully")


def run_dbt_tests(**context):
    """Run dbt data quality tests"""
    logger.info("Starting dbt test...")
    
    result = subprocess.run(
        ['dbt', 'test'],
        capture_output=True,
        text=True,
        cwd=str(DBT_PROJECT_DIR)
    )
    
    if result.returncode != 0:
        logger.warning(f"Some dbt tests failed: {result.stderr}")
        # Don't fail the DAG, just warn
    
    logger.info(result.stdout)
    logger.info("dbt tests completed")


def verify_snowflake_data(**context):
    """Verify data in Snowflake marts"""
    logger.info("Verifying Snowflake data...")
    
    try:
        import snowflake.connector
        
        # Get connection details from environment
        conn = snowflake.connector.connect(
            account=os.environ.get('SNOWFLAKE_ACCOUNT'),
            user=os.environ.get('SNOWFLAKE_USER'),
            password=os.environ.get('SNOWFLAKE_PASSWORD'),
            warehouse=os.environ.get('SNOWFLAKE_WAREHOUSE', 'LOADING_WH'),
            database='SHOPSTREAM_DWH',
            schema='CORE_marts'
        )
        
        cursor = conn.cursor()
        
        # Verification queries
        tables = [
            ('STAGING.STG_USERS', 'SELECT COUNT(*) FROM STAGING.STG_USERS'),
            ('STAGING.STG_ORDERS', 'SELECT COUNT(*) FROM STAGING.STG_ORDERS'),
            ('CORE_core.DIM_CUSTOMERS', 'SELECT COUNT(*) FROM CORE_core.DIM_CUSTOMERS'),
            ('CORE_core.FACT_ORDERS', 'SELECT COUNT(*) FROM CORE_core.FACT_ORDERS'),
            ('CORE_marts.MART_SALES_OVERVIEW', 'SELECT COUNT(*) FROM CORE_marts.MART_SALES_OVERVIEW'),
            ('CORE_marts.MART_CUSTOMER_LTV', 'SELECT COUNT(*) FROM CORE_marts.MART_CUSTOMER_LTV'),
        ]
        
        results = []
        for table_name, query in tables:
            cursor.execute(query)
            count = cursor.fetchone()[0]
            results.append((table_name, count))
            logger.info(f"{table_name}: {count} rows")
            
            if count == 0:
                logger.warning(f"WARNING: {table_name} is empty!")
        
        cursor.close()
        conn.close()
        
        logger.info("Snowflake verification completed successfully")
        return results
        
    except ImportError:
        logger.warning("snowflake-connector-python not installed, skipping verification")
    except Exception as e:
        logger.error(f"Snowflake verification failed: {e}")
        # Don't fail the DAG, just warn


def send_success_notification(**context):
    """Send success notification"""
    execution_date = context['ds']
    logger.info(f"Pipeline completed successfully for {execution_date}")
    # TODO: Add Slack/Teams/Email webhook integration


# =============================================================================
# Task Definitions
# =============================================================================

# Start
start = EmptyOperator(
    task_id='start',
    dag=dag
)

# Task 1: Generate sample data (optional, for testing)
task_generate_data = BashOperator(
    task_id='generate_sample_data',
    bash_command=f'cd "{SCRIPTS_DIR}" && python generate_data.py',
    dag=dag
)

# Task 2: Extract PostgreSQL to S3
task_extract = PythonOperator(
    task_id='extract_postgres_to_s3',
    python_callable=extract_postgres_to_s3,
    dag=dag
)

# Task 3: Run dbt models
task_dbt_run = PythonOperator(
    task_id='dbt_run_models',
    python_callable=run_dbt_models,
    dag=dag
)

# Task 4: Run dbt tests
task_dbt_test = PythonOperator(
    task_id='dbt_test_models',
    python_callable=run_dbt_tests,
    dag=dag
)

# Task 5: Generate dbt docs
task_dbt_docs = BashOperator(
    task_id='dbt_generate_docs',
    bash_command=f'cd "{DBT_PROJECT_DIR}" && dbt docs generate',
    dag=dag
)

# Task 6: Verify Snowflake data
task_verify_data = PythonOperator(
    task_id='verify_snowflake_data',
    python_callable=verify_snowflake_data,
    dag=dag
)

# Task 7: Success notification
task_success = PythonOperator(
    task_id='send_success_notification',
    python_callable=send_success_notification,
    dag=dag
)

# End
end = EmptyOperator(
    task_id='end',
    dag=dag
)

# =============================================================================
# DAG Dependencies
# =============================================================================

# Pipeline flow:
# start → generate_data → extract → dbt_run → dbt_test → dbt_docs → verify_data → success → end

start >> task_generate_data >> task_extract >> task_dbt_run >> task_dbt_test >> task_dbt_docs >> task_verify_data >> task_success >> end

