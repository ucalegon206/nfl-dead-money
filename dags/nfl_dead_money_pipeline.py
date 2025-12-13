"""
[TODO: PRODUCTIONIZATION - PHASE 2]

Airflow DAG for NFL Dead Money Data Pipeline

This DAG orchestrates the daily collection and processing of NFL dead money data:
1. Extract data from Spotrac/Over The Cap
2. Load to Iceberg raw layer
3. Transform with dbt (staging → intermediate → mart)
4. Run data quality validations
5. Alert on failures

Current Status: TEMPLATE ONLY - Awaiting real data source integration
Target Deployment: Q1 2025
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.exceptions import AirflowException
import logging

logger = logging.getLogger(__name__)

# TODO: Define configuration
DEFAULT_ARGS = {
    'owner': 'nfl-analytics',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# TODO: Create DAG
dag = DAG(
    'nfl_dead_money_pipeline',
    default_args=DEFAULT_ARGS,
    description='Daily NFL dead money data collection and transformation',
    schedule_interval='0 2 * * *',  # 2 AM daily
    catchup=False,
    tags=['nfl', 'dead-money', 'finance'],
)


# TODO: Implement extract task
def extract_spotrac_data(**context):
    """
    Extract dead money data from Spotrac.
    
    Future implementation:
    - Call Spotrac API or run web scraper
    - Validate response
    - Save to staging location
    """
    logger.info("TODO: Implement Spotrac extraction")
    # from src.data_collection import scrape_spotrac_dead_money
    # df = scrape_spotrac_dead_money(year=datetime.now().year)
    pass


# TODO: Implement load task
def load_to_iceberg(**context):
    """
    Load extracted data to Iceberg raw layer.
    
    Future implementation:
    - Read from staging
    - Convert to Iceberg format
    - Write to data warehouse
    - Create snapshots
    """
    logger.info("TODO: Implement Iceberg load")
    pass


# TODO: Implement validation task
def run_data_quality_checks(**context):
    """
    Run data quality validations.
    
    Future implementation:
    - Import from src/data_validation.py
    - Run all validation functions
    - Raise exception if any checks fail
    - Log detailed results
    """
    logger.info("TODO: Implement data quality checks")
    # from src.data_validation import run_all_validations
    # results = run_all_validations(df_players, df_teams)
    # if not results['overall_valid']:
    #     raise AirflowException("Data quality checks failed")


# Task definitions
extract_task = PythonOperator(
    task_id='extract_spotrac',
    python_callable=extract_spotrac_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_to_iceberg_raw',
    python_callable=load_to_iceberg,
    dag=dag,
)

# TODO: Add dbt tasks when dbt project is ready
# transform_staging = BashOperator(
#     task_id='dbt_staging_models',
#     bash_command='cd /dbt && dbt run --select staging --profiles-dir .',
#     dag=dag,
# )
#
# transform_intermediate = BashOperator(
#     task_id='dbt_intermediate_models',
#     bash_command='cd /dbt && dbt run --select intermediate --profiles-dir .',
#     dag=dag,
# )
#
# transform_mart = BashOperator(
#     task_id='dbt_mart_models',
#     bash_command='cd /dbt && dbt run --select mart --profiles-dir .',
#     dag=dag,
# )

validation_task = PythonOperator(
    task_id='validate_data_quality',
    python_callable=run_data_quality_checks,
    dag=dag,
)

# Task dependencies
# TODO: Update when dbt tasks are added
# extract_task >> load_task >> transform_staging >> transform_intermediate >> transform_mart >> validation_task

extract_task >> load_task >> validation_task


if __name__ == "__main__":
    logger.warning("=" * 80)
    logger.warning("TEMPLATE ONLY - Ready for implementation when real data source is available")
    logger.warning("See .github/productionization-roadmap.md for detailed implementation plan")
    logger.warning("=" * 80)
