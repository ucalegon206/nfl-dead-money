"""
Daily NFL Dead Money Pipeline DAG for Apache Airflow.

This DAG orchestrates a comprehensive data pipeline for NFL dead money analysis,
integrating data from multiple sources (Pro Football Reference rosters and Spotrac
team cap data) and transforming it through staging, normalization, and mart layers.

Pipeline Flow:
1. Snapshot Spotrac team cap data and player rankings (weekly and backfill)
2. Stage raw Spotrac data into staging layer
3. Validate staging tables for data quality
4. Run dbt seed to load reference data
5. Transform staging data using dbt (staging layer)
6. Normalize and process data into marts layer
7. Scrape PFR rosters for years 2015 to current
8. Merge dead money data with roster contracts
9. Run final data quality validation

Configuration:
- Schedule: Weekly (@weekly)
- Owner: nfl-analytics
- Retries: 2 (with 5-minute delay)
- Email notifications: On failure only
- Catchup: Disabled

Dependencies:
- src.pipeline_tasks: Core ETL functions for scraping, merging, and validation
- src.ingestion: Spotrac data staging functions
- src.normalization: Data normalization utilities
- dbt: Data transformation and mart generation
- Apache Airflow: Workflow orchestration

Note:
- Hardcoded local paths should be parameterized for production deployment
- Player rankings backfill is a one-time load for historical years (2011-2024)
- Debug mode available for local task testing
Daily NFL Dead Money Pipeline (PFR rosters + dead money CSV).

Steps:
1) Scrape PFR rosters for 2015-current year
2) Merge dead money CSV (sample or real) into contracts
3) Recompute cap impact + team dead money aggregates
4) Run data quality tests

Notes:
- Uses src/pipeline_tasks.py helpers
- Replace dead_money_csv path with Spotrac/OTC export when available
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.exceptions import AirflowException
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
import logging
import os

from src.pipeline_tasks import scrape_rosters, merge_dead_money, run_data_quality
from src.pipeline_tasks import validate_staging
from src.ingestion import stage_spotrac_team_cap, stage_spotrac_player_rankings, stage_spotrac_dead_money
from src.normalization import normalize_team_cap, normalize_player_rankings, normalize_dead_money
import os
from pathlib import Path

logger = logging.getLogger(__name__)

# Project root: parent directory of dags/
PROJECT_ROOT = str(Path(__file__).parent.parent.absolute())


def slack_on_snapshot_complete(**context):
    """Post to Slack on player rankings snapshot completion."""
    task_id = context['task'].task_id
    status = context['task_instance'].state
    slack_webhook = os.getenv('SLACK_WEBHOOK_URL')
    if not slack_webhook:
        logger.info("SLACK_WEBHOOK_URL not set; skipping Slack notification")
        return
    
    try:
        import requests
        execution_date = context['execution_date']
        msg = f":nfl: Player Rankings Snapshot ({execution_date.strftime('%Y-%m-%d')})\n"
        msg += f"Status: {status}\nTask: {task_id}"
        requests.post(slack_webhook, json={'text': msg}, timeout=5)
    except Exception as e:
        logger.warning(f"Failed to post to Slack: {e}")


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
    schedule='@weekly',  # Weekly schedule
    catchup=False,
    tags=['nfl', 'dead-money', 'finance'],
)


def task_scrape_rosters(**context):
    end_year = datetime.now().year
    scrape_rosters(start_year=2015, end_year=end_year)


def task_merge_dead_money(**context):
    merge_dead_money()
def task_stage_spotrac(**context):
    year = datetime.now().year
    stage_spotrac_team_cap(year)
    stage_spotrac_player_rankings(year)
    stage_spotrac_dead_money(year)


def task_normalize_staging(**context):
    year = datetime.now().year
    normalize_team_cap(year)
    normalize_player_rankings(year)
    normalize_dead_money(year)



def task_run_data_quality(**context):
    run_data_quality()


def task_validate_staging(**context):
    validate_staging()


# Task definitions
scrape_task = PythonOperator(
    task_id='scrape_rosters',
    python_callable=task_scrape_rosters,
    dag=dag,
)

merge_task = PythonOperator(
    task_id='merge_dead_money',
    python_callable=task_merge_dead_money,
    dag=dag,
)

validation_task = PythonOperator(
    task_id='validate_data_quality',
    python_callable=task_run_data_quality,
    dag=dag,
)
stage_task = PythonOperator(
    task_id='stage_spotrac_raw_to_staging',
    python_callable=task_stage_spotrac,
    dag=dag,
)

staging_validation_task = PythonOperator(
    task_id='validate_staging_tables',
    python_callable=task_validate_staging,
    dag=dag,
)

normalize_task = PythonOperator(
    task_id='normalize_staging_to_processed',
    python_callable=task_normalize_staging,
    dag=dag,
)



scrape_task >> merge_task >> validation_task

# Weekly Spotrac team cap snapshot (cron via Airflow schedule)
team_cap_snapshot = BashOperator(
    task_id='snapshot_spotrac_team_cap',
    bash_command=f'cd {PROJECT_ROOT} && ./.venv/bin/python scripts/download_spotrac_data.py --snapshot-team-cap --year {{{{ ds.strftime("%Y") }}}} --method auto',
    dag=dag,
)

# Historical backfill (2015-2024) - one-time task, can be triggered manually
player_rankings_backfill = BashOperator(
    task_id='backfill_player_rankings_2015_2024',
    bash_command=f'cd {PROJECT_ROOT} && ./.venv/bin/python scripts/backfill_player_rankings.py --start-year 2015 --end-year 2024 --delay 30',
    dag=dag,
)
dbt_seed = BashOperator(
    task_id='dbt_seed_spotrac',
    bash_command=f'cd {PROJECT_ROOT}/dbt && ../.venv/bin/dbt seed --project-dir . --profiles-dir .',
    dag=dag,
)

dbt_run_staging = BashOperator(
    task_id='dbt_run_staging',
    bash_command=f'cd {PROJECT_ROOT}/dbt && ../.venv/bin/dbt run --select staging --project-dir . --profiles-dir .',
    dag=dag,
)

dbt_run_marts = BashOperator(
    task_id='dbt_run_marts',
    bash_command=f'cd {PROJECT_ROOT}/dbt && ../.venv/bin/dbt run --select marts --project-dir . --profiles-dir .',
    dag=dag,
)

data_quality_player_rankings = BashOperator(
    task_id='validate_player_rankings_quality',
    bash_command=f'cd {PROJECT_ROOT} && ./.venv/bin/python scripts/validate_player_rankings.py --year {{ ds.strftime("%Y") }}',
    dag=dag,
)

# Integrate snapshots into pipeline ordering: run snapshots before merge
player_rankings_weekly = BashOperator(
    task_id='snapshot_player_rankings_weekly',
    bash_command=f'cd {PROJECT_ROOT} && ./.venv/bin/python scripts/player_rankings_snapshot.py --year {{ ds.strftime("%Y") }} --retries 3',
    on_success_callback=slack_on_snapshot_complete,
    on_failure_callback=slack_on_snapshot_complete,
    dag=dag,
)

[team_cap_snapshot, player_rankings_weekly, player_rankings_backfill] >> stage_task >> staging_validation_task >> dbt_seed >> dbt_run_staging >> normalize_task >> dbt_run_marts >> data_quality_player_rankings >> scrape_task


if __name__ == "__main__":
    logger.warning("=" * 80)
    logger.warning("RUNNING LOCAL DEBUG OF DAG TASKS")
    logger.warning("=" * 80)
    task_scrape_rosters()
    task_merge_dead_money()
    task_run_data_quality()
