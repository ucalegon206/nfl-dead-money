"""
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
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.exceptions import AirflowException
import logging

from src.pipeline_tasks import scrape_rosters, merge_dead_money, run_data_quality
from src.pipeline_tasks import validate_staging
from src.ingestion import stage_spotrac_team_cap, stage_spotrac_player_rankings, stage_spotrac_dead_money
from src.normalization import normalize_team_cap, normalize_player_rankings, normalize_dead_money
from airflow.operators.bash import BashOperator

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
        schedule_interval='@weekly',  # Weekly schedule
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
    bash_command='cd /Users/andrewsmith/Documents/portfolio/nfl-dead-money && ./.venv/bin/python scripts/download_spotrac_data.py --snapshot-team-cap --year {{ ds.strftime("%Y") }} --method auto',
    dag=dag,
)

# One-time player rankings backfill (2011-2024)
player_rankings_backfill = BashOperator(
    task_id='snapshot_player_rankings_2011_2024',
    bash_command='cd /Users/andrewsmith/Documents/portfolio/nfl-dead-money && ./.venv/bin/python scripts/download_spotrac_data.py --snapshot-player-rankings --start-year 2011 --end-year 2024 --method auto',
    dag=dag,
)
dbt_seed = BashOperator(
    task_id='dbt_seed_spotrac',
    bash_command='cd /Users/andrewsmith/Documents/portfolio/nfl-dead-money/dbt && ../../.venv/bin/dbt seed --project-dir . --profiles-dir .',
    dag=dag,
)

dbt_run_staging = BashOperator(
    task_id='dbt_run_staging',
    bash_command='cd /Users/andrewsmith/Documents/portfolio/nfl-dead-money/dbt && ../../.venv/bin/dbt run --select staging --project-dir . --profiles-dir .',
    dag=dag,
)

dbt_run_marts = BashOperator(
    task_id='dbt_run_marts',
    bash_command='cd /Users/andrewsmith/Documents/portfolio/nfl-dead-money/dbt && ../../.venv/bin/dbt run --select marts --project-dir . --profiles-dir .',
    dag=dag,
)

# Integrate snapshots into pipeline ordering: run snapshots before merge
team_cap_snapshot >> stage_task >> staging_validation_task >> dbt_seed >> dbt_run_staging >> normalize_task >> dbt_run_marts >> scrape_task
player_rankings_backfill >> stage_task >> staging_validation_task >> dbt_seed >> dbt_run_staging >> normalize_task >> dbt_run_marts >> scrape_task


if __name__ == "__main__":
    logger.warning("=" * 80)
    logger.warning("RUNNING LOCAL DEBUG OF DAG TASKS")
    logger.warning("=" * 80)
    task_scrape_rosters()
    task_merge_dead_money()
    task_run_data_quality()
