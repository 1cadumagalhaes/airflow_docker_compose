"""Turn the standalone `launch_api.py` client into an Airflow DAG.

The API code, response models, and normalization live in `dags/launch_api.py`
and can run without Airflow. This DAG adds scheduling, trigger-form
parameters, TaskFlow dependencies, and an idempotent Postgres load.
"""

from __future__ import annotations

import datetime
import logging

import pendulum
from airflow.sdk import Param, dag, task
from airsql import Table
from db_interface import (
    ensure_table,
    sql,
    upsert_rows,
)
from launch_api import fetch_launches

POSTGRES_CONN_ID = 'postgres_local'
log = logging.getLogger(__name__)
STAGING_TABLE = Table(conn_id=POSTGRES_CONN_ID, table_name='launches_staging')
LAUNCH_COLUMNS = [
    'id',
    'name',
    'provider',
    'rocket',
    'orbit',
    'country',
    'status',
    'launch_time',
]
STAGING_COLUMNS = {
    'id': 'TEXT PRIMARY KEY',
    'name': 'TEXT',
    'provider': 'TEXT',
    'rocket': 'TEXT',
    'orbit': 'TEXT',
    'country': 'TEXT',
    'status': 'TEXT',
    'launch_time': 'TIMESTAMPTZ',
}


@task
def create_staging_table() -> None:
    log.info('Ensuring staging table %s exists', STAGING_TABLE.table_name)
    ensure_table(STAGING_TABLE, STAGING_COLUMNS)
    log.info('Staging table %s is ready', STAGING_TABLE.table_name)


@task
def extract_launches(params: dict | None = None) -> list[dict]:
    """Call the standalone API client with the values from Airflow's form."""
    if params is None:
        raise RuntimeError('Airflow did not provide the DAG params to this task.')

    return fetch_launches(limit=params['limit'], mode=params['mode'])


@task
def load_launches(launches: list[dict]) -> int:
    log.info('Upserting %d launches into %s', len(launches), STAGING_TABLE.table_name)
    count = upsert_rows(
        STAGING_TABLE,
        columns=LAUNCH_COLUMNS,
        rows=launches,
    )
    log.info('Upsert complete: %d launches processed', count)
    return count


@sql.dataframe(source_conn=POSTGRES_CONN_ID)
def transform_launches(source_table: Table):
    return """
        SELECT
            provider,
            country,
            COUNT(*) AS launches,
            SUM(CASE WHEN status = 'Launch Successful' THEN 1 ELSE 0 END) AS successes,
            ROUND(
                100.0 * SUM(CASE WHEN status = 'Launch Successful' THEN 1 ELSE 0 END)
                / COUNT(*), 1
            ) AS success_rate_pct
        FROM {{ source_table }}
        GROUP BY provider, country
        ORDER BY launches DESC, provider;
    """


@task
def print_launch_summary(summary_df) -> None:
    log.info('Launch summary contains %d provider/country groups', len(summary_df))
    log.info('Launches by provider & country:\n%s', summary_df.to_string())


@dag(
    dag_id='api_etl_tutorial',
    dag_display_name='API ETL Tutorial',
    schedule='@daily',
    start_date=pendulum.datetime(2026, 1, 1, tz='UTC'),
    catchup=False,
    tags=['tutorial'],
    default_args={
        'owner': 'cadu',
        'retries': 1,
        'retry_delay': datetime.timedelta(minutes=5),
    },
    owner_links={'cadu': 'https://blog.cadumagalhaes.dev'},
    params={
        'limit': Param(
            50,
            type='integer',
            minimum=1,
            maximum=100,
            title='Launch count',
            description='Maximum number of launches to request (API maximum: 100).',
        ),
        'mode': Param(
            'detailed',
            type='string',
            enum=['list', 'normal', 'detailed'],
            title='Response detail',
            description='Level of detail returned for each launch.',
        ),
    },
    doc_md=__doc__,
)
def api_etl_tutorial():
    staging = create_staging_table()
    launches = extract_launches()
    loaded = load_launches(launches)
    summary = transform_launches(source_table=STAGING_TABLE)
    printed_summary = print_launch_summary(summary)

    staging >> launches >> loaded >> summary >> printed_summary


dag = api_etl_tutorial()

if __name__ == '__main__':
    # Debug pattern: runs the whole Dag in a single process.
    # https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/debug.html
    dag.test()
