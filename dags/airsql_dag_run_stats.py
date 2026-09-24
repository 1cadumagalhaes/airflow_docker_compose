from __future__ import annotations

import logging

import pandas as pd
import pendulum
from airflow.sdk import dag, task
from airsql import Table, sql

log = logging.getLogger(__name__)

POSTGRES_CONN_ID = 'postgres_local'


@dag(
    schedule='@daily',
    start_date=pendulum.datetime(2024, 1, 1, tz='UTC'),
    catchup=False,
    tags=['airsql', 'tutorial'],
    default_args={'owner': 'cadu'},
    owner_links={'cadu': 'https://blog.cadumagalhaes.dev'},
    doc_md=__doc__,
)
def airsql_dag_run_stats():
    """
    ### Dag run stats with airsql

    Queries Airflow's own metadata database with the airsql `@sql.dataframe`
    decorator: dag runs aggregated by state, then logged by a plain task.
    The `{{ source_table }}` placeholder renders to the `Table`'s name.
    """

    dag_run_table = Table(conn_id=POSTGRES_CONN_ID, table_name='dag_run')

    @sql.dataframe(source_conn=POSTGRES_CONN_ID)
    def dag_runs_by_state(source_table: Table):
        return """
            SELECT
                state,
                COUNT(*) AS run_count
            FROM {{ source_table }}
            GROUP BY state
            ORDER BY run_count DESC;
        """

    @task
    def print_run_stats(df: pd.DataFrame):
        """
        #### Print task
        Logs the aggregated DataFrame; airsql already returned it as pandas.
        """
        log.info('Dag runs by state:')
        log.info('\n%s', df.to_string())
        if df.empty:
            log.warning('The DataFrame is empty!')

    dag_runs = dag_runs_by_state(source_table=dag_run_table)
    print_run_stats(df=dag_runs)


# Instantiating the factory registers the DAG with Airflow.
dag = airsql_dag_run_stats()

if __name__ == '__main__':
    # Debug pattern: runs the whole Dag in a single process.
    # https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/debug.html
    dag.test()
