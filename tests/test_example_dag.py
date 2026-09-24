"""Unit tests for the example DAG: structure and rendered SQL.

No database involved. Runtime verification (a real Dag run) is done via
`dag.test()` — see the `if __name__ == '__main__'` block in the DAG file
and https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/debug.html
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

DAGS_DIR = Path(__file__).resolve().parent.parent / 'dags'
sys.path.insert(0, str(DAGS_DIR))

from airsql_dag_run_stats import POSTGRES_CONN_ID, dag  # noqa: E402


def test_dag_registers_tasks():
    task_ids = {t.task_id for t in dag.tasks}
    assert task_ids == {'dag_runs_by_state', 'print_run_stats'}


def test_dag_dependency_order():
    print_run_stats = dag.get_task('print_run_stats')
    assert print_run_stats.upstream_task_ids == {'dag_runs_by_state'}


def test_sql_renders_with_source_table():
    """{{ source_table }} renders to the Table's table name at runtime
    (airsql operators.py:61); the function body is the SQL template."""

    task = dag.get_task('dag_runs_by_state')
    source_table = task.op_kwargs['source_table']
    assert source_table.table_name == 'dag_run'
    assert source_table.conn_id == POSTGRES_CONN_ID

    # The inner function takes the Table and returns the SQL template;
    # {{ source_table }} renders to the table name at runtime.
    sql = task.python_callable.__wrapped__(source_table)
    assert 'FROM {{ source_table }}' in sql
    assert 'GROUP BY state' in sql
    assert 'COUNT(*) AS run_count' in sql


def test_dag_config():
    assert dag.dag_id == 'airsql_dag_run_stats'
    assert not dag.catchup
    assert dag.schedule == '@daily'


def test_tasks_have_no_retries_set():
    pytest.importorskip('airflow.sdk')
    for task in dag.tasks:
        assert task.retries in (0, None)
