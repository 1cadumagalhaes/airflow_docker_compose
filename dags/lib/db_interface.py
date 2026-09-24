"""Small database interface for the tutorial DAG.

Use Airsql's `@sql.dataframe` for SELECT queries and its `SQLHookManager`
for DataFrame merges. The helpers here cover table DDL and merging Python
API rows after converting them to a DataFrame.

Column specs are dicts of column name -> Postgres type, e.g.

    SPEC = {'id': 'TEXT PRIMARY KEY', 'name': 'TEXT'}

which `ensure_table` renders into a CREATE TABLE IF NOT EXISTS.
"""

from __future__ import annotations

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airsql import Table, sql
from airsql.hooks import SQLHookManager


def ensure_table(
    table: Table,
    columns: dict[str, str],
) -> None:
    """Create the table if missing (never fails on re-runs).

    No TRUNCATE: the table holds accumulated history across runs, and
    the UPSERT in `upsert_rows` keeps re-runs idempotent.
    """
    rendered = ', '.join(f'"{name}" {ctype}' for name, ctype in columns.items())
    hook = PostgresHook(postgres_conn_id=table.conn_id)
    hook.run(f'CREATE TABLE IF NOT EXISTS {table.table_name} ({rendered});')


def drop_table(table: Table) -> None:
    """Drop the table (scratch cleanup, safe if missing)."""
    hook = PostgresHook(postgres_conn_id=table.conn_id)
    hook.run(f'DROP TABLE IF EXISTS {table.table_name};')


def upsert_rows(
    table: Table,
    columns: list[str],
    rows: list[dict],
) -> int:
    """UPSERT rows into the table (idempotent on the first column).

    Uses airsql's merge (INSERT ... ON CONFLICT DO UPDATE) so re-running
    the same interval never duplicates rows.

    This is a plain function because the rows arrive at task runtime from
    the API task's XCom. Airsql's `@sql.merge_dataframe` expects a DataFrame
    while constructing the DAG, so SQLHookManager is the right API here.
    """
    if not rows:
        print('Nothing to load.')
        return 0

    df = pd.DataFrame(rows, columns=columns)
    SQLHookManager().merge_dataframe_to_table(
        df=df,
        table=table,
        conflict_columns=[columns[0]],
    )
    print(f'Upserted {len(df)} rows into {table.table_name}')
    return len(df)


__all__ = ['ensure_table', 'drop_table', 'upsert_rows', 'sql']
