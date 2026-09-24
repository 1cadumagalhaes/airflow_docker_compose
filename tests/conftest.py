"""Test environment setup for dag.test() runs.

Airflow reads its configuration from environment variables, so the test
suite points it at a throwaway SQLite metadata database and this repo's
dags/ folder before anything imports airflow. The `postgres_local`
connection used by the example DAG comes from the regular .env file
(AIRFLOW_CONN_POSTGRES_LOCAL), which `uv run` loads automatically.
"""

import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

os.environ.setdefault('AIRFLOW_HOME', str(REPO_ROOT / '.test_airflow'))
os.environ.setdefault(
    'AIRFLOW__DATABASE__SQL_ALCHEMY_CONN',
    f'sqlite:///{REPO_ROOT / ".test_airflow/airflow.db"}',
)
os.environ.setdefault('AIRFLOW__CORE__DAGS_FOLDER', str(REPO_ROOT / 'dags'))
os.environ.setdefault('AIRFLOW__CORE__LOAD_EXAMPLES', 'false')
# The compose Postgres publishes port 5432 on the host; tests run outside
# the compose network, so rewrite the in-network hostname to localhost.
# Assignment (not setdefault): tests deliberately override .env, whose
# postgres host only resolves inside the compose network.
os.environ['AIRFLOW_CONN_POSTGRES_LOCAL'] = (
    'postgresql://airflow:airflow@localhost:5432/airflow'
)
