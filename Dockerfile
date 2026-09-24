ARG PYTHON_VERSION=3.12
ARG AIRFLOW_VERSION=3.3.2

FROM apache/airflow:slim-${AIRFLOW_VERSION}-python${PYTHON_VERSION}

ENV AIRFLOW_USER_HOME_DIR=/opt/airflow

WORKDIR $AIRFLOW_USER_HOME_DIR

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

COPY --chown=airflow:root pyproject.toml uv.lock ./

RUN uv export --frozen --no-dev --no-emit-project --no-hashes -o requirements.txt \
    && uv pip install -r requirements.txt "apache-airflow==${AIRFLOW_VERSION}" \
    && rm -rf requirements.txt

COPY --chown=airflow:root dags ./dags

# `uv pip install` as the airflow user lands in the user site-packages.
# Services may run as a different host-mapped UID (AIRFLOW_UID), which can't
# see that directory by default — expose it through PYTHONPATH.
ENV PYTHONPATH=/home/airflow/.local/lib/python3.12/site-packages

USER airflow