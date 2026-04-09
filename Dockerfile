FROM apache/airflow:2.9.1-python3.11

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    gcc \
    python3-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Instalacja pakietów pythona dla samego Airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Tworzenie venv dla dbt i instalacja dbt-snowflake
RUN python -m venv /opt/airflow/dbt_venv && \
    /opt/airflow/dbt_venv/bin/pip install --no-cache-dir --upgrade pip && \
    /opt/airflow/dbt_venv/bin/pip install --no-cache-dir dbt-snowflake

# Kopiowanie plików projektowych
COPY --chown=airflow:root dbt /opt/airflow/dbt
COPY --chown=airflow:root dags /opt/airflow/dags
