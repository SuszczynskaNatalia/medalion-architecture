import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

# ── Profil dbt/Cosmos – identyczny z taxi_pipeline.py ────────────────────────
profile_config = ProfileConfig(
    profile_name="nyc_taxi_profile",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_default",
        profile_args={
            "database": os.getenv("SNOWFLAKE_DATABASE"),
            "schema":   os.getenv("SNOWFLAKE_SCHEMA", "BRONZE"),
        },
    ),
)

default_args = {
    "owner":        "natalia.s",
    "retries":      1,
    "retry_delay":  timedelta(minutes=5),
}

_SHARED = dict(
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    template_searchpath=["/opt/airflow/dags/sql"],
    tags=["nyc_taxi", "kafka", "streaming", "snowflake", "dbt"],
)

_UPLOAD_BASH = (
    "set -euo pipefail\n"
    "FILENAME=\"{{ ti.xcom_pull(task_ids='listen_kafka') }}\"\n"
    "python /opt/airflow/dags/taxi_streaming_cli.py upload \"${FILENAME}\"\n"
)


def _build_pipeline(dag: DAG, listen_bash: str) -> None:
    """Buduje graf zadań wewnątrz przekazanego DAG-a."""

    listen_kafka = BashOperator(
        task_id="listen_kafka",
        bash_command=listen_bash,
        do_xcom_push=True,
        dag=dag,
    )

    upload_to_stage = BashOperator(
        task_id="upload_to_stage",
        bash_command=_UPLOAD_BASH,
        dag=dag,
    )

    setup_env = SQLExecuteQueryOperator(
        task_id="setup_snowflake_environment",
        conn_id="snowflake_default",
        sql="setup/create_database_and_schema.sql",
        dag=dag,
    )

    load_bronze = SQLExecuteQueryOperator(
        task_id="load_data_to_bronze",
        conn_id="snowflake_default",
        sql="bronze/load_raw.sql",
        dag=dag,
    )

    transform_silver = DbtTaskGroup(
        group_id="transform_silver",
        project_config=ProjectConfig("/opt/airflow/dbt"),
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path="/opt/airflow/dbt_venv/bin/dbt"),
        render_config=RenderConfig(select=["models/silver"]),
        dag=dag,
    )

    transform_gold = DbtTaskGroup(
        group_id="transform_gold",
        project_config=ProjectConfig("/opt/airflow/dbt"),
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path="/opt/airflow/dbt_venv/bin/dbt"),
        render_config=RenderConfig(select=["models/gold"]),
        dag=dag,
    )

    export_csv = BashOperator(
        task_id="export_gold_to_csv",
        bash_command="python /opt/airflow/scripts/export_to_csv.py",
        dag=dag,
    )

    listen_kafka >> setup_env >> upload_to_stage >> load_bronze >> transform_silver >> transform_gold >> export_csv


# ── DAG 1 – ad-hoc (trigger ręczny) ──────────────────────────────────────────
with DAG(
    dag_id="taxi_streaming_http",
    schedule=None,
    description="Ad-hoc: 1 wiadomość Kafka → Stage Snowflake → Bronze COPY INTO → dbt",
    **_SHARED,
) as dag_adhoc:
    _build_pipeline(
        dag_adhoc,
        listen_bash="python /opt/airflow/dags/taxi_streaming_cli.py listen",
    )


# ── DAG 2 – automatyczny (codziennie, skip jeśli brak wiadomości) ─────────────
with DAG(
    dag_id="nyc_taxi_streaming_incremental",
    schedule=timedelta(days=1),
    description=(
        "Automatyczny przyrost z Kafki: listen → [skip jeśli brak] "
        "→ Stage → Bronze → dbt Silver/Gold"
    ),
    **_SHARED,
) as dag_auto:

    listen_auto = BashOperator(
        task_id="listen_kafka",
        bash_command=(
            "export KAFKA_LISTEN_NO_FAIL=1; "
            "export KAFKA_CONSUMER_GROUP=airflow_incremental_group; "
            "python /opt/airflow/dags/taxi_streaming_cli.py listen"
        ),
        do_xcom_push=True,
    )

    check_message = ShortCircuitOperator(
        task_id="check_message",
        python_callable=lambda **ctx: bool(
            (ctx["ti"].xcom_pull(task_ids="listen_kafka") or "").strip()
        ),
    )

    upload_auto = BashOperator(
        task_id="upload_to_stage",
        bash_command=_UPLOAD_BASH,
    )

    setup_auto = SQLExecuteQueryOperator(
        task_id="setup_snowflake_environment",
        conn_id="snowflake_default",
        sql="setup/create_database_and_schema.sql",
    )

    load_auto = SQLExecuteQueryOperator(
        task_id="load_data_to_bronze",
        conn_id="snowflake_default",
        sql="bronze/load_raw.sql",
    )

    silver_auto = DbtTaskGroup(
        group_id="transform_silver",
        project_config=ProjectConfig("/opt/airflow/dbt"),
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path="/opt/airflow/dbt_venv/bin/dbt"),
        render_config=RenderConfig(select=["models/silver"]),
    )

    gold_auto = DbtTaskGroup(
        group_id="transform_gold",
        project_config=ProjectConfig("/opt/airflow/dbt"),
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path="/opt/airflow/dbt_venv/bin/dbt"),
        render_config=RenderConfig(select=["models/gold"]),
    )

    export_auto = BashOperator(
        task_id="export_gold_to_csv",
        bash_command="python /opt/airflow/scripts/export_to_csv.py",
    )

    listen_auto >> check_message >> setup_auto >> upload_auto >> load_auto >> silver_auto >> gold_auto >> export_auto
