import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

# ======================================
# KONFIGURACJA PROFILU DBT (COSMOS)
# ======================================
profile_config_main = ProfileConfig(
    profile_name="nyc_taxi_profile",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_default",  
        profile_args={
            "database": os.getenv("SNOWFLAKE_DATABASE"),
            "schema": "PUBLIC",
        },
    ),
)

default_args = {
    "owner": "data_engineer",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ======================================
# GŁÓWNA DEFINICJA DAG-a
# ======================================
with DAG(
    dag_id="nyc_taxi_end_to_end_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 4, 1), 
    schedule_interval=None,
    catchup=False,
    description="Kompletny ELT: Skrypt Python (Extract) -> SQLExecute (Load Bronze) -> dbt (Transform Silver/Gold)",
    tags=["nyc_taxi", "elt", "snowflake", "dbt"],
    template_searchpath=["/opt/airflow/dags/sql"], 
) as dag:

    # -------------------------------------------------------------
    # ETAP 1: EXTRACT (Python ładujący pliki Parquet/CSV na Stage)
    # -------------------------------------------------------------
    ingest_to_stage = BashOperator(
        task_id="ingest_files_to_snowflake_stage",
        bash_command="python /opt/airflow/dags/ingest_to_snowflake.py"
    )

    # -------------------------------------------------------------
    # ETAP 2: SETUP (Tworzenie bazy, schematów, konfiguracja początkowa)
    # -------------------------------------------------------------
    run_setup_sql = SQLExecuteQueryOperator(
        task_id="setup_snowflake_environment",
        conn_id="snowflake_default",
        sql="setup/create_database_and_schema.sql" 
    )

    # -------------------------------------------------------------
    # ETAP 3: LOAD BRONZE (Wykonanie COPY INTO z plików do tabel)
    # -------------------------------------------------------------
    load_bronze_sql = SQLExecuteQueryOperator(
        task_id="load_data_to_bronze",
        conn_id="snowflake_default",
        sql="bronze/load_raw.sql" 
    )

    # -------------------------------------------------------------
    # ETAP 4: TRANSFORM (Uruchomienie logiki dbt dla Silver i Gold)
    # -------------------------------------------------------------
    transform_silver = DbtTaskGroup(
        group_id="transform_silver",
        project_config=ProjectConfig("/opt/airflow/dbt"),
        profile_config=profile_config_main,
        execution_config=ExecutionConfig(dbt_executable_path="/opt/airflow/dbt_venv/bin/dbt"),
        render_config=RenderConfig(select=["models/silver"]), 
    )

    transform_gold = DbtTaskGroup(
        group_id="transform_gold",
        project_config=ProjectConfig("/opt/airflow/dbt"),
        profile_config=profile_config_main,
        execution_config=ExecutionConfig(dbt_executable_path="/opt/airflow/dbt_venv/bin/dbt"),
        render_config=RenderConfig(select=["models/gold"]), 
    )

    # ======================================
    # PIPELINE
    # ======================================
    run_setup_sql >> ingest_to_stage >> load_bronze_sql >> transform_silver >> transform_gold