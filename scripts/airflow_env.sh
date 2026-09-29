#!/usr/bin/env bash
cd "$(dirname "${BASH_SOURCE[0]}")/.."
source config/runtime/env.sh
export AIRFLOW_HOME="$OSP_ROOT/build/airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$OSP_ROOT/airflow/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__EXECUTOR=SequentialExecutor
export AIRFLOW__CORE__DEFAULT_TIMEZONE=utc
export AIRFLOW__CORE__PARALLELISM=1
export AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=1
export AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=True
export AIRFLOW__CORE__FERNET_KEY=''
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="sqlite:///$AIRFLOW_HOME/airflow.db"
export AIRFLOW__LOGGING__BASE_LOG_FOLDER="$AIRFLOW_HOME/logs"
export AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL=15
export AIRFLOW__SCHEDULER__MIN_FILE_PROCESS_INTERVAL=10
export AIRFLOW__SCHEDULER__DAG_FILE_PROCESSOR_TIMEOUT=180
export AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT=120
export AIRFLOW__WEBSERVER__SECRET_KEY="$("$OSP_ROOT/.runtime/python/bin/python3.11" -c 'import secrets;print(secrets.token_hex(32))')"
export AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK=False
export AIRFLOW__METRICS__STATSD_ON=False
export AIRFLOW__CORE__CHECK_SLAS=False
export PATH="$OSP_ROOT/.runtime/airflow-env/bin:$PATH"
export PYTHONPATH="$OSP_ROOT"
export TMPDIR="/var/tmp/osp-offline-1000/airflow-tmp"
mkdir -p "$TMPDIR"
