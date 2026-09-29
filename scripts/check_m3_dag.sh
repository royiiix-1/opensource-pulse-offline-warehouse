#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/airflow_env.sh"
exec "$OSP_ROOT/.runtime/airflow-env/bin/python" scripts/check_m3_dag.py
