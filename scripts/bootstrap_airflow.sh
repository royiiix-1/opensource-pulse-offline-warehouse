#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ ! -x .runtime/python/bin/python3.11 ]]; then python3 scripts/fetch_runtime.py; fi
if [[ ! -x .runtime/airflow-env/bin/python ]]; then .runtime/python/bin/python3.11 -m venv .runtime/airflow-env; fi
if [[ ! -x .runtime/airflow-env/bin/airflow ]]; then
  .runtime/airflow-env/bin/python -m pip --isolated --cache-dir build/airflow-install/pip-cache install --index-url https://pypi.org/simple wheel==0.45.1
  .runtime/python/bin/python3.11 scripts/install_airflow_locked.py
fi
bash scripts/start_cluster.sh
.runtime/python/bin/python3.11 scripts/wait_cluster.py
exec bash scripts/start_airflow.sh
