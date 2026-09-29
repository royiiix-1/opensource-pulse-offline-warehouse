#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/airflow_env.sh
systemctl --user is-active --quiet osp-offline-cluster.service
systemctl --user is-active --quiet osp-offline-airflow.service
exec systemd-run --user --scope --slice=osp-offline.slice --quiet "$PYSPARK_PYTHON" scripts/expand_m3_dates.py "$@"
