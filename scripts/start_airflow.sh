#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/airflow_env.sh
mkdir -p evidence/airflow
airflow db migrate > evidence/airflow/db-migrate.log 2>&1
if ! systemctl --user is-active --quiet osp-offline-airflow.service; then
  if systemctl --user is-failed --quiet osp-offline-airflow.service; then systemctl --user reset-failed osp-offline-airflow.service; fi
  systemd-run --user --slice=osp-offline.slice --unit=osp-offline-airflow --property=TasksMax=2048 --working-directory="$PWD" /bin/bash "$PWD/scripts/airflow_scheduler.sh"
fi
