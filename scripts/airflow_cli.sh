#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/airflow_env.sh"
exec airflow "$@"
