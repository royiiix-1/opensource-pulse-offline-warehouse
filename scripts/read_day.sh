#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
source config/runtime/env.sh
exec systemd-run --user --scope --slice=osp-offline.slice --quiet "$PYSPARK_PYTHON" scripts/read_day.py "$@"
