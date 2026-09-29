#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ ! -x .runtime/python/bin/python3.11 || ! -f evidence/runtime-artifacts-verified.json ]]; then
  python3 scripts/fetch_runtime.py
fi
if ! systemctl --user is-active --quiet osp-offline-cluster.service; then
  bash scripts/start_cluster.sh
fi
.runtime/python/bin/python3.11 scripts/wait_cluster.py
source config/runtime/env.sh
date="$1"; hour="$2"; shift 2
"$PYSPARK_PYTHON" scripts/ensure_source.py "$date" "$hour"
# Driver and all Hadoop/YARN processes share one bounded slice.
exec systemd-run --user --scope --slice=osp-offline.slice --quiet /bin/bash -c 'source config/runtime/env.sh; exec "$PYSPARK_DRIVER_PYTHON" scripts/run_slice.py "$@"' bash --date "$date" --hour "$hour" "$@"
