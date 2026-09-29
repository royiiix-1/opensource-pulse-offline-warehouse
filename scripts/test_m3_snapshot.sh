#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
source config/runtime/env.sh
log="evidence/m3-snapshot-fixture-$(date -u +%Y%m%dT%H%M%SZ).log"
systemd-run --user --scope --slice=osp-offline.slice --quiet "$SPARK_HOME/bin/spark-submit" --master yarn --conf spark.executor.instances=1 scripts/test_m3_snapshot.py > "$log" 2>&1
