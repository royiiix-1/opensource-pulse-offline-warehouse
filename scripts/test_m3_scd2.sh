#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
source config/runtime/env.sh
run="evidence/m3-scd2-$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$run"
systemd-run --user --scope --slice=osp-offline.slice --quiet "$SPARK_HOME/bin/spark-submit" --master yarn scripts/test_m3_scd2.py "$run" > "$run/spark.log" 2>&1
cat "$run/result.json"
