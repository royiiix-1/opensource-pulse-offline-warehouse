#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
source config/runtime/env.sh
systemd-run --user --scope --slice=osp-offline.slice --quiet "$PYSPARK_PYTHON" scripts/test_spark_fixture.py