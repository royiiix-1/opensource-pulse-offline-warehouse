#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if systemctl --user is-active --quiet osp-offline-cluster.service; then
  echo "Project cluster already active"
  exit 0
fi
if systemctl --user is-failed --quiet osp-offline-cluster.service; then systemctl --user reset-failed osp-offline-cluster.service; fi
if [[ -d build/cluster/logs ]]; then
  archive="build/cluster/logs-history/$(date -u +%Y%m%dT%H%M%SZ)-$$"
  mkdir -p "$archive"
  cp -a build/cluster/logs "$archive/"
fi
python3 scripts/configure_runtime.py
for port in 19000 19870 19866 19864 19867 18032 18030 18031 18033 18088 18041 18040 18042 19083; do
  if ss -ltnH "sport = :$port" | grep -q .; then echo "Port occupied: $port"; exit 1; fi
done
systemctl --user set-property --runtime osp-offline.slice MemoryMax=10G CPUQuota=600%
systemd-run --user --slice=osp-offline.slice --unit=osp-offline-cluster --property=MemoryMax=10G --property=CPUQuota=600% --property=TasksMax=2048 --property=KillMode=control-group --working-directory="$PWD" /bin/bash "$PWD/scripts/cluster_supervisor.sh"
