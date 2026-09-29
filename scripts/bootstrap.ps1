$ErrorActionPreference='Stop'
& wsl -d Ubuntu-26.04 --cd /mnt/d/Projects/opensource-pulse-offline-warehouse --exec python3 scripts/fetch_runtime.py
if($LASTEXITCODE -ne 0){throw 'Download failed; see evidence'}
& wsl -d Ubuntu-26.04 --cd /mnt/d/Projects/opensource-pulse-offline-warehouse --exec bash scripts/start_cluster.sh
if($LASTEXITCODE -ne 0){throw 'Cluster start failed; see build/cluster/logs'}
