param([ValidateSet('portable','full')][string]$Mode='full')
$ErrorActionPreference='Stop'
$root=Split-Path -Parent $PSScriptRoot
if($root -ne 'D:\Projects\opensource-pulse-offline-warehouse'){throw 'Use Python portable mode in another checkout; full mode is bound to the validated WSL deployment.'}
& wsl -d Ubuntu-26.04 --cd /mnt/d/Projects/opensource-pulse-offline-warehouse --exec bash scripts/verify_portfolio.sh --mode $Mode
if($LASTEXITCODE -ne 0){throw 'Validation failed; inspect build/validation/*/result.json. No download or rebuild was attempted.'}
