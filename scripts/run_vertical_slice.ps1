param(
 [Parameter(Mandatory=$true)][ValidatePattern('^\d{4}-\d{2}-\d{2}$')][string]$Date,
 [Parameter(Mandatory=$true)][ValidateRange(0,23)][int]$Hour,
 [switch]$Force,
 [ValidateSet('ods','dwd')][string]$FailAfter
)
$ErrorActionPreference='Stop'
$arguments=@($Date,[string]$Hour)
if($Force){$arguments+='--force'}
if($FailAfter){$arguments+=@('--fail-after',$FailAfter)}
& wsl -d Ubuntu-26.04 --cd /mnt/d/Projects/opensource-pulse-offline-warehouse --exec bash scripts/run_slice.sh @arguments
if($LASTEXITCODE -ne 0){throw 'Slice failed; inspect evidence/slice-*'}
