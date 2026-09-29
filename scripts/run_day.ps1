param(
 [ValidatePattern('^\d{4}-\d{2}-\d{2}$')][string]$Date,
 [ValidateRange(0,23)][Nullable[int]]$Hour,
 [ValidatePattern('^\d{4}-\d{2}-\d{2}$')][string]$StartDate,
 [ValidatePattern('^\d{4}-\d{2}-\d{2}$')][string]$EndDate,
 [switch]$CacheOnly
)
$ErrorActionPreference='Stop'
$project=Split-Path -Parent $PSScriptRoot
if($project -ne 'D:\Projects\opensource-pulse-offline-warehouse'){throw 'Unexpected project root'}
$config=@{}
if($Date){$config.process_date=$Date}
if($null -ne $Hour){$config.process_hour=[int]$Hour}
if($StartDate){$config.start_date=$StartDate}
if($EndDate){$config.end_date=$EndDate}
if($CacheOnly){$config.acquisition_policy='cache_only'}
$relative='build/requests/'+[guid]::NewGuid().ToString('N')+'.json'
$path=Join-Path $project $relative
[IO.Directory]::CreateDirectory([IO.Path]::GetDirectoryName($path))|Out-Null
[IO.File]::WriteAllText($path,($config|ConvertTo-Json -Compress),[Text.UTF8Encoding]::new($false))
& wsl -d Ubuntu-26.04 --cd /mnt/d/Projects/opensource-pulse-offline-warehouse --exec bash scripts/trigger_day.sh $relative
if($LASTEXITCODE -ne 0){throw 'Airflow DAG failed; inspect evidence/m2 and build/airflow/logs'}
