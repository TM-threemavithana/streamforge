# PowerShell script for StreamForge database backup and disaster recovery drill
param(
    [string]$Container = "docker-postgres-1",
    [string]$User = "streamforge",
    [switch]$Drill
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Definition
$RepoRoot = Split-Path -Parent $ScriptDir

if ($Drill) {
    Write-Host "[*] Executing full DR drill using python harness..." -ForegroundColor Cyan
    python (Join-Path $ScriptDir "backup_restore_drill.py")
    exit $LASTEXITCODE
}

$Timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$BackupDir = Join-Path $RepoRoot ".tmp\backups"
if (-not (Test-Path $BackupDir)) {
    New-Item -ItemType Directory -Path $BackupDir -Force | Out-Null
}

$CoreFile = Join-Path $BackupDir "streamforge_core_$Timestamp.sql"
$AlertsFile = Join-Path $BackupDir "streamforge_alerts_$Timestamp.sql"

Write-Host "[*] Backing up StreamForge Core database..." -ForegroundColor Yellow
docker exec $Container pg_dump -U $User streamforge > $CoreFile
$CoreHash = (Get-FileHash -Path $CoreFile -Algorithm SHA256).Hash
Write-Host "[+] Core backup created: $CoreFile (SHA-256: $CoreHash)" -ForegroundColor Green

Write-Host "[*] Backing up StreamForge Alerts database..." -ForegroundColor Yellow
docker exec $Container pg_dump -U $User streamforge_alerts > $AlertsFile
$AlertsHash = (Get-FileHash -Path $AlertsFile -Algorithm SHA256).Hash
Write-Host "[+] Alerts backup created: $AlertsFile (SHA-256: $AlertsHash)" -ForegroundColor Green

Write-Host "[+] Database backups completed successfully." -ForegroundColor Green
