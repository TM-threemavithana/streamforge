# StreamForge 2.0: Unified Release Gate PowerShell Wrapper
Param(
    [switch]$VerboseOutput
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Split-Path -Parent $ScriptDir

Write-Host "=================================================================" -ForegroundColor Cyan
Write-Host " STREAMFORGE 2.0: PRODUCTION UNIFIED RELEASE GATE (PowerShell)" -ForegroundColor Cyan
Write-Host "=================================================================" -ForegroundColor Cyan

python "$RepoRoot/scripts/release_gate.py"
if ($LASTEXITCODE -ne 0) {
    Write-Host "[!] Release Gate Failed with exit code $LASTEXITCODE" -ForegroundColor Red
    exit $LASTEXITCODE
}

Write-Host "[+] Release Gate Passed Successfully." -ForegroundColor Green
exit 0
