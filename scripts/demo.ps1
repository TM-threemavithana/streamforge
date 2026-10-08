param(
    [string]$DatabaseUrl = 'postgres://streamforge:local-development-only@127.0.0.1:5433/streamforge?sslmode=disable'
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$temporaryRoot = Join-Path ([System.IO.Path]::GetTempPath()) ("streamforge-demo-" + [guid]::NewGuid().ToString('N'))
$coreProcess = $null

try {
    New-Item -ItemType Directory -Path $temporaryRoot | Out-Null
    Push-Location (Join-Path $root 'deploy\docker')
    docker compose up -d --wait postgres
    if ($LASTEXITCODE -ne 0) { throw 'PostgreSQL did not become healthy' }
    Pop-Location

    $fixture = Join-Path $temporaryRoot 'known-100.parquet'
    python (Join-Path $root 'scripts\create_demo_fixture.py') $fixture | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'Could not create the known fixture' }
    $sourceHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $fixture).Hash.ToLowerInvariant()
    $sourceSize = (Get-Item -LiteralPath $fixture).Length

    $coreBinary = Join-Path $temporaryRoot 'streamforge-core.exe'
    Push-Location (Join-Path $root 'services\core-go')
    go build -o $coreBinary ./cmd/core
    if ($LASTEXITCODE -ne 0) { throw 'Could not build the core service' }
    Pop-Location
    $env:STREAMFORGE_DATABASE_URL = $DatabaseUrl
    $coreProcess = Start-Process -FilePath $coreBinary -PassThru -WindowStyle Hidden

    $ready = $false
    for ($attempt = 0; $attempt -lt 30; $attempt++) {
        try {
            $health = Invoke-RestMethod -Uri 'http://127.0.0.1:8080/health/ready' -TimeoutSec 2
            if ($health.status -eq 'ready') { $ready = $true; break }
        } catch { Start-Sleep -Milliseconds 500 }
    }
    if (-not $ready) { throw 'Core service did not become ready' }

    $dataset = Invoke-RestMethod -Method Post -ContentType 'application/json' -Uri 'http://127.0.0.1:8080/api/v1/datasets' -Body (@{
        source_type = 'nyc-yellow'; source_sha256 = $sourceHash; source_schema_version = 'v1'
        filename = 'known-100.parquet'; source_size_bytes = $sourceSize
    } | ConvertTo-Json)
    $run = Invoke-RestMethod -Method Post -ContentType 'application/json' -Uri 'http://127.0.0.1:8080/api/v1/runs' -Body (@{ dataset_id = $dataset.id } | ConvertTo-Json)

    $priorPythonPath = $env:PYTHONPATH
    $env:PYTHONPATH = (Join-Path $root 'tools\replay-python\src') + [IO.Path]::PathSeparator + $priorPythonPath
    python -m streamforge_replay.replay_cli $fixture --dataset-id $dataset.id --run-id $run.id --batch-size 17 | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'Replay failed' }
    $env:PYTHONPATH = $priorPythonPath

    $completed = Invoke-RestMethod -Method Post -ContentType 'application/json' -Uri ("http://127.0.0.1:8080/api/v1/runs/{0}/complete" -f $run.id) -Body (@{ expected_input_count = 100 } | ConvertTo-Json)
    $analytics = Invoke-RestMethod -Uri ("http://127.0.0.1:8080/api/v1/analytics/zone-hourly?dataset_id={0}&start=2024-01-08T00:00:00Z&end=2024-01-09T00:00:00Z&limit=1000" -f $dataset.id)
    $analyticsTrips = ($analytics.items | Measure-Object -Property trip_count -Sum).Sum
    if ($completed.input_count -ne 100 -or $completed.accepted_count -ne 90 -or $completed.rejected_count -ne 10 -or $analyticsTrips -ne 90) {
        throw "Reconciliation failed: run=$($completed | ConvertTo-Json -Compress) analytics_trips=$analyticsTrips"
    }
    [pscustomobject]@{
        dataset_id = $dataset.id; run_id = $run.id; input = 100; accepted = 90; rejected = 10
        analytics_trips = $analyticsTrips; result = 'PASS'
    } | ConvertTo-Json
}
finally {
    if ($coreProcess -and -not $coreProcess.HasExited) { Stop-Process -Id $coreProcess.Id -Force }
    if ((Test-Path -LiteralPath $temporaryRoot) -and $temporaryRoot.StartsWith([System.IO.Path]::GetTempPath())) {
        Remove-Item -LiteralPath $temporaryRoot -Recurse -Force
    }
}
