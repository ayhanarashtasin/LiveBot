$repoRoot = "C:\LiveBot"
$logsDir = Join-Path $repoRoot "logs"
$config = Join-Path $repoRoot "config\hype-live.json"
$engineOut = Join-Path $logsDir "hype-live.stdout.log"
$engineErr = Join-Path $logsDir "hype-live.stderr.log"

# Load .env
$envPath = Join-Path $repoRoot ".env"
Get-Content $envPath | ForEach-Object {
    $line = $_.Trim()
    if ($line -and -not $line.StartsWith("#") -and $line -match "^([^=]+)=(.*)$") {
        $k = $matches[1].Trim()
        $v = $matches[2].Trim().Trim('"').Trim("'")
        [System.Environment]::SetEnvironmentVariable($k, $v, "Process")
    }
}
$env:PYTHONPATH = $repoRoot
$env:ESCANOR_LIVE_TRADING_ENABLED = "true"

# Start detached engine process
$engine = Start-Process python -ArgumentList "-u", "-m", "live_engine.main", "--config", $config `
    -WorkingDirectory $repoRoot -WindowStyle Hidden -PassThru `
    -RedirectStandardOutput $engineOut -RedirectStandardError $engineErr

Write-Host "Started engine with PID: $($engine.Id)"
Start-Sleep -Seconds 5
$proc = Get-Process -Id $engine.Id -ErrorAction SilentlyContinue
if ($proc) {
    Write-Host "Engine PID $($engine.Id) is running healthy!" -ForegroundColor Green
} else {
    Write-Host "Engine failed to stay running. Check $engineErr and $engineOut" -ForegroundColor Red
}
