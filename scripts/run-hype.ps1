<#
.SYNOPSIS
Launch one isolated HYPE engine and its read-only dashboard in SHADOW, PAPER, TESTNET or LIVE.

.DESCRIPTION
Each mode owns its own database (data/hype_<mode>.db) and its own dashboard. Before anything
starts the script re-verifies the frozen benchmark, dry-runs the engine and validates the
dashboard, so a broken manifest, a stale parity report or a failed safety gate stops the
launch instead of the first trade.

TESTNET and LIVE additionally require credentials in the environment; LIVE also requires the
explicit operator acknowledgement and an interactive confirmation.

.PARAMETER Mode
SHADOW, PAPER, TESTNET or LIVE (default: PAPER).

.PARAMETER Port
Dashboard port (default: 8083).

.PARAMETER DryRunOnly
Validate everything and exit without launching any process.

.PARAMETER Force
Skip the interactive LIVE confirmation. Intended for a supervised, already-reviewed restart.
#>
param(
    [ValidateSet("SHADOW", "PAPER", "TESTNET", "LIVE")][string]$Mode = "PAPER",
    [int]$Port = 8083,
    [switch]$DryRunOnly = $false,
    [switch]$Force = $false
)

$repoRoot = Split-Path -Parent $PSScriptRoot
$logsDir = Join-Path $repoRoot "logs"
$modeLower = $Mode.ToLowerInvariant()
$config = Join-Path $repoRoot "config\hype-$modeLower.json"
$database = "data/hype_$modeLower.db"
New-Item -ItemType Directory -Path $logsDir -Force | Out-Null

if (-not (Test-Path $config)) { throw "HYPE $Mode configuration not found: $config" }

# Authenticated modes need credentials before anything connects. They come only from the
# environment; a configuration file carrying them is rejected at load.
# Auto-populate process environment from repo .env if present and unset:
$envPath = Join-Path $repoRoot ".env"
if (Test-Path $envPath) {
    Get-Content $envPath | ForEach-Object {
        $line = $_.Trim()
        if ($line -and -not $line.StartsWith("#") -and $line -match "^([^=]+)=(.*)$") {
            $k = $matches[1].Trim()
            $v = $matches[2].Trim().Trim('"').Trim("'")
            if (-not [System.Environment]::GetEnvironmentVariable($k, "Process")) {
                [System.Environment]::SetEnvironmentVariable($k, $v, "Process")
            }
        }
    }
}
# TESTNET accepts the dedicated testnet pair the broker prefers, so a testnet key and a
# funded key can both be set without either shadowing the other.
if ($Mode -eq "TESTNET") {
    $haveTestnet = $env:BINANCE_TESTNET_API_KEY -and $env:BINANCE_TESTNET_API_SECRET
    $haveGeneric = $env:BINANCE_API_KEY -and $env:BINANCE_API_SECRET
    if (-not ($haveTestnet -or $haveGeneric)) {
        throw "TESTNET requires BINANCE_TESTNET_API_KEY + BINANCE_TESTNET_API_SECRET (preferred), or BINANCE_API_KEY + BINANCE_API_SECRET, in the environment. Testnet keys come from testnet.binancefuture.com and are separate from your funded keys."
    }
}
if ($Mode -eq "LIVE") {
    if (-not $env:BINANCE_API_KEY) { throw "LIVE requires BINANCE_API_KEY in the environment." }
    if (-not $env:BINANCE_API_SECRET) { throw "LIVE requires BINANCE_API_SECRET in the environment." }
}

if ($Mode -eq "LIVE") {
    if ($env:ESCANOR_LIVE_TRADING_ENABLED -ne "true") {
        throw "LIVE refused: ESCANOR_LIVE_TRADING_ENABLED is not 'true'. Funded trading requires an explicit operator acknowledgement."
    }
    if (-not $DryRunOnly) {
        Write-Host "WARNING: YOU ARE ABOUT TO LAUNCH FUNDED LIVE TRADING ON HYPEUSDT WITH 12 SLOTS AND 3X LEVERAGE." -ForegroundColor Red
        Write-Host "         Database: $database   Canary ceiling: `$100 USD" -ForegroundColor Red
        if (-not $Force) {
            $answer = Read-Host "Proceed with funded LIVE execution? [y/N]"
            if ($answer -ne "y" -and $answer -ne "Y") { Write-Host "Aborted by operator." -ForegroundColor Yellow; exit 1 }
        }
    }
}

Push-Location $repoRoot
try {
    & python scripts\verify_hype_benchmark.py
    if ($LASTEXITCODE -ne 0) { throw "HYPE benchmark verification failed" }
    & python -m live_engine.main --config $config --dry-run
    if ($LASTEXITCODE -ne 0) { throw "HYPE $Mode dry-run failed" }
    & python -c "from live_engine.dashboards.hype_dashboard import HYPEDashboard; HYPEDashboard(r'$config').get_dashboard_payload()"
    if ($LASTEXITCODE -ne 0) { throw "HYPE dashboard validation failed" }
} finally {
    Pop-Location
}

if ($DryRunOnly) {
    Write-Host "HYPE $Mode benchmark, engine, database, and dashboard: PASS" -ForegroundColor Green
    exit 0
}

$engineOut = Join-Path $logsDir "hype-$modeLower.stdout.log"
$engineErr = Join-Path $logsDir "hype-$modeLower.stderr.log"
$dashOut = Join-Path $logsDir "hype-$modeLower-dashboard.stdout.log"
$dashErr = Join-Path $logsDir "hype-$modeLower-dashboard.stderr.log"
$engine = Start-Process python -ArgumentList "-u", "-m", "live_engine.main", "--config", $config `
    -WorkingDirectory $repoRoot -WindowStyle Hidden -PassThru `
    -RedirectStandardOutput $engineOut -RedirectStandardError $engineErr
$dashboard = Start-Process python -ArgumentList "-u", "-m", "live_engine.dashboards.hype_dashboard", `
    "--config", $config, "--host", "127.0.0.1", "--port", "$Port" `
    -WorkingDirectory $repoRoot -WindowStyle Hidden -PassThru `
    -RedirectStandardOutput $dashOut -RedirectStandardError $dashErr

$processes = @(
    [ordered]@{ Component = "ENGINE"; Symbol = "HYPEUSDT"; Mode = $Mode; PID = $engine.Id; Database = $database; Stdout = $engineOut; Stderr = $engineErr },
    [ordered]@{ Component = "DASHBOARD"; Symbol = "HYPEUSDT"; Mode = $Mode; PID = $dashboard.Id; Database = $null; Stdout = $dashOut; Stderr = $dashErr }
)
$manifest = Join-Path $logsDir "hype-processes.json"
$processes | ConvertTo-Json -Depth 3 | Out-File $manifest -Encoding utf8
Write-Host "HYPE $Mode engine PID $($engine.Id); dashboard PID $($dashboard.Id)" -ForegroundColor Green
Write-Host "Dashboard: http://127.0.0.1:$Port"
Write-Host "Process manifest: $manifest"
Write-Host ".\scripts\escanor-control.ps1 -Supervise -Manifest $manifest"
Write-Host ".\scripts\escanor-control.ps1 -Stop -Manifest $manifest"
