<#
.SYNOPSIS
Launches three independent PAPER engine instances (BTCUSDT, LITUSDT, ZECUSDT) and
one unified multi-symbol paper dashboard process for Escanor LiveBot.

.PARAMETER DryRunOnly
Runs all pre-launch validations without starting any background processes.

.PARAMETER BindHost
IP address to bind the dashboard HTTP server to (default: 127.0.0.1).

.PARAMETER Port
Port to bind the dashboard HTTP server to (default: 8080).
#>

param(
    [switch]$DryRunOnly = $false,
    [string]$BindHost = "127.0.0.1",
    [int]$Port = 8080
)

# param block ends
$repoRoot = Split-Path -Parent $PSScriptRoot
$logsDir = Join-Path $repoRoot "logs"

Write-Host "========================================================"
Write-Host "  Escanor BTC, LIT, ZEC PAPER and Dashboard Launcher"
Write-Host "========================================================"
Write-Host "Repository root: $repoRoot"
Write-Host "Dashboard URL:   http://${BindHost}:${Port}"
Write-Host "Notice:          SIMULATED PAPER ACCOUNTS - NO REAL ORDERS"
Write-Host ""

# Ensure logs directory exists
if (-not (Test-Path $logsDir)) {
    New-Item -ItemType Directory -Path $logsDir -Force | Out-Null
}

# Verify python executable is available
$pythonCmd = Get-Command python -ErrorAction SilentlyContinue
if (-not $pythonCmd) {
    Write-Host "[FATAL ERROR] python executable was not found on PATH." -ForegroundColor Red
    exit 1
}

$engineConfigs = @(
    @{ Name = "btc-paper"; Symbol = "BTCUSDT"; Mode = "PAPER"; Config = "config\btc-paper.json"; DB = "data/btc_paper.db" },
    @{ Name = "lit-paper"; Symbol = "LITUSDT"; Mode = "PAPER"; Config = "config\lit-paper.json"; DB = "data/lit_paper.db" },
    @{ Name = "zec-paper"; Symbol = "ZECUSDT"; Mode = "PAPER"; Config = "config\zec-paper.json"; DB = "data/zec_paper.db" }
)

Write-Host "Phase 1: Pre-launch validation of engines and dashboard..."
Write-Host ""

$resolvedConfigs = @()
$seenDatabases = @()

foreach ($eng in $engineConfigs) {
    $configFile = Join-Path $repoRoot $eng.Config
    Write-Host "  [$($eng.Symbol) $($eng.Mode)] Validating $($eng.Config)..."

    if (-not (Test-Path $configFile)) {
        Write-Host "    [ERROR] Config file not found: $configFile" -ForegroundColor Red
        exit 1
    }

    # Run dry-run validation using engine CLI
    Push-Location $repoRoot
    & python -m live_engine.main --config $configFile --dry-run 2>&1 | Out-Null
    $exitCode = $LASTEXITCODE
    Pop-Location

    if ($exitCode -ne 0) {
        Write-Host "    [ERROR] Dry-run validation failed for $($eng.Config) (exit code: $exitCode)" -ForegroundColor Red
        exit 1
    }

    # Verify database isolation and uniqueness
    $cfgJson = Get-Content $configFile -Raw | ConvertFrom-Json
    $dbPath = $cfgJson.event_store_path
    $resolvedDb = [System.IO.Path]::GetFullPath((Join-Path $repoRoot $dbPath))
    $dataDir = [System.IO.Path]::GetFullPath((Join-Path $repoRoot "data"))

    if (-not $resolvedDb.StartsWith($dataDir, [System.StringComparison]::OrdinalIgnoreCase) -or $resolvedDb -eq $dataDir) {
        Write-Host "    [ERROR] Database path '$dbPath' must resolve strictly underneath 'data/'." -ForegroundColor Red
        exit 1
    }

    if ($seenDatabases -contains $resolvedDb) {
        Write-Host "    [ERROR] Duplicate database path detected: $resolvedDb" -ForegroundColor Red
        exit 1
    }

    $seenDatabases += $resolvedDb
    $resolvedConfigs += $configFile
    Write-Host "    [OK] Config and DB valid ($dbPath)"
}

# Pre-validate dashboard multi-config aggregator
Write-Host "  [DASHBOARD] Validating multi-symbol aggregator configs..."
Push-Location $repoRoot
& python -c "import sys; from live_engine.dashboard import validate_multi_symbol_dashboard_configs; validate_multi_symbol_dashboard_configs(sys.argv[1:])" config/btc-paper.json config/lit-paper.json config/zec-paper.json >$null 2>&1
$dashValidateExit = $LASTEXITCODE
Pop-Location

if ($dashValidateExit -ne 0) {
    Write-Host "    [ERROR] Multi-symbol dashboard configuration validation failed." -ForegroundColor Red
    exit 1
}
Write-Host "    [OK] Dashboard multi-config validation passed."

if ($DryRunOnly) {
    Write-Host ""
    Write-Host "[SUCCESS] All pre-launch validations passed successfully (DryRunOnly specified)."
    exit 0
}

Write-Host ""
Write-Host "Phase 2: Launching processes..."
Write-Host ""

$launchedProcesses = @()

function Stop-LaunchedProcesses {
    param([array]$processes)
    if ($processes.Count -gt 0) {
        Write-Host "Rolling back launched processes..." -ForegroundColor Yellow
        foreach ($p in $processes) {
            try {
                Stop-Process -Id $p.PID -Force -ErrorAction SilentlyContinue
                Write-Host "  Terminated PID $($p.PID)"
            } catch {
                # Ignore errors during rollback
            }
        }
    }
}

# 1. Launch 3 PAPER engine instances
foreach ($eng in $engineConfigs) {
    $configFile = Join-Path $repoRoot $eng.Config
    $stdout = Join-Path $logsDir "$($eng.Name).stdout.log"
    $stderr = Join-Path $logsDir "$($eng.Name).stderr.log"

    Write-Host "  Launching $($eng.Symbol) PAPER engine..."
    try {
        $proc = Start-Process python `
            -ArgumentList "-u", "-m", "live_engine.main", "--config", $configFile `
            -WindowStyle Hidden -PassThru `
            -RedirectStandardOutput $stdout `
            -RedirectStandardError $stderr `
            -WorkingDirectory $repoRoot

        if (-not $proc -or $proc.HasExited) {
            throw "Process exited immediately after start."
        }

        $record = @{
            Component = "Engine"
            Symbol    = $eng.Symbol
            Mode      = $eng.Mode
            PID       = $proc.Id
            Database  = $eng.DB
            Stdout    = $stdout
            Stderr    = $stderr
        }
        $launchedProcesses += $record
        Write-Host "    [OK] $($eng.Symbol) PID: $($proc.Id)"
    } catch {
        Write-Host "    [FATAL] Failed to launch $($eng.Symbol) engine: $_" -ForegroundColor Red
        Stop-LaunchedProcesses -processes $launchedProcesses
        exit 1
    }
}

# 2. Launch unified multi-symbol dashboard
$dashStdout = Join-Path $logsDir "paper-dashboard.stdout.log"
$dashStderr = Join-Path $logsDir "paper-dashboard.stderr.log"

Write-Host "  Launching multi-symbol web dashboard on http://${BindHost}:${Port}..."
try {
    $dashArgs = @(
        "-u", "-m", "live_engine.main",
        "--dashboard",
        "--dashboard-config", (Join-Path $repoRoot "config\btc-paper.json"),
        "--dashboard-config", (Join-Path $repoRoot "config\lit-paper.json"),
        "--dashboard-config", (Join-Path $repoRoot "config\zec-paper.json"),
        "--host", $BindHost,
        "--port", "$Port"
    )

    $dashProc = Start-Process python `
        -ArgumentList $dashArgs `
        -WindowStyle Hidden -PassThru `
        -RedirectStandardOutput $dashStdout `
        -RedirectStandardError $dashStderr `
        -WorkingDirectory $repoRoot

    if (-not $dashProc -or $dashProc.HasExited) {
        throw "Dashboard process exited immediately after start."
    }

    $dashRecord = @{
        Component = "Dashboard"
        Symbol    = "BTC, LIT, ZEC"
        Mode      = "OBSERVABILITY"
        PID       = $dashProc.Id
        Database  = "Isolated DBs"
        Stdout    = $dashStdout
        Stderr    = $dashStderr
    }
    $launchedProcesses += $dashRecord
    Write-Host "    [OK] Dashboard PID: $($dashProc.Id)"
} catch {
    Write-Host "    [FATAL] Failed to launch multi-symbol dashboard: $_" -ForegroundColor Red
    Stop-LaunchedProcesses -processes $launchedProcesses
    exit 1
}

# Startup liveness probe. This only proves the processes launched; ongoing supervision
# is escanor-control.ps1 -Supervise, which watches heartbeats and crashes continuously.
Start-Sleep -Seconds 2
$deadProcesses = @()
foreach ($p in $launchedProcesses) {
    $live = Get-Process -Id $p.PID -ErrorAction SilentlyContinue
    if (-not $live -or $live.HasExited) {
        $deadProcesses += $p
    }
}

if ($deadProcesses.Count -gt 0) {
    Write-Host "[FATAL ERROR] One or more processes terminated prematurely:" -ForegroundColor Red
    foreach ($dp in $deadProcesses) {
        $comp = $dp.Component
        $sym = $dp.Symbol
        $dpid = $dp.PID
        $logf = $dp.Stderr
        Write-Host "  - $comp ($sym, PID: $dpid) terminated. Check logs: $logf" -ForegroundColor Red
    }
    Stop-LaunchedProcesses -processes $launchedProcesses
    exit 1
}

# Output Summary Table
Write-Host ""
Write-Host "========================================================================================"
Write-Host "                                ACTIVE PROCESS SUMMARY                                  "
Write-Host "========================================================================================"
Write-Host ("{0,-12} {1,-10} {2,-8} {3,-8} {4,-22} {5}" -f "COMPONENT", "SYMBOL", "MODE", "PID", "DATABASE", "LOG (STDOUT)")
Write-Host ("-" * 88)
foreach ($p in $launchedProcesses) {
    $stdoutName = Split-Path $p.Stdout -Leaf
    Write-Host ("{0,-12} {1,-10} {2,-8} {3,-8} {4,-22} {5}" -f $p.Component, $p.Symbol, $p.Mode, $p.PID, $p.Database, $stdoutName)
}
Write-Host ("-" * 88)
Write-Host "Dashboard URL:  http://${BindHost}:${Port}"
Write-Host "Kill Switch:    .kill_switch (Shared across all instances)"
Write-Host "Notice:         SIMULATED PAPER ACCOUNTS - NO REAL ORDERS"
Write-Host "========================================================================================"
Write-Host ""

# Write the process manifest the supervisor and the graceful stop path both read.
$manifestPath = Join-Path $logsDir "escanor-processes.json"
$launchedProcesses | ConvertTo-Json -Depth 4 | Out-File -FilePath $manifestPath -Encoding utf8
Write-Host "Process manifest: $manifestPath"
Write-Host ""

Write-Host "To supervise the running instances (crash, stale heartbeat, kill switch):"
Write-Host "  .\scripts\escanor-control.ps1 -Supervise" -ForegroundColor Green
Write-Host ""
Write-Host "To stop everything cleanly (graceful, with a bounded forced fallback):"
Write-Host "  .\scripts\escanor-control.ps1 -Stop" -ForegroundColor Green
Write-Host ""
Write-Host "Stop-Process -Force is a last resort, not a clean shutdown: it can leave a"
Write-Host "half-applied fill and an unflushed heartbeat for reconciliation to repair."
Write-Host ""
