<#
.SYNOPSIS
Launches four concurrent instances of Escanor LiveBot for LIT/ZEC in SHADOW and PAPER modes.
#>

param([switch]$DryRunOnly = $false)

$repoRoot = Split-Path -Parent $PSScriptRoot
$logsDir = "$repoRoot\logs"

Write-Host "========================================================"
Write-Host "  Escanor LIT/ZEC SHADOW & PAPER Launcher"
Write-Host "========================================================"
Write-Host "Repository root: $repoRoot"
Write-Host ""

if (-not (Test-Path $logsDir)) {
    New-Item -ItemType Directory -Path $logsDir -Force | Out-Null
}

$configs = @("lit-shadow", "lit-paper", "zec-shadow", "zec-paper")
$instances = @(
    @{ Config = "lit-shadow"; Mode = "SHADOW"; Symbol = "LITUSDT"; DB = "data/lit_shadow.db" },
    @{ Config = "lit-paper"; Mode = "PAPER"; Symbol = "LITUSDT"; DB = "data/lit_paper.db" },
    @{ Config = "zec-shadow"; Mode = "SHADOW"; Symbol = "ZECUSDT"; DB = "data/zec_shadow.db" },
    @{ Config = "zec-paper"; Mode = "PAPER"; Symbol = "ZECUSDT"; DB = "data/zec_paper.db" }
)

Write-Host "Phase 1: Dry-run validation of all configurations..."
Write-Host ""

$validConfigs = @()
$seenDatabases = @()

foreach ($cfg in $configs) {
    $configFile = "$repoRoot\config\$cfg.json"
    $inst = $instances | Where-Object {$_.Config -eq $cfg}

    Write-Host "  [$($inst.Symbol) $($inst.Mode)] $cfg..."

    if (-not (Test-Path $configFile)) {
        Write-Host "    [ERROR] Config not found" -ForegroundColor Red
        continue
    }

    # Dry-run validation with correct working directory
    Push-Location $repoRoot
    &python -m live_engine.main --config $configFile --dry-run >$null 2>&1
    $exitCode = $LASTEXITCODE
    Pop-Location

    if ($exitCode -eq 0) {
        # Extract database path from config JSON to check for duplicates
        $jsonContent = Get-Content $configFile | ConvertFrom-Json
        $dbPath = $jsonContent.event_store_path
        if ($seenDatabases -contains $dbPath) {
            Write-Host "    [ERROR] Duplicate database path: $dbPath" -ForegroundColor Red
            continue
        }
        $seenDatabases += $dbPath
        Write-Host "    [OK] (DB: $dbPath)"
        $validConfigs += $cfg
    } else {
        Write-Host "    [ERROR] exit code $exitCode" -ForegroundColor Red
    }
}

Write-Host ""
Write-Host "Validation: $($validConfigs.Count)/4 passed"

if ($validConfigs.Count -ne 4) {
    Write-Host "ABORTED: Configs validation failed" -ForegroundColor Red
    exit 1
}

if ($DryRunOnly) {
    Write-Host "[OK] Dry-run validation complete"
    exit 0
}

Write-Host ""
Write-Host "Phase 2: Launching four instances..."
Write-Host ""

$launched = @()

foreach ($cfg in $validConfigs) {
    $configFile = "$repoRoot\config\$cfg.json"
    $stdout = "$logsDir\$cfg.stdout.log"
    $stderr = "$logsDir\$cfg.stderr.log"
    $inst = $instances | Where-Object {$_.Config -eq $cfg}

    Write-Host "  [$($inst.Symbol) $($inst.Mode)] Launching..."

    $proc = Start-Process python `
        -ArgumentList "-u", "-m", "live_engine.main", "--config", $configFile `
        -WindowStyle Hidden -PassThru `
        -RedirectStandardOutput $stdout `
        -RedirectStandardError $stderr `
        -WorkingDirectory $repoRoot

    $inst | Add-Member -MemberType NoteProperty -Name PID -Value $proc.Id -Force
    $inst | Add-Member -MemberType NoteProperty -Name Stdout -Value $stdout -Force
    $launched += $inst

    Write-Host "    [OK] PID $($proc.Id)"
}

Write-Host ""
Write-Host "========================================================"
Write-Host "  Instance Status"
Write-Host "========================================================"
Write-Host ""
foreach ($inst in $launched) {
    Write-Host "[$($inst.Symbol) $($inst.Mode)]"
    Write-Host "  PID:      $($inst.PID)"
    Write-Host "  Database: $($inst.DB)"
    Write-Host "  StdOut:   $($inst.Stdout)"
    Write-Host "  StdErr:   $($logsDir)\$($inst.Config).stderr.log"
    Write-Host ""
}

Write-Host "========================================================"
Write-Host "  Shutdown"
Write-Host "========================================================"
Write-Host ""
$manifestPath = Join-Path $logsDir "escanor-processes.json"
$launched | ConvertTo-Json -Depth 4 | Out-File -FilePath $manifestPath -Encoding utf8
Write-Host "Process manifest: $manifestPath"
Write-Host ""
Write-Host "Supervise the running instances:"
Write-Host "  .\scripts\escanor-control.ps1 -Supervise" -ForegroundColor Green
Write-Host ""
Write-Host "Stop all instances cleanly (graceful, bounded forced fallback):"
Write-Host "  .\scripts\escanor-control.ps1 -Stop" -ForegroundColor Green
Write-Host ""
Write-Host "Stop-Process -Force is a last resort, not a clean shutdown."
Write-Host ""
