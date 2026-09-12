<#
.SYNOPSIS
Supervises and gracefully stops Escanor engine processes.

.DESCRIPTION
A launcher's two-second "did it start" check proves only that a process launched. This
script keeps watching: it detects a crashed child, a heartbeat that stopped advancing in
the instance database, and an engaged kill switch, and it writes every finding to the
same structured incident sink the engine uses (logs/incidents.jsonl).

-Stop performs a graceful shutdown: it asks each process to stop, waits up to
-StopTimeoutSeconds for the engine to flush state and close its streams, and only then
falls back to a forced termination for anything still running. Stop-Process -Force on its
own is not a clean shutdown - it can leave a half-applied fill, an unflushed heartbeat and
orphaned background tasks for reconciliation to untangle on the next start.

.PARAMETER Supervise
Watch the processes listed in the manifest until Ctrl+C.

.PARAMETER Stop
Gracefully stop every process in the manifest.

.PARAMETER Manifest
Path to the launcher-written process manifest (default: logs/escanor-processes.json).

.PARAMETER IntervalSeconds
Supervision interval (default: 15).

.PARAMETER StopTimeoutSeconds
How long a process may take to shut down cleanly before it is forced (default: 30).

.PARAMETER MaxHeartbeatAgeSeconds
Heartbeat age in an instance database beyond which the engine is considered stalled
(default: 300).
#>

param(
    [switch]$Supervise = $false,
    [switch]$Stop = $false,
    [string]$Manifest = "",
    [int]$IntervalSeconds = 15,
    [int]$StopTimeoutSeconds = 30,
    [int]$MaxHeartbeatAgeSeconds = 300
)

$repoRoot = Split-Path -Parent $PSScriptRoot
$logsDir = Join-Path $repoRoot "logs"
if (-not $Manifest) { $Manifest = Join-Path $logsDir "escanor-processes.json" }
$incidentSink = Join-Path $logsDir "incidents.jsonl"

function Write-Incident {
    param([string]$Category, [string]$Severity, [string]$Message)

    $record = [ordered]@{
        kind         = "INCIDENT"
        category     = $Category
        severity     = $Severity
        message      = $Message
        source       = "supervisor"
        timestamp_ms = [int64]([DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds())
    }
    if (-not (Test-Path $logsDir)) { New-Item -ItemType Directory -Path $logsDir -Force | Out-Null }
    Add-Content -Path $incidentSink -Value ($record | ConvertTo-Json -Compress) -Encoding utf8
    $colour = if ($Severity -eq "CRITICAL") { "Red" } else { "Yellow" }
    Write-Host "[$Severity] $Category - $Message" -ForegroundColor $colour
}

function Get-Processes {
    if (-not (Test-Path $Manifest)) {
        Write-Host "[ERROR] Process manifest not found: $Manifest" -ForegroundColor Red
        Write-Host "        Launch the engines first; the launcher writes this file."
        exit 1
    }
    return (Get-Content $Manifest -Raw | ConvertFrom-Json)
}

function Get-HeartbeatAgeSeconds {
    param([string]$DbPath)

    $full = Join-Path $repoRoot $DbPath
    if (-not (Test-Path $full)) { return $null }
    $query = "SELECT MAX(last_beat_ms) FROM heartbeats;"
    $result = & python -c "import sqlite3,sys,time
try:
    c=sqlite3.connect('file:'+sys.argv[1].replace('\\','/')+'?mode=ro',uri=True,timeout=5)
    v=c.execute('SELECT MAX(last_beat_ms) FROM heartbeats;').fetchone()[0]
    print('' if v is None else round(time.time()-v/1000.0,1))
except Exception:
    print('')" $full 2>$null
    if (-not $result) { return $null }
    return [double]$result
}

function Invoke-Supervision {
    $entries = Get-Processes
    Write-Host "Supervising $($entries.Count) process(es) every $IntervalSeconds s. Ctrl+C to stop watching."
    $killSwitch = Join-Path $repoRoot ".kill_switch"
    $alerted = @{}

    while ($true) {
        foreach ($entry in $entries) {
            $name = "$($entry.Component) $($entry.Symbol)"
            $live = Get-Process -Id $entry.PID -ErrorAction SilentlyContinue

            if (-not $live) {
                if (-not $alerted.ContainsKey("crash:$($entry.PID)")) {
                    Write-Incident "PROCESS_CRASHED" "CRITICAL" "$name (PID $($entry.PID)) is no longer running. Log: $($entry.Stderr)"
                    $alerted["crash:$($entry.PID)"] = $true
                }
                continue
            }

            if ($entry.Database) {
                $age = Get-HeartbeatAgeSeconds -DbPath $entry.Database
                if ($null -ne $age -and $age -gt $MaxHeartbeatAgeSeconds) {
                    if (-not $alerted.ContainsKey("stale:$($entry.PID)")) {
                        Write-Incident "STALE_HEARTBEAT" "HIGH" "$name has not written a heartbeat for $age s (limit $MaxHeartbeatAgeSeconds s)."
                        $alerted["stale:$($entry.PID)"] = $true
                    }
                } elseif ($alerted.ContainsKey("stale:$($entry.PID)")) {
                    Write-Incident "STALE_HEARTBEAT_RECOVERED" "INFO" "$name is reporting heartbeats again."
                    $alerted.Remove("stale:$($entry.PID)")
                }
            }
        }

        if (Test-Path $killSwitch) {
            if (-not $alerted.ContainsKey("killswitch")) {
                Write-Incident "KILL_SWITCH_ACTIVE" "CRITICAL" "Kill switch is engaged; no instance will open new risk."
                $alerted["killswitch"] = $true
            }
        } elseif ($alerted.ContainsKey("killswitch")) {
            Write-Incident "KILL_SWITCH_CLEARED" "INFO" "Kill switch disengaged."
            $alerted.Remove("killswitch")
        }

        Start-Sleep -Seconds $IntervalSeconds
    }
}

function Invoke-GracefulStop {
    $entries = Get-Processes
    Write-Host "Requesting graceful shutdown of $($entries.Count) process(es)..."

    foreach ($entry in $entries) {
        $live = Get-Process -Id $entry.PID -ErrorAction SilentlyContinue
        if (-not $live) {
            Write-Host "  [$($entry.Component) $($entry.Symbol)] PID $($entry.PID) already stopped."
            continue
        }
        # CloseMainWindow reaches a console child as a close request; the engine's signal
        # handler turns that into a flush-and-exit.
        try { $live.CloseMainWindow() | Out-Null } catch { }
        try { Stop-Process -Id $entry.PID -ErrorAction SilentlyContinue } catch { }
        Write-Host "  [$($entry.Component) $($entry.Symbol)] stop requested (PID $($entry.PID))."
    }

    $deadline = (Get-Date).AddSeconds($StopTimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        $remaining = @($entries | Where-Object { Get-Process -Id $_.PID -ErrorAction SilentlyContinue })
        if ($remaining.Count -eq 0) {
            Write-Host "All processes shut down cleanly." -ForegroundColor Green
            return
        }
        Start-Sleep -Seconds 1
    }

    # Bounded fallback: anything that ignored the request is forced, and that is recorded.
    $stubborn = @($entries | Where-Object { Get-Process -Id $_.PID -ErrorAction SilentlyContinue })
    foreach ($entry in $stubborn) {
        Write-Incident "FORCED_TERMINATION" "HIGH" "$($entry.Component) $($entry.Symbol) (PID $($entry.PID)) did not stop within $StopTimeoutSeconds s and was forced. Expect reconciliation on next start."
        Stop-Process -Id $entry.PID -Force -ErrorAction SilentlyContinue
    }
    if ($stubborn.Count -gt 0) {
        Write-Host "Forced $($stubborn.Count) process(es) after the grace period." -ForegroundColor Yellow
    }
}

if ($Stop) {
    Invoke-GracefulStop
} elseif ($Supervise) {
    Invoke-Supervision
} else {
    Write-Host "Usage:"
    Write-Host "  .\scripts\escanor-control.ps1 -Supervise    # watch running instances"
    Write-Host "  .\scripts\escanor-control.ps1 -Stop         # graceful shutdown, forced fallback"
}
