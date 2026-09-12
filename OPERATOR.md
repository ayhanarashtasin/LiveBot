# Escanor Live Engine Operator Runbook

Every command in this document was executed against this build on Windows 11 with
Python 3.14.3. Paths are repository-relative from `E:\EC\LiveBot`.

---

## 1. Installation

### Prerequisites

- Python 3.11 or newer (this build is tested on 3.14.3).
- The native **TA-Lib** C library, which `strategies/approved/supertrend_helper.py`
  depends on through the `TA-Lib` Python wrapper:
  - **Windows**: install a prebuilt `TA_Lib` wheel matching your Python version *before*
    running the requirements install.
  - **Linux**: build `ta-lib` 0.4.0+ from source (`./configure --prefix=/usr && make &&
    sudo make install`), then install the wrapper.

### Install

```powershell
cd E:\EC\LiveBot
python -m pip install -r requirements.txt
python -m pip install -r requirements-dev.txt   # adds pytest for verification
```

`requirements.txt` lists only packages that are actually imported by `live_engine/` or
`strategies/approved/`, each constrained to the range this build was tested against.

### Verify the installation

```powershell
python -m pytest tests -q
python -m compileall -q live_engine strategies tests
```

### Credentials

API credentials come **only** from environment variables. A configuration file that
contains `binance_api_key`, `binance_api_secret`, or any other credential field is
rejected at load with `CREDENTIALS IN CONFIG`.

```powershell
$env:BINANCE_API_KEY    = "<key>"       # TESTNET/LIVE only
$env:BINANCE_API_SECRET = "<secret>"    # TESTNET/LIVE only
$env:ESCANOR_LIVE_TRADING_ENABLED = "false"
```

Copy `.env.example` for the full list. SHADOW and PAPER need no credentials at all.

---

## 2. What each mode means, and what it does not

| Mode | Market data | Orders | Account | Database | What it proves |
|---|---|---|---|---|---|
| **SHADOW** (default) | Real Binance public stream | None | None | `data/<sym>_shadow.db` | Strategy decisions match the benchmark |
| **PAPER** | Real Binance public stream | Simulated | Simulated wallet | `data/<sym>_paper.db` | Decisions plus a simulated portfolio |
| **TESTNET** | Real Binance public stream | Real, on Binance testnet | Testnet account | `data/<sym>_testnet.db` | Authenticated order handling works |
| **LIVE** | Real Binance public stream | Real, funded | Funded account | `data/<sym>_live.db` | Nothing until it has been run |

**Limitations you must not forget:**

- **SHADOW** keeps the benchmark reference price for exits. It is a decision-parity
  instrument, not an execution model. It says nothing about fills.
- **PAPER** fills are simulated under an explicit model (`PaperExecutionModel`). The
  approved manifests declare `slippage: zero_or_measured`, so the default model is
  zero-impact and is labelled `benchmark_replay` everywhere it is persisted. That is a
  benchmark replay, **not** a prediction of real Binance fills. Exits are submitted at the
  market price available when the decision became known, never at the historical stop
  level, so paper PnL will differ from the benchmark by design.
- **TESTNET** has its own liquidity and matching behaviour. It proves authentication,
  filters, order acceptance, reconciliation and latency plumbing. It does **not**
  represent production fills.
- **LIVE** is blocked until every activation gate has real evidence (section 8).

BTC and HYPE permit all four modes (`allowed_modes: [SHADOW, PAPER, TESTNET, LIVE]`). LIT and
ZEC are restricted to SHADOW and PAPER by their manifests (`allowed_modes: [SHADOW, PAPER]`);
TESTNET and LIVE are rejected at config validation.

---

## 3. Health states

The engine exposes one health state; only `READY` may open new risk.

| State | Meaning | New entries |
|---|---|---|
| `WARMING_UP` | Warmup candles not yet primed | Blocked |
| `READY` | Everything fresh and reconciled | Allowed |
| `DEGRADED` | A required component's heartbeat is stale | Blocked |
| `DATA_DESYNCED` | Unrecovered aggTrade gap or history hole | Blocked; strategy evaluation stops |
| `EXCHANGE_DISCONNECTED` | Private execution stream stale in LIVE/TESTNET | Blocked |
| `RECONCILIATION_REQUIRED` | Reconciliation failed, or an UNKNOWN order is unresolved | Blocked |
| `CONFIGURATION_MISMATCH` | Manifest/hash/config disagreement | Blocked |
| `KILLED` | Kill switch engaged | Blocked |

Protective exits and observation continue while entries are blocked, wherever exchange
state allows. The state is printed in the startup banner and included in every
`SYSTEM_SHUTDOWN` and supervisor record.

---

## 4. Running

### Validate a configuration without launching (dry run)

```powershell
python -m live_engine.main --config config/btc-paper.json --dry-run
```

This loads the strategy (verifying its SHA-256), primes warmup, backfills the gap between
the Parquet tail and now, runs full startup reconciliation, and prints the banner.

### Single instance

```powershell
python -m live_engine.main --config config/btc-shadow.json
python -m live_engine.main --config config/btc-paper.json
python -m live_engine.main --config config/lit-shadow.json
python -m live_engine.main --config config/lit-paper.json
python -m live_engine.main --config config/zec-shadow.json
python -m live_engine.main --config config/zec-paper.json
python -m live_engine.main --config config/hype-shadow.json
python -m live_engine.main --config config/hype-paper.json
```

### All three PAPER engines plus the unified dashboard

```powershell
.\scripts\run-btc-lit-zec-paper-dashboard.ps1 -DryRunOnly   # validate only
.\scripts\run-btc-lit-zec-paper-dashboard.ps1               # launch
```

The launcher validates every config, verifies database isolation, launches the processes,
and writes `logs/escanor-processes.json` — the manifest the supervisor and the graceful
stop path both read.

### LIT/ZEC SHADOW + PAPER

```powershell
.\scripts\run-lit-zec-paper-shadow.ps1 -DryRunOnly
.\scripts\run-lit-zec-paper-shadow.ps1
```

### HYPE Rank 12 (run SHADOW first, then PAPER)

```powershell
python scripts/verify_hype_benchmark.py
.\scripts\run-hype.ps1 -Mode SHADOW -DryRunOnly
.\scripts\run-hype.ps1 -Mode SHADOW -Port 8083
# After the forward SHADOW observation period and a clean shutdown:
.\scripts\run-hype.ps1 -Mode PAPER -Port 8083
```

HYPE is approved for all four modes as of Phase 48. Its dashboard is isolated at the
selected port and reads only `data/hype_<mode>.db`. TESTNET and LIVE need credentials in the
environment, and LIVE needs the operator acknowledgement and an interactive confirmation —
see section 8.3 for the full funded procedure.

### Dashboard on its own

```powershell
python -m live_engine.main --dashboard `
  --dashboard-config config/btc-paper.json `
  --dashboard-config config/lit-paper.json `
  --dashboard-config config/zec-paper.json `
  --host 127.0.0.1 --port 8080
```

Read-only (`mode=ro`); `POST` returns `405`.

---

## 5. Supervision and shutdown

### Supervise running instances

```powershell
.\scripts\escanor-control.ps1 -Supervise
```

Watches for a crashed child, a heartbeat that stopped advancing in the instance database,
and an engaged kill switch. Findings go to `logs/incidents.jsonl`, the same structured
sink the engine writes to. The launcher's two-second post-launch probe only proves the
processes started; this is the ongoing check.

The engine runs its own in-process supervisor every 15 seconds, covering crash, stale
heartbeat, reconnect storm, unresolved gap, expired private stream, reconciliation
failure, UNKNOWN order, kill-switch activation and daily-loss activation. Alerts are
deduplicated and paired with recovery notifications.

### Alert routing

Alerts always go to `logs/incidents.jsonl` and the `incidents` table. To also forward
them, set a webhook **in the environment only**:

```powershell
$env:ESCANOR_ALERT_WEBHOOK_URL = "https://<your-endpoint>"
```

Alert bodies are scrubbed: API keys, secrets, signatures and listen keys are replaced with
`[REDACTED]` before anything is written or sent.

### Graceful shutdown

```powershell
.\scripts\escanor-control.ps1 -Stop
```

This requests a clean stop, waits up to 30 seconds for each engine to cancel its
background tasks, close its streams and flush durable state, and only then forces whatever
is still running — recording a `FORCED_TERMINATION` incident when it has to.

> `Stop-Process -Force` on its own is **not** a clean shutdown. It can leave a
> half-applied fill, an unflushed heartbeat and orphaned background tasks for
> reconciliation to untangle on the next start. Use it only when the graceful path has
> already failed, and expect a reconciliation pass on restart.

Pressing `Ctrl+C` in a foreground engine triggers the same graceful path.

---

## 6. Kill switch

Shared across every instance (`.kill_switch` in the repository root). Fail-closed: an
engaged, empty or corrupted file blocks all new entries.

```powershell
python -m live_engine.main --kill "Operator emergency halt"
python -m live_engine.main --disengage-kill
python -m live_engine.main --config config/btc-paper.json --status   # shows the state
```

Engaging the kill switch stops **new risk**. It does not liquidate an open position; that
is a separate, deliberate operator action.

---

## 7. Parity verification

```powershell
# Fast: replay the canonical candle dataset
python -m live_engine.main --config config/btc-paper.json  --parity-check
python -m live_engine.main --config config/lit-shadow.json --parity-check
python -m live_engine.main --config config/zec-shadow.json --parity-check

# Thorough: rebuild every candle from raw aggTrades, then compare
python -m live_engine.main --config config/btc-paper.json --parity-check --raw-aggtrades
```

The report distinguishes two numbers that used to be conflated:

- **Trade % Sum** — the arithmetic sum of per-trade percentages. Not a portfolio return.
- **Compounded Portfolio Return** — an actual equity curve built from the initial balance,
  the manifest's `stake_rule`, leverage, fills and fees.

`--raw-aggtrades` additionally validates that the declared raw monthly files cover the
whole evaluation window *before* replay starts, and compares every evaluation-window
candle timestamp and OHLCV field against the canonical dataset at tick resolution.

Every saved report is bound to SHA-256 hashes of the manifest, strategy, helper, benchmark
CSV, canonical candles and raw files, plus the tool version and generation time. Change any
of them and the report is reported stale rather than trusted.

Reports are written to `benchmarks/reports/<BENCHMARK_ID>_parity_report.json`. Tests and
failing runs write to a temporary directory and can never overwrite an operational report.

### Known finding: raw candle parity for LIT

`--raw-aggtrades` for `LIT_SUPERTREND_15M` currently reports **FAIL** with
**99.39% candle OHLCV parity** (1632 compared, 0 missing, 0 extra, 0 duplicated,
11 mismatched). Strategy decision parity is still **100%** — all 20 benchmark trades match
on direction, timestamps, prices, exit reasons and profit.

The 11 mismatched bars are volume differences (plus the open on three of them) that occur
in **adjacent pairs whose deltas cancel exactly**: the same quantity sits on the other side
of a 15m bucket edge. An independent sum of raw aggTrade quantities over the exact
half-open UTC window reproduces the engine's reconstruction to the unit, so the engine's
bucketing follows the documented `[T, T+900000)` rule correctly. The canonical
`LITUSDT_15m.parquet` assigns those boundary trades to the neighbouring bucket.

This is a **dataset provenance difference, not an engine defect**, and it is deliberately
left failing rather than absorbed by a wider tolerance. Resolve it by regenerating the
canonical candle dataset from the same aggTrade archive with the same boundary rule, then
re-run raw parity. Until then, raw parity is not valid evidence for LIT, and the raw
coverage gate for LIT must not be treated as satisfied.

### Execution quality (benchmark versus observed)

```powershell
python -m live_engine.main --config config/btc-paper.json --execution-report
```

Reports entry/exit slippage, implementation shortfall, acknowledgement and first-fill
latency, fees, and which signals the benchmark had that the runtime did not — separating
strategy/data divergence from execution divergence. Anything the engine did not actually
capture is printed as `not captured` rather than estimated.

---

## 8. Funded LIVE activation

LIVE refuses to start unless every activation gate passes. Each gate resolves to an
evidence record with a source, a value, an observation time and an expiry policy. Missing,
stale or unverifiable evidence fails closed, and a source or test file merely existing
satisfies nothing.

Beyond the original 24 gates, the following runtime evidence is required:

- raw dataset coverage complete
- market data synchronized
- private execution stream fresh
- account reconciled with no UNKNOWN orders
- position / margin / leverage mode correct
- exchange filters current
- protective state consistent
- mark price and account data fresh
- execution telemetry available
- testnet soak and chaos evidence recorded

`ESCANOR_LIVE_TRADING_ENABLED=true` remains an **additional** operator acknowledgement on
top of that evidence, never a substitute for it.

Record durable soak/chaos evidence with:

```python
from live_engine.persistence.event_store import EventStore
from live_engine.risk.safety_gates import record_evidence

store = EventStore("data/btc_live.db")
record_evidence(store, "testnet_soak_evidence", True, {"hours": 72, "orders": 40})
record_evidence(store, "chaos_evidence", True, {"scenarios": 16})
```

Testnet soak evidence expires after 7 days; chaos evidence after 30.

### 8.1 How the gates are evaluated

Authorisation is checked in two stages, and neither stage can be skipped:

1. **Before anything connects.** The kill switch, `ESCANOR_LIVE_TRADING_ENABLED` and the
   presence of `BINANCE_API_KEY` / `BINANCE_API_SECRET` (Gate 24). An unauthorised LIVE start
   never opens an authenticated session.
2. **After the engine initialises and its streams report.** All 34 gates, evaluated against
   the live engine: exchange filters, account reconciliation, position mode, protective
   state and stream freshness are runtime facts that no pre-connection check can observe.
   Trades arriving during the evaluation are held in event order and released only once the
   gates pass; nothing is dropped and no order is submitted before then.

`--dry-run` on a LIVE configuration performs both stages and then stops without processing a
single trade. That is the funded pre-flight: exit code 0 means every gate passed.

### 8.2 Evidence lives in the instance database

Each gate reads the database the engine will actually use, which is what keeps instances
isolated — and means a cold `data/hype_live.db` has no evidence in it yet. Three gate groups
have to be seasoned into the target database before funded activation:

| Gate | Needs | Produced by |
|---|---|---|
| 14, 21 (SHADOW/PAPER tested) | `SYSTEM_INITIALIZED` for those modes | a **dry run** of the LIVE config with `--mode SHADOW` and `--mode PAPER` |
| 33 (execution telemetry) | at least one telemetry record | the TESTNET soak (`data/hype_testnet.db`) or the PAPER instance (`data/hype_paper.db`) — read from there, see below |
| 34 (soak + chaos) | durable attestations | `scripts/record_hype_live_evidence.py`, after the exercises have actually been run |

> **Never run a full PAPER or SHADOW *session* against a funded database.** A PAPER session
> writes simulated orders and fills into the instance ledger, and the next LIVE start replays
> that ledger through `fill_applier.replay_from_store()` into a phantom position, which
> startup reconciliation then sees as a mismatch against exchange truth. Use `--dry-run`: it
> records the initialisation for gates 14 and 21 and writes no order, fill or telemetry row.
> Verified on `data/hype_live.db`: after a SHADOW and a PAPER dry run, `orders`, `fills` and
> `execution_telemetry` are all still empty and both modes are recorded.

**Gate 33 reads the instance that captured the telemetry.** `Execution telemetry available`
used to read `execution_telemetry` only from the funded database. Telemetry is written when an
order is submitted and the gate is evaluated before the first order, so a first funded
activation could never satisfy it — and the unsafe workaround was the PAPER session ruled out
above. The gate now checks the funded database first and then, for LIVE only, this symbol's
own `_testnet` and `_paper` instances, which is where the soak's telemetry actually lives. It
is still direct observation of real telemetry from a real order, and the failure message names
every database it searched:

```
Gate 33 Execution telemetry available: No execution telemetry has ever been recorded
  (searched hype_live.db, hype_paper.db, hype_shadow.db)
```

Only this symbol's instances are consulted — `lit_paper.db` telemetry cannot satisfy HYPE —
and a TESTNET or PAPER instance must still produce its own.

### 8.3 HYPE TESTNET and funded LIVE procedure

```powershell
# 0. Re-verify the frozen benchmark and the hash-bound parity report.
python scripts/verify_hype_benchmark.py

# 1. TESTNET soak. Real orders, test capital, its own database.
#    Testnet keys come from testnet.binancefuture.com and are a different account from your
#    funded one. Use the dedicated variables so a funded key is never in play here.
$env:BINANCE_TESTNET_API_KEY    = "<testnet key>"
$env:BINANCE_TESTNET_API_SECRET = "<testnet secret>"
.\scripts\run-hype.ps1 -Mode TESTNET -DryRunOnly          # validate only
.\scripts\run-hype.ps1 -Mode TESTNET -Port 8084           # soak for >= 72h
.\scripts\escanor-control.ps1 -Supervise -Manifest logs\hype-processes.json

# 2. Stop the soak cleanly and review it.
.\scripts\escanor-control.ps1 -Stop -Manifest logs\hype-processes.json
python -m live_engine.main --config config/hype-testnet.json --execution-report

# 3. Season the LIVE database with mode evidence (section 8.2). Dry runs only: these record
#    the initialisation for gates 14 and 21 and write no orders, fills or telemetry.
python -m live_engine.main --config config/hype-live.json --mode SHADOW --dry-run
python -m live_engine.main --config config/hype-live.json --mode PAPER --dry-run
python -c "import sqlite3;c=sqlite3.connect('file:data/hype_live.db?mode=ro',uri=True);print({t:c.execute('SELECT COUNT(*) FROM '+t).fetchone()[0] for t in ('orders','fills','execution_telemetry')})"

# 4. Record the soak and chaos attestations into the LIVE database.
python scripts/record_hype_live_evidence.py --database data/hype_live.db `
    --soak-hours 72 --soak-orders 40 --chaos-scenarios 16
python scripts/record_hype_live_evidence.py --verify-only    # expiry check, records nothing

# 5. Funded activation. Both variables are required; the launcher refuses without them.
$env:BINANCE_API_KEY    = "<funded key>"
$env:BINANCE_API_SECRET = "<funded secret>"
$env:ESCANOR_LIVE_TRADING_ENABLED = "true"
.\scripts\run-hype.ps1 -Mode LIVE -DryRunOnly              # full 34-gate pre-flight
.\scripts\run-hype.ps1 -Mode LIVE -Port 8085               # prompts [y/N] before funding
```

`-Force` skips only the interactive confirmation, for a supervised restart that has already
been reviewed. It skips no gate. LIVE runs with `canary_mode: true` and a `$100` ceiling:
roughly `$25` notional per slot at 3x across 12 slots, which clears the 5 USDT minimum
notional. Every capped entry is recorded as `CANARY_CAP_APPLIED`. Raise the ceiling only by
editing `config/hype-live.json`, and re-run the pre-flight afterwards.

### 8.4 Graceful stop

```powershell
.\scripts\escanor-control.ps1 -Stop -Manifest logs\hype-processes.json
```

The launcher writes `logs/hype-processes.json` with the engine and dashboard PIDs, the mode
and the database, which is what the supervisor watches and the stop path reads. `-Stop` asks
each process to flush and exit, waits up to `-StopTimeoutSeconds` (default 30), and only then
forces anything still running — recording a `FORCED_TERMINATION` incident when it has to, so
the next start knows to expect reconciliation work.

To halt trading immediately without stopping the process:

```powershell
python -m live_engine.main --config config/hype-live.json --kill "Operator halt"
```

---

## 9. Monitoring

```powershell
# Live feed check (5 real trades with latency)
python -m live_engine.main --config config/lit-shadow.json --check-feed

# Engine status and database audit
python -m live_engine.main --config config/lit-shadow.json --status

# Logs
Get-Content logs/lit-shadow.stdout.log -Tail 20
Get-Content logs/lit-shadow.stdout.log -Wait
Get-Content logs/incidents.jsonl -Tail 20
```

Database and log locations:

| Instance | Database | Stdout log |
|---|---|---|
| BTC PAPER | `data/btc_paper.db` | `logs/btc-paper.stdout.log` |
| LIT SHADOW | `data/lit_shadow.db` | `logs/lit-shadow.stdout.log` |
| LIT PAPER | `data/lit_paper.db` | `logs/lit-paper.stdout.log` |
| ZEC SHADOW | `data/zec_shadow.db` | `logs/zec-shadow.stdout.log` |
| ZEC PAPER | `data/zec_paper.db` | `logs/zec-paper.stdout.log` |
| HYPE SHADOW | `data/hype_shadow.db` | `logs/hype-shadow.stdout.log` |
| HYPE PAPER | `data/hype_paper.db` | `logs/hype-paper.stdout.log` |
| HYPE TESTNET | `data/hype_testnet.db` | `logs/hype-testnet.stdout.log` |
| HYPE LIVE | `data/hype_live.db` | `logs/hype-live.stdout.log` |
| HYPE dashboard | read-only HYPE instance | `logs/hype-<mode>-dashboard.stdout.log` |
| Dashboard | read-only aggregate | `logs/paper-dashboard.stdout.log` |

Each instance owns its database. `ESCANOR_EVENT_STORE` is rejected at startup.

---

## 10. Recovery

### After any restart

Startup performs, in order: durable fill-ledger replay, balance and account reconciliation,
recovery of exchange fills the engine never saw, open/UNKNOWN order reconciliation,
position reconciliation, and protective-state reconciliation. Every step is idempotent, so
running it repeatedly changes nothing.

The market-data high-water mark (`last_accepted_agg_trade_id`) is restored, so the first
live event after a restart is compared against durable state and any real gap is detected
and recovered rather than silently skipped.

### UNKNOWN order

An ambiguous submission marks the order `UNKNOWN` and blocks new entries for that symbol.
The engine queries by client order ID with bounded backoff. An order is declared absent
only after three consecutive `NOT_FOUND` results spanning at least the indexing window
**and** no account trade referencing that client order ID. Nothing is ever resubmitted on
weaker evidence.

If it stays unresolved, the engine stays in `RECONCILIATION_REQUIRED`, keeps observing, and
opens no new risk. Inspect it with:

```powershell
python -m live_engine.main --config config/btc-paper.json --status
Get-Content logs/incidents.jsonl -Tail 20
```

### Market-data gap

A gap sets `DATA_DESYNCED` and blocks strategy evaluation. Recovery refetches the exact
missing aggregate-trade ID range off the event loop, validates every page (rejecting empty
responses, truncated pages, duplicates, out-of-range IDs, wrong symbols and malformed
rows), and only clears the gap when **every** expected ID is present. A partial recovery
records the exact remaining interval and stays fail-closed.

### Daily loss limit

Drawdown is measured against a persisted start-of-day equity snapshot at the UTC day
boundary. It survives restart: reaching the limit engages the kill switch and blocks new
entries for the rest of the UTC day. A new snapshot is created at the next rollover.

### Troubleshooting

| Symptom | Cause and action |
|---|---|
| `PARITY VALIDATION FAILED` | Run `--parity-check` for that config first. |
| `WARMUP VALIDATION FAILED: Dataset not found` | Check `<SYMBOL>_USDM_DATA/candles/`. |
| `RAW COVERAGE ERROR` | The manifest's `dataset_aggtrade_paths` does not cover the evaluation window. |
| `CREDENTIALS IN CONFIG` | Remove credential fields; use environment variables. |
| `CONFIGURATION ERROR: mode=...` | The mode is not one of SHADOW/PAPER/TESTNET/LIVE. |
| `MANIFEST INCOMPLETE` | The manifest is missing explicit execution semantics or fees/leverage. |
| `UNSUPPORTED POSITION MODE` | Futures position mode could not be determined or is unsupported. |
| `DATABASE ISOLATION VIOLATION` | The database path escapes `data/`, or two instances share one. |
| Kill switch engaged | `python -m live_engine.main --disengage-kill` after investigating. |

---

## 11. Maintenance

```powershell
# Back up databases (stop the engines first)
.\scripts\escanor-control.ps1 -Stop
Copy-Item data/*.db "backup-$(Get-Date -Format 'yyyyMMdd-HHmmss')\"

# Prune logs older than 7 days
Get-ChildItem logs/*.log | Where-Object { $_.LastWriteTime -lt (Get-Date).AddDays(-7) } | Remove-Item

# Confirm nothing is still running
Get-Process python -ErrorAction SilentlyContinue | Measure-Object | Select-Object -ExpandProperty Count
```

The persistence schema is versioned and migrated forward in place; new columns and tables
are added without dropping or rewriting existing data, so a database from an earlier build
opens without losing history.
