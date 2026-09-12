# Escanor Live Trading Engine: Project Specification & Architecture

## 1. Project Purpose & Objective

Escanor is an institutional-grade algorithmic trading engine engineered for Binance USD-M Futures (`futures/um`).
The primary objective of the engine is to execute mathematically verified quantitative strategies with **100% decision parity** against historical benchmarks:

> **Fundamental Invariant**: Given the identical sequence of market events, the live trading engine and historical benchmark runner must produce the exact same strategy decisions (signal generation, entry candles, exit triggers, and position sizing).

Real-world exchange execution nuances (slippage, latency, order fills, exchange fees) are measured and accounted for separately within the execution and reconciliation layers, preserving strict separation of concerns from strategy logic.

The engine architecture supports multi-symbol operations across four distinct execution modes:
- **BTCUSDT**: Full production lifecycle (SHADOW, PAPER, TESTNET, and funded LIVE trading with 34 evidence-backed safety gates and canary allocation).
- **HYPEUSDT**: Approved production benchmark with TESTNET and LIVE capability (SHADOW, PAPER, TESTNET, and funded LIVE; 12 logical slots at 3x aggregate leverage behind the same 34 gates and a canary ceiling).
- **LITUSDT & ZECUSDT**: Parallel candidate portfolio operations (restricted strictly to SHADOW and PAPER modes).
- **Multi-Symbol Paper Observability**: Unified real-time dashboard aggregating isolated simulated accounts concurrently.

---

## 2. Approved Benchmark Strategies & Assets

### 2.1 BTC_ST_09_5M (Primary Production Benchmark)

- **Benchmark ID**: `BTC_ST_09_5M`
- **Exchange / Market**: Binance USD-M Futures (`futures/um`)
- **Trading Pair**: `BTCUSDT`
- **Timeframe**: `5m` (derived from canonical 1m candles reconstructed from `aggTrade`)
- **Strategy Class**: `BTCSupertrendPullback5M` (ST_09)
  - Parameters: Supertrend ATR Length = 10, Multiplier = 3.0, Pullback Threshold = 0.5 ATR, Warmup = 250 candles
  - Signal Logic: first bullish confirmation bar (`close > open`) within 0.5 ATR above the Supertrend line while direction is bullish
  - Exit Logic: 1.5 ATR stop loss, then 3.0 ATR take profit, then bearish Supertrend flip; stop has conservative same-bar priority
  - Source File: [`strategies/approved/BTCSupertrendPullback5M.py`](file:///E:/EC/LiveBot/strategies/approved/BTCSupertrendPullback5M.py)
  - Source SHA-256: `ec15e095fc3256a224bb965eb733455cff90ec2cd279f9d2395e6f8ba59c0e0d`
- **Historical Benchmark Validation (2026-08-15 to 2026-08-31)**:
  - Total Trades: 12
  - Win Rate: 25.0% (3 wins, 9 losses)
  - Profit Factor: 0.17
  - Net Return after 0.05% taker fee per side: -1.92%
  - Source of Truth Log: [`benchmarks/results/BTC_ST_09_5M_trades.csv`](file:///E:/EC/LiveBot/benchmarks/results/BTC_ST_09_5M_trades.csv)
- **Allowed Modes**: `SHADOW`, `PAPER`, `TESTNET`, `LIVE`

### 2.2 LIT_SUPERTREND_15M (Candidate Strategy)

- **Benchmark ID**: `LIT_SUPERTREND_15M`
- **Exchange / Market**: Binance USD-M Futures (`futures/um`)
- **Trading Pair**: `LITUSDT`
- **Timeframe**: `15m`
- **Strategy Class**: `LITSupertrendPullback15M`
  - Parameters: Supertrend(28, 2.0), ATR(7), Pullback factor = 0.5 * ATR(7), Lookback = 3 bars
  - Exit Model: Bracket order emulation with Stop-Loss = 1.5x ATR(7), Take-Profit = 3.0x ATR(7), Max Hold = 128 bars; Flip Exit = False
  - Source File: [`strategies/approved/LITSupertrendPullback15M.py`](file:///E:/EC/LiveBot/strategies/approved/LITSupertrendPullback15M.py)
  - Source SHA-256: `0460bef40daaf8698a7aa1643592f5377ccd6e027be6ca853fd62dba8256f5e8`
- **Historical Validation**:
  - Evaluation Window (2026-08-15 to 2026-08-31): 20 trades, +7.74% return
  - Extended Horizon (2026-02-01 to 2026-09-05): 365 trades, +255.86% return
  - Source of Truth Log: [`benchmarks/results/LIT_15M_trades.csv`](file:///E:/EC/LiveBot/benchmarks/results/LIT_15M_trades.csv)
- **Allowed Modes**: `SHADOW`, `PAPER` ONLY (TESTNET and LIVE are rejected at config validation)

### 2.3 ZEC_MOMENTUM_M03_15M (Candidate Strategy)

- **Benchmark ID**: `ZEC_MOMENTUM_M03_15M`
- **Exchange / Market**: Binance USD-M Futures (`futures/um`)
- **Trading Pair**: `ZECUSDT`
- **Timeframe**: `15m`
- **Strategy Class**: `ZECMomentumEMA15M`
  - Model: Momentum M03 - EMA Cross (10, 100) institutional trend-following strategy
  - Indicators: Fast EMA (span=10), Slow EMA (span=100)
  - Long Entry: `(ema_fast > ema_slow) & (prev_fast <= prev_slow) & (close > ema_fast)`
  - Exit: Opposing cross (`ema_fast < ema_slow & prev_fast >= prev_slow`)
  - Execution Model: Full reversal exit / next bar open fill, 100% compounding, $10,000 initial balance, 1.0x leverage, 0.05% taker fee per side (0.10% round-trip)
  - Source File: [`strategies/approved/ZECMomentumEMA15M.py`](file:///E:/EC/LiveBot/strategies/approved/ZECMomentumEMA15M.py)
  - Source SHA-256: `ab11519515f84efc45ecdab92d1774ea187ccc00df5c4218fcefac140a497e0e`
- **Historical Validation**:
  - Evaluation Window (2025-08-01 to 2026-08-31, 396 calendar days): 321 trades, +415.95% return (100.0% decision, timestamp, direction, and return parity)
  - Source of Truth Log: [`benchmarks/results/ZEC_15M_trades.csv`](file:///E:/EC/LiveBot/benchmarks/results/ZEC_15M_trades.csv)
  - Parity Report: [`benchmarks/reports/ZEC_MOMENTUM_M03_15M_parity_report.json`](file:///E:/EC/LiveBot/benchmarks/reports/ZEC_MOMENTUM_M03_15M_parity_report.json)
- **Allowed Modes**: `SHADOW`, `PAPER` ONLY (TESTNET and LIVE are rejected at config validation)

### 2.4 HYPE_LUXALGO_RANK12_5M (Candidate Strategy)

- **Benchmark ID**: `HYPE_LUXALGO_RANK12_5M`
- **Exchange / Market**: Binance USD-M Futures (`futures/um`)
- **Trading Pair / Timeframe**: `HYPEUSDT`, `5m`
- **Strategy Class**: `HYPELuxAlgoRank12_5M`
  - Entry: frozen three-rule LuxAlgo Rank-12 OR model, evaluated only on closed candles and filled at next open
  - Exit: first aggTrade touch of 4.5 × ATR(14) target or 6 × ATR(14) stop; 384-bar time stop
  - Portfolio: 12 independent logical slots, long-only, 3× maximum aggregate leverage, 0.05% taker fee per side
  - Data: `E:/EC/Binance-AggTrades/HYPEUSDT_USDM_DATA`
  - Source File: `strategies/approved/HYPELuxAlgoRank12_5M.py`
- **Historical Validation (2025-09-01 to 2026-08-31)**:
  - Exact original aggTrade tick oracle: 1,369 trades, 77.72% win rate, 2.525 profit factor
  - 3× compounded result: $10,000.00 to $125,614.79 (+1,156.15%); in-sample evidence, not a forward guarantee
  - Signal comparison: 132,066 candles compared, zero mismatches
  - Source of Truth Log: `benchmarks/results/HYPE_LUXALGO_RANK12_5M_trades.csv`
  - Hash-Bound Report: `benchmarks/reports/HYPE_LUXALGO_RANK12_5M_parity_report.json`
- **Isolation**: `data/hype_shadow.db`, `data/hype_paper.db`, `data/hype_testnet.db` and `data/hype_live.db`; dedicated read-only HYPE dashboard per mode
- **Allowed Modes**: `SHADOW`, `PAPER`, `TESTNET`, `LIVE` — promoted to **Approved Production Benchmark** in Phase 48
- **Production Configurations**: [`config/hype-testnet.json`](file:///E:/EC/LiveBot/config/hype-testnet.json) (`binance_testnet: true`) and [`config/hype-live.json`](file:///E:/EC/LiveBot/config/hype-live.json) (`canary_mode: true`, `$100.00` allocation ceiling). Neither carries credentials; `BINANCE_API_KEY` / `BINANCE_API_SECRET` come only from the environment
- **Funded Activation**: all 34 evidence-backed gates must pass, plus `ESCANOR_LIVE_TRADING_ENABLED=true` as an additional operator acknowledgement. Soak and chaos evidence is recorded with [`scripts/record_hype_live_evidence.py`](file:///E:/EC/LiveBot/scripts/record_hype_live_evidence.py) (7-day and 30-day expiry)
- **Canary Exposure**: the `$100` ceiling yields roughly `$25` notional per slot at 3x across 12 slots, clearing the 5 USDT minimum notional on `HYPEUSDT`

---

## 3. End-to-End System Architecture & Data-Flow

```text
                                       BINANCE EXCHANGE (USD-M Futures)
                     ┌─────────────────────────────────┼─────────────────────────────────┐
                     │                                 │                                 │
                     ▼                                 ▼                                 ▼
         Public WebSocket Stream            Secondary Kline Stream            Private User Data Stream
        (wss @aggTrade live ticks)         (wss @kline_1m validator)         (listenKey: orders, fills)
                     │                                 │                                 │
                     ▼                                 │                                 ▼
           AggTradeGapDetector                         │                        BinanceUserDataStream
        (ID sequence & REST backfill)                  │                       (30m keep-alive loop)
                     │                                 │                                 │
                     ▼                                 │                                 │
           MinuteCandleBuilder                         │                                 │
         (1m UTC closed candles)                       │                                 │
                     │                                 ▼                                 │
                     ├───[Register 1m]────>  BinanceKlineValidator                       │
                     │                      (OHLCV diff tolerance check)                 │
                     ▼                                                                   │
           TimeframeResampler                                                            │
         (1m -> strategy closed bars)                                                    │
                     │                                                                   │
                     ▼                                                                   │
          WarmupManager / State                                                          │
          (250 historical bars)                                                          │
                     │                                                                   │
                     ▼                                                                   │
            StrategyLoader                                                               │
        (SHA-256 code verification)                                                      │
                     │                                                                   │
                     ▼                                                                   │
            StrategyAdapter                                                              │
         (Executes approved code)                                                        │
                     │                                                                   │
                     ▼                                                                   │
               SignalEngine                                                              │
          (Immutable SignalEvent)                                                        │
                     │                                                                   │
                     ▼                                                                   │
             RiskGuardEngine                                                             │
         (KillSwitch, Gap, Stale,                                                        │
          Notional, Deviation limits)                                                    │
                     │                                                                   │
                     ▼                                                                   │
               OrderManager <────────────────────────────────────────────────────────────┤
         (Monotonic state machine,                                                       │
          Idempotent client_order_id)                                                    ▼
                     │                                                          AccountReconciler
                     ▼                                                     (Startup & periodic audits:
              ExecutionBroker                                               position, balance, orders)
       ┌─────────────┼─────────────┬─────────────┐                                       │
       ▼             ▼             ▼             ▼                                       ▼
  ShadowBroker  PaperBroker  TestnetBroker  LiveBroker ─────────────>               KillSwitch
  (Simulation)  (Leverage,    (Testnet API)  (Funded REST,                          (.kill_switch)
                 Fees, PnL)                  HMAC-SHA256,
                                             Query-Retry)
                     │
                     ▼
          Persistent EventStore
     (SQLite WAL: raw_aggtrades,
      candles, signals, orders,
      incidents, audit_events)
                     │
                     ▼
       Multi-Symbol Web Dashboard
    (TradingView Charts, Supertrend,
     Fills, Health, Read-Only mode)
```

---

## 4. Multi-Process Instance Architecture & Isolation

The system operates under an **isolated multi-process architecture** where each trading instance runs as an independent OS process with its own configuration, database, and market stream:

```text
[Operator / Launcher Script]
     │
     ├── PID 101: BTC-PAPER  ──> config/btc-paper.json  ──> data/btc_paper.db  ──> logs/btc-paper.stdout.log
     ├── PID 102: LIT-PAPER  ──> config/lit-paper.json  ──> data/lit_paper.db  ──> logs/lit-paper.stdout.log
     ├── PID 103: ZEC-PAPER  ──> config/zec-paper.json  ──> data/zec_paper.db  ──> logs/zec-paper.stdout.log
     │
     └── PID 104: DASHBOARD  ──> Read-Only Multi-Aggregator (http://127.0.0.1:8080)
```

### 4.1 Database Isolation & Path Security Rules

1. **Strict Directory Containment**: Every database path must resolve strictly underneath the `data/` directory. Path traversal attempts (`../`), pointing to `data/` itself, or referencing parent directories are blocked at configuration load (`validate_database_path`).
2. **Dedicated Instance Databases**: Each running instance must use its own distinct database file (`data/btc_paper.db`, `data/lit_paper.db`, `data/zec_paper.db`, etc.).
3. **Environment Override Ban**: Setting the `ESCANOR_EVENT_STORE` environment variable is explicitly rejected with a `DATABASE ISOLATION VIOLATION` to prevent shared state collisions.
4. **Collision Detection**: Multi-config aggregators execute `validate_unique_databases()` before launch, guaranteeing that no two instances share a database path.

### 4.2 Unified Emergency Kill Switch

- **Path**: `.kill_switch` in the repository root.
- **Shared Protection**: All running instances poll this persistent file.
- **Fail-Closed**: If the file contains an engaged payload, is empty, or is corrupted, all engines immediately halt entry orders.
- **CLI Management**:
  ```powershell
  python -m live_engine.main --kill "Operator emergency halt"
  python -m live_engine.main --disengage-kill
  ```

---

## 5. Core Pipeline Subsystems & Specifications

### 5.1 Market Data Ingestion Pipeline

1. **`BinanceAggTradeStream`**:
   - Asynchronous WebSocket client connecting to `wss://fstream.binance.com/market/ws`.
   - Subscribes to `<symbol>@aggTrade`.
   - Implements exponential backoff reconnection (1s to 30s) and keepalive ping intervals.
2. **`AggTradeGapDetector`**:
   - Inspects aggregate trade ID sequences ($A_t - A_{t-1} - 1$).
   - Flags sequence gaps and immediately sets `is_desynced = True`, blocking entry orders.
   - Automatically executes REST backfill recovery via `/fapi/v1/aggTrades` (batches of up to 1,000 trades) and resumes once all missing trades are reconstructed.
3. **`MinuteCandleBuilder`**:
   - Aggregates trades into deterministic 1-minute OHLCV candles on UTC minute boundaries $[T, T+60,000)\text{ ms}$.
   - Maintains a bounded in-memory deduplication set (up to 50,000 IDs).
4. **`TimeframeResampler`**:
   - Re-aggregates closed 1m candles into higher-timeframe strategy bars (15m, 1h, etc.) aligned to UTC epoch boundaries.
5. **`BinanceKlineValidator`**:
   - Secondary listener subscribing to `<symbol>@kline_1m`.
   - Compares locally reconstructed candles against closed Binance exchange klines.
   - Enforces tolerances: maximum price difference $\le \$0.50$, volume difference $\le 1.0\%$, close boundary $\le 1000\text{ ms}$. Records incidents upon mismatch.
6. **`WarmupManager`**:
   - Primes indicators using 250 pre-trade historical candles loaded from Parquet.
   - Validates data freshness: logs warning if last candle is $>24\text{ hours}$ old; halts startup in LIVE mode if $>72\text{ hours}$ stale.

### 5.2 Strategy Execution & Signal Engine

1. **`StrategyLoader`**:
   - Cryptographically verifies SHA-256 hashes of strategy and helper source code against the frozen benchmark manifest prior to execution.
   - Fails startup if code has been modified without an updated manifest.
2. **`StrategyAdapter`**:
   - Converts internal `Candle` models to standard Pandas DataFrames.
   - Invokes `populate_indicators`, `populate_entry_trend`, and `populate_exit_trend` without duplicating or modifying approved strategy code.
3. **`SignalEngine`**:
   - Evaluates strategy signals strictly upon closed candle events (`closed_candle_only: true`).
   - Generates deterministic, immutable `SignalEvent` objects containing:
     - `signal_id`: `SIG-{symbol}-{tf}-{candle_open_time}-{action}`
     - `reference_price`, `candle_close_time`, `reason`, and full indicator snapshot.
   - Guarantees zero repainting and idempotent signal emission.

### 5.3 Execution Brokers & Order Management

1. **Broker Hierarchy (`ExecutionBroker`)**:
   - **`ShadowBroker`**: Fully local simulation with zero network calls; tracks hypothetical positions and PnL.
   - **`PaperBroker`**: Simulates Binance USD-M Futures with margin verification, 1x leverage, 0.05% taker fees, PnL updates, and periodic account snapshots persisted to SQLite.
   - **`BinanceTestnetBroker`**: Authenticated REST client pointing to `https://testnet.binancefuture.com` with testnet credentials.
   - **`BinanceLiveBroker`**: Production REST client pointing to `https://fapi.binance.com`. Enforces `ESCANOR_LIVE_TRADING_ENABLED="true"` environment gate, synchronizes clock offset (`sync_time_offset`), and executes query-before-retry on network timeouts.
2. **`OrderManager`**:
   - Strict state machine: `CREATED -> SUBMITTING -> NEW -> PARTIALLY_FILLED -> FILLED` (with `UNKNOWN`, `CANCELED`, `REJECTED`, `EXPIRED`).
   - Terminal states are strictly absorbing.
   - Enforces monotonic cumulative fill quantities and fee progression.
3. **`PositionManager`**:
   - Derived **strictly from verified execution fills** (never mutated on order submission).
   - Computes weighted average entry price on scale-ins, realized PnL on scale-outs/closes, and tracks unrealized PnL via mark price.
4. **`SymbolFilters`**:
   - Dynamic public exchange information parsing (`PRICE_FILTER`, `LOT_SIZE`, `MIN_NOTIONAL`).
   - Decimal rounding: prices quantized to tick size (`ROUND_HALF_UP`), quantities rounded down to step size (`ROUND_DOWN`) to avoid over-allocation.
5. **`generate_client_order_id`**:
   - Deterministic client order ID generation: `ESC-{symbol}-{tf}-{candle_ts_sec}-{action}` (bounded to 36 characters for Binance compatibility).

### 5.4 Risk Management & Funded Safety Gates

1. **`RiskGuardEngine`**:
   Evaluates 8 pre-trade safety gates before any order submission:
   - Kill switch status (must be disengaged).
   - Active data gap / desync status (must be synchronized).
   - Maximum open positions limit (`max_open_trades=1`).
   - Candle staleness guard (rejects if latest candle is $>1800\text{ s}$ old).
   - Price sanity guard (rejects if execution price deviates $>2.0\%$ from candle close).
   - Maximum order notional limit ($\le \$50,000$).
   - Symbol lot size, step size, and minimum notional filter compliance.
   - Daily loss / drawdown circuit breaker ($-\$2,000$).
2. **`SafetyGateVerifier`**:
   - Evaluates all **24 mandatory funded activation gates** against runtime evidence prior to LIVE execution.
   - All 24 gates must be in `PASS` status.
3. **Canary Mode**:
   - Configurable capital allocation cap (`max_canary_allocation_usd`, default $\$100$) for cautious funded rollout.

### 5.5 Account Reconciliation & User Stream

1. **`BinanceUserDataStream`**:
   - Manages listenKey REST lifecycle (creation and 30-minute keep-alive loop).
   - Subscribes to private WebSocket stream to process `ORDER_TRADE_UPDATE` and `ACCOUNT_UPDATE`.
2. **`AccountReconciler`**:
   - Startup and periodic auditing of account balance, open positions, and unresolved orders.
   - In LIVE mode, any unresolvable position discrepancy triggers the emergency kill switch.

### 5.6 Persistence Layer (`EventStore`)

SQLite database in WAL mode (`PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;`) providing crash tolerance and an immutable audit ledger:
- `raw_aggtrades`: Full tick-by-tick trade records with exchange and arrival timestamps.
- `candles`: Reconstructed 1m and higher-timeframe OHLCV candles.
- `signals`: Deterministic strategy signals with indicator snapshots.
- `orders`: Complete order lifecycle records with status, fill prices, and fees.
- `incidents`: Operational warnings, gaps, and error events.
- `audit_events`: Structured event trail (`SYSTEM_INITIALIZED`, `POSITION_RECONCILIATION`, `PAPER_ACCOUNT_SNAPSHOT`).

---

## 6. Multi-Symbol Observability Web Dashboard

An interactive, responsive single-page web terminal served locally on `http://127.0.0.1:8080`:

- **TradingView Lightweight Charts**:
  - Interactive multi-timeframe candlestick view (1m, 3m, 5m, 15m, 30m, 1h, 4h).
  - Dynamic Supertrend line overlay calculated for the selected timeframe.
  - Interactive theme switching (Dark and Light modes).
  - Visual buy/sell execution markers plotted on filled orders.
- **Live Strategy Feed**: Real-time signal stream with candle close countdown and decision badges.
- **Audit & Order Log**: Tabular order tracking with client order IDs, target vs fill prices, fees, and execution status.
- **Account Summary Strip**: Total equity, cash balance, total return, win/loss count, profit factor, drawdown, and fees paid.
- **Multi-Symbol Aggregator**:
  - Seamlessly queries isolated databases (`data/btc_paper.db`, `data/lit_paper.db`, `data/zec_paper.db`) in read-only mode (`mode=ro`).
  - Endpoints: `GET /api/accounts`, `GET /api/data?symbol=...`, `GET /api/ping`.
  - Strictly read-only: `POST` requests are rejected with `405 Method Not Allowed`.
  - Prominent banner: `SIMULATED PAPER ACCOUNTS — NO REAL ORDERS`.

---

## 7. Execution Modes & Configuration Matrix

| Mode | External Orders | API Credentials | Portfolio Simulation | Database | Use Case |
|---|---|---|---|---|---|
| **SHADOW** (Default) | No (0 orders) | Not required | Local tracking | `data/<sym>_shadow.db` | Strategy verification, zero risk |
| **PAPER** | No (0 orders) | Not required | Margin, fees, PnL | `data/<sym>_paper.db` | Simulated trading, live forward testing |
| **TESTNET** | Yes (Testnet) | Testnet keys | Testnet exchange | `data/<sym>_testnet.db` | Integration testing on Binance testnet |
| **LIVE** | Yes (Real capital) | Funded keys | Binance USD-M | `data/<sym>_live.db` | Production funded trading (all 34 gates pass) |

Symbol availability: `BTCUSDT` and `HYPEUSDT` permit all four modes; `LITUSDT` and `ZECUSDT`
fail closed on TESTNET/LIVE via their manifest `allowed_modes`.

---

## 8. Historical Parity Replay Verification

Historical decision parity is enforced through [`live_engine/parity/replay.py`](file:///E:/EC/LiveBot/live_engine/parity/replay.py):
- **Fast Candle Replay**: Streams historical strategy-timeframe candles through `SignalEngine` and compares trades against the benchmark CSV.
- **Thorough Raw aggTrade Replay**: Streams raw historical tick-by-tick `aggTrade` parquet files through `MinuteCandleBuilder` $\rightarrow$ `TimeframeResampler` $\rightarrow$ `SignalEngine`.
- **Validation Criteria**:
  - `status == PASS`
  - `strategy_decision_parity_pct == 100.0%`
  - `matched_trades_count == benchmark_trades_count`
  - Zero trade direction or timing mismatches.

CLI invocation:
```powershell
python -m live_engine.main --config config/btc-paper.json --parity-check
python -m live_engine.main --config config/btc-paper.json --parity-check --raw-aggtrades
```

---

## 9. Implementation Status Checklist

- [x] **Phase 1**: Repository audit & architecture modernization
- [x] **Phase 2**: Approved benchmark manifests & SHA-256 hash locking
- [x] **Phase 3**: Strongly-typed market data models (`AggTrade`, `Candle`, `SignalEvent`, `Order`)
- [x] **Phase 4**: Historical aggTrade parquet streaming adapter
- [x] **Phase 5**: Deterministic 1-minute canonical candle builder
- [x] **Phase 6**: UTC-aligned timeframe resampler (1m to 15m, 1h)
- [x] **Phase 7**: Strategy adapter executing approved strategies without modifications
- [x] **Phase 8**: Historical parity replay runner (candle and raw aggTrade modes)
- [x] **Phase 9**: 100% benchmark decision parity confirmed across all approved models
- [x] **Phase 10**: Live Binance public aggTrade WebSocket client with exponential backoff
- [x] **Phase 11**: Real-time trade sequence gap detection and REST backfill recovery
- [x] **Phase 12**: Secondary Binance 1m kline validation listener
- [x] **Phase 13**: 250-candle historical warmup loading and staleness validation
- [x] **Phase 14**: Live SHADOW mode runner
- [x] **Phase 15**: PAPER execution broker with margin accounting and taker fee deduction
- [x] **Phase 16**: Binance authenticated USD-M Futures REST broker
- [x] **Phase 17**: Dynamic symbol filter parsing (`PRICE_FILTER`, `LOT_SIZE`, `MIN_NOTIONAL`) and Decimal rounding
- [x] **Phase 18**: Monotonic order lifecycle state machine
- [x] **Phase 19**: Idempotent client order ID generation and query-before-retry UNKNOWN handling
- [x] **Phase 20**: Binance User Data Stream with listenKey 30-minute keep-alive loop
- [x] **Phase 21**: Position manager derived strictly from verified execution fills
- [x] **Phase 22**: Startup and periodic account reconciliation layer
- [x] **Phase 23**: Bracket order exit simulation (Take-Profit, Stop-Loss, Time limits)
- [x] **Phase 24**: Operational safety guards and persistent file-based kill switch
- [x] **Phase 25**: SQLite WAL persistent event store (`raw_aggtrades`, `candles`, `signals`, `orders`, `incidents`, `audit_events`)
- [x] **Phase 26**: Database path isolation underneath `data/` and traversal protection
- [x] **Phase 27**: Multi-symbol candidate strategy support (LITUSDT & ZECUSDT in SHADOW/PAPER)
- [x] **Phase 28**: Interactive dark/light mode TradingView web dashboard with multi-timeframe switching
- [x] **Phase 29**: Multi-symbol paper dashboard aggregator (`/api/accounts`, `/api/data`)
- [x] **Phase 30**: Orchestrated PowerShell automation scripts (`run-btc-lit-zec-paper-dashboard.ps1`, `run-lit-zec-paper-shadow.ps1`)
- [x] **Phase 31**: Mandatory 24 funded safety gates verifier
- [x] **Phase 32**: Comprehensive test suite

### Correctness & Safety Release

- [x] **Phase 33**: Single delta-based fill path (`live_engine/execution/fill_applier.py`) with an append-only `fills` ledger; REST responses, `ORDER_TRADE_UPDATE` events and reconciliation all apply `new_cumulative - already_applied` and nothing else
- [x] **Phase 34**: Position-safe futures exits — position-mode detection, `reduceOnly` / explicit `positionSide`, authoritative pre-exit clamping, and the same no-reversal invariant in SHADOW and PAPER
- [x] **Phase 35**: Correct USD-M user data stream (`<base>/ws/<listenKey>`), key rotation on keepalive failure and `listenKeyExpired`, jittered bounded backoff, non-blocking REST via `asyncio.to_thread`, orphan-free shutdown
- [x] **Phase 36**: UNKNOWN-order recovery with a documented absence proof, plus full reconciliation of balances, account configuration, fills, orders and position (idempotent across restarts)
- [x] **Phase 37**: Exact, asynchronous, fail-closed aggTrade gap recovery (`live_engine/market_data/gap_recovery.py`) with batch persistence and durable last-accepted-ID tracking
- [x] **Phase 38**: Explicit `entry_fill_model` / `exit_fill_model` in every manifest and a persisted pending-action state for `next_open` execution
- [x] **Phase 39**: Documented protective-exit semantics, persisted protective state and linkage, and an explicit unprotected-interval health condition (`live_engine/risk/protective.py`)
- [x] **Phase 40**: Authoritative equity, mark-price freshness and a persisted UTC start-of-day drawdown snapshot (`live_engine/risk/equity.py`)
- [x] **Phase 41**: Immutable execution-quality telemetry with T0-T4 monotonic milestones and a benchmark-versus-observed report (`live_engine/monitoring/telemetry.py`)
- [x] **Phase 42**: Portfolio, candle and raw-coverage parity with hash-bound reports (`live_engine/parity/portfolio.py`)
- [x] **Phase 43**: PAPER execution fidelity — explicit execution model, exchange filters, funding, mark-to-market and simulated-value labelling
- [x] **Phase 44**: Evidence-based safety gates with source, value, timestamp, expiry and failure reason
- [x] **Phase 45**: Health supervision, deduplicated incident alerting with recovery notifications, and a graceful stop path (`live_engine/risk/health.py`, `scripts/escanor-control.ps1`)
- [x] **Phase 46**: Reproducible install (`requirements.txt`), strict configuration parsing, and versioned forward-only persistence migrations
- [x] **Phase 47**: HYPEUSDT Rank-12 candidate — exact indicators, taker-buy volume, 12-slot aggTrade exits, 3× PAPER sizing, isolated databases/dashboard, and hash-bound oracle verification
- [x] **Phase 48**: HYPEUSDT Production LIVE Readiness — Testnet/Live configurations, evidence tooling, and operator controls (`config/hype-testnet.json`, `config/hype-live.json`, `scripts/record_hype_live_evidence.py`, `scripts/run-hype.ps1` TESTNET/LIVE guards, two-stage gate evaluation with runtime evidence, slot-book protective reconciliation, telemetry evidence read from the instance that captured it, `tests/test_hype_live_readiness.py`)

---

## 10. Correctness Release Subsystems

### 10.1 Fill Application (`execution/fill_applier.py`)

Binance reports `z` as a *cumulative* filled quantity, and the same fill arrives twice —
once in the REST order response and again on the user data stream. Every source now routes
through one applier that:

- appends the raw execution to an immutable `fills` table keyed on the exchange trade ID
  (`UNIQUE (client_order_id, dedup_key)`), so replaying an execution is harmless,
- applies only `delta = new_cumulative - previously_applied`,
- accumulates commission per event instead of overwriting a running total,
- clamps a reduce-only fill to the open quantity so it can never reverse the position,
- rebuilds identical order and position state from the ledger after a restart.

### 10.2 Health & Supervision (`risk/health.py`)

`HealthMonitor` owns the runtime state machine and per-component heartbeat freshness;
`HealthSupervisor` turns that state into deduplicated incidents with recovery
notifications; `IncidentAlerter` writes a local structured sink (`logs/incidents.jsonl`)
and optionally forwards to a webhook whose URL comes only from the environment. Alert
bodies are scrubbed of anything resembling a credential.

### 10.3 Parity Verification (`parity/portfolio.py`)

The parity report now separates the **arithmetic sum of trade percentages** from the
**compounded portfolio return** computed on a real equity curve, compares every
evaluation-window candle field at tick resolution, validates that the declared raw monthly
files cover the whole window before replay starts, and binds its verdict to SHA-256 hashes
of every input plus the tool version — so a stale report is detectable rather than trusted.

### 10.4 Safety Gates (`risk/safety_gates.py`)

Each gate resolves to an `Evidence` record carrying source, value, observation time,
expiry policy and failure reason. Missing, stale or unverifiable evidence fails closed. A
source string or an existing test file satisfies nothing;
`ESCANOR_LIVE_TRADING_ENABLED=true` is an additional operator acknowledgement on top of
evidence, never a replacement for it.
