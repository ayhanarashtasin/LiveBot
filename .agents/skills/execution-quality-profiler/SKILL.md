---
name: execution-quality-profiler
description: High-precision execution latency profiling, slippage tracking, and implementation shortfall analysis for Escanor live trading. Trigger automatically whenever the user asks about fill price drift, slippage, tick-to-trade latency, order execution delays, spread crossing impact, execution quality metrics, benchmark fill comparison, or adverse selection on Binance USD-M Futures.
---

# Execution Quality & Latency Profiler Skill

Quantitative engineering guide for measuring, auditing, and optimizing real-world execution quality, tick-to-trade latency, and slippage shortfall on Binance USD-M Futures.

## 1. When to Use This Skill

Activate this skill when:
- Evaluating execution performance differences between Paper, Testnet, and Live brokers.
- Auditing slippage: comparing actual executed fill prices against closed-candle signal prices.
- Profiling latency across the 5 critical pipeline milestones ($T_0$ to $T_4$).
- Diagnosing adverse selection during high-volatility 15m candle closes.
- Verifying whether exchange fee and spread assumptions match live reality.

## 2. The 5 Latency Milestones

```text
T0: Candle Close Event (UTC boundary, e.g. 15:00:00.000)
 │
 ▼ [AggTrade Ingestion & Flush Delay]
T1: AggTrade Received by WebSocket Client (local machine time)
 │
 ▼ [Resampling & Strategy Evaluation]
T2: Signal Generated & Risk Guards Passed (local machine time)
 │
 ▼ [Network REST Transmission & Binance Ingress]
T3: Binance Order Ack Received (Binance server time + network RTT)
 │
 ▼ [Matching Engine Execution]
T4: Order Fill Event (Private WebSocket ORDER_TRADE_UPDATE)
```

Key Metrics:
- **Pipeline Processing Latency**: $\Delta T_{\text{pipe}} = T_2 - T_1$ (Target: $< 5\text{ ms}$)
- **REST Network Round-Trip**: $\Delta T_{\text{rest}} = T_3 - T_2$ (Target: $< 80\text{ ms}$)
- **Total Tick-to-Ack Latency**: $\Delta T_{\text{total}} = T_3 - T_0$ (Target: $< 250\text{ ms}$)

## 3. Implementation Shortfall & Slippage Math

For each executed order:

$$\text{Slippage (USD)} = (\text{Fill Price} - \text{Signal Price}) \times \text{Side Multiplier}$$
$$\text{Where } \text{Side Multiplier} = +1 \text{ for BUY}, -1 \text{ for SELL}$$

$$\text{Slippage (bps)} = \frac{|\text{Fill Price} - \text{Signal Price}|}{\text{Signal Price}} \times 10,000$$

If 15m Supertrend breakout slippage exceeds 25 bps systematically:
- Check if order submission is delayed into the new 1m candle.
- Profile whether public WebSocket stream lags behind Binance direct REST time (`sync_time_offset`).

## 4. Telemetry Schema & Storage

Store execution telemetry alongside order records in SQLite:

```sql
CREATE TABLE IF NOT EXISTS execution_telemetry (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_order_id TEXT NOT NULL UNIQUE,
    symbol TEXT NOT NULL,
    signal_price REAL NOT NULL,
    fill_price REAL NOT NULL,
    slippage_bps REAL NOT NULL,
    t0_candle_close_ms INTEGER NOT NULL,
    t1_ws_recv_ms INTEGER NOT NULL,
    t2_signal_gen_ms INTEGER NOT NULL,
    t3_order_ack_ms INTEGER NOT NULL,
    t4_fill_event_ms INTEGER NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
```

## 5. Verification & Parity Rules

- Never alter the approved strategy's signal logic to compensate for slippage.
- All slippage and latency metrics must be recorded post-execution without interfering with order placement.
- Use `Decimal` for price and quantity calculations to avoid floating-point drift.
