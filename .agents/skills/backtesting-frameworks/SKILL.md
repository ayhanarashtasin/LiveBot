---
name: backtesting-frameworks
description: Historical replay, benchmark validation, look-ahead prevention, fees, slippage, point-in-time data, and parity analysis in Escanor. Do not optimize, replace, or silently change the approved strategy or benchmark parameters.
---

# Backtesting Frameworks & Parity Replay Skill

Institutional standards for validating 100% strategy decision parity against historical benchmarks.

## 1. When to Use This Skill

Activate this skill when:
- Running historical parity verification via `live_engine.parity.replay`.
- Validating raw tick-by-tick `aggTrade` parquet replay against historical candle benchmarks.
- Auditing look-ahead bias, future leak prevention, or timestamp alignment.
- Comparing simulated trades with benchmark CSV files (`FT_15M_trades.csv`, `LIT_15M_trades.csv`, `ZEC_15M_trades.csv`).

## 2. Fundamental Invariant

> **Fundamental Invariant**: Given the identical sequence of market events, the live trading engine and historical benchmark runner must produce the exact same strategy decisions (signal generation, entry candles, exit triggers, and position sizing).

## 3. Strict Prohibitions

- **NEVER** silently change indicator formulas, lengths, multipliers, or warm-up requirements.
- **NEVER** modify signal logic, entry conditions, or exit conditions to force a parity pass.
- **NEVER** engage in strategy curve-fitting, parameter optimization, or discretionary trade logic.
- Parity must be achieved by faithfully reconstructing the exact market data feed and executing the approved, frozen strategy source code.

## 4. Replay Verification Commands

Fast historical candle parity check:
```powershell
python -m live_engine.main --config config/btc-paper.json --parity-check
```

Thorough raw tick-by-tick aggTrade replay:
```powershell
python -m live_engine.main --config config/btc-paper.json --parity-check --raw-aggtrades
```

All parity reports must confirm `status == PASS` and `strategy_decision_parity_pct == 100.0%`.
