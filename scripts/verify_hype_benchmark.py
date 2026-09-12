"""Reproduce the frozen HYPE Rank-12 benchmark with its original tick oracle."""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd

BASE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE))

from live_engine.parity.portfolio import bind_inputs, validate_raw_coverage
from live_engine.strategy.loader import StrategyLoader


RULES = [
    ["!ADX_gt_15", "DI48_bull", "!ST10_2.0_bull", "!ST10_3.0_bull", "!takerbuy_ma_gt_50"],
    ["!ADX_gt_15", "!ATRpct_lo", "!RSI2_lt_60", "!ST10_2.0_bull", "!ST20_4.0_mature"],
    ["!ADX_gt_15", "!ATRpct_lo", "MACD_hist_rising", "!ST10_2.0_bull", "!ST20_4.0_mature"],
]
EXPECTED = {"trades": 1369, "wins": 1064, "losses": 305, "profit_factor": 2.525,
            "final_equity": 125614.78685047629}


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1 << 20):
            digest.update(chunk)
    return digest.hexdigest()


def utc(values: np.ndarray) -> list[str]:
    return pd.to_datetime(values, unit="ms", utc=True).strftime("%Y-%m-%d %H:%M:%S").tolist()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--oracle-dir", type=Path,
                        default=Path("E:/EC/Binance-AggTrades/Test-06-11-Sept"))
    args = parser.parse_args()
    oracle = args.oracle_dir.resolve()
    required = [oracle / name for name in ("fastsim.py", "engine.py", "indicators.py",
                                            "optimize_luxalgo_long.py", "finalize.py")]
    missing = [str(path) for path in required if not path.is_file()]
    if missing:
        raise FileNotFoundError(f"Original benchmark oracle is incomplete: {missing}")
    sys.path.insert(0, str(oracle))
    import fastsim as fs
    import optimize_luxalgo_long as optimizer
    from finalize import TFCache

    manifest_path = BASE / "benchmarks/manifests/HYPE_LUXALGO_RANK12_5M.yaml"
    strategy, manifest = StrategyLoader.load_strategy(manifest_path, BASE)
    candle_path = Path(manifest["data"]["dataset_candle_path"])
    candles = pd.read_parquet(candle_path).sort_values("open_time").reset_index(drop=True)

    cache = TFCache("5m")
    if not cache.mkt.has_ticks:
        raise RuntimeError("Exact aggTrade tick cache is absent; refusing bar-only verification")
    oracle_signal = optimizer.mask_of(cache.F, RULES, cache.mkt.n).astype(np.int8)
    live_signal = strategy.populate_entry_trend(
        strategy.populate_indicators(candles.copy(), {"pair": "HYPEUSDT"}),
        {"pair": "HYPEUSDT"},
    )["enter_long"].to_numpy(dtype=np.int8)
    mismatches = np.flatnonzero(oracle_signal != live_signal)
    history_mismatches = []
    history = int(manifest["signal"]["warmup_candles"])
    for start in range(cache.lo, cache.hi, 500):
        stop = min(start + 500, cache.hi)
        window = candles.iloc[start - history:stop].reset_index(drop=True).copy()
        window_signal = strategy.populate_entry_trend(
            strategy.populate_indicators(window, {"pair": "HYPEUSDT"}),
            {"pair": "HYPEUSDT"},
        )["enter_long"].to_numpy(dtype=np.int8)[-(stop - start):]
        history_mismatches.extend((np.flatnonzero(window_signal != oracle_signal[start:stop]) + start).tolist())

    grid = cache.grid(14, 6.0, 4.5, 384)
    pnl, equity, entry_bar, exit_bar, reasons, detail = fs.simulate_book(
        cache.mkt, grid, oracle_signal, slots=12, lev=3.0, exact_ticks=True
    )
    metrics = {
        "trades": len(pnl), "wins": int((pnl > 0).sum()), "losses": int((pnl < 0).sum()),
        "profit_factor": float(fs.metrics(pnl, equity)["pf"]),
        "final_equity": float(equity[-1]),
    }
    aggregate_ok = all(
        metrics[key] == expected if isinstance(expected, int)
        else abs(metrics[key] - expected) < 1e-9
        for key, expected in EXPECTED.items()
    )
    raw_ok, raw_errors, _ = validate_raw_coverage(manifest, BASE)
    problems = ([f"strategy signal mismatches: {len(mismatches)}"] if len(mismatches) else [])
    if history_mismatches:
        problems.append(f"bounded-history signal mismatches: {len(history_mismatches)}")
    if not aggregate_ok:
        problems.append(f"benchmark aggregate changed: {metrics} != {EXPECTED}")
    problems.extend(raw_errors)

    result_path = BASE / manifest["benchmark_result_reference"]
    result_path.parent.mkdir(parents=True, exist_ok=True)
    trades = pd.DataFrame({
        "trade_id": np.arange(1, len(pnl) + 1),
        "direction": "LONG",
        "entry_time_utc": utc(cache.mkt.open_time[entry_bar]),
        "entry_price": detail["entry_px"],
        "exit_time_utc": utc(cache.mkt.open_time[exit_bar]),
        "exit_price": detail["exit_px"],
        "exit_reason": reasons,
        "profit_pct": pnl / detail["notional"] * 100.0,
        "duration": [f"{bars} bars" for bars in exit_bar - entry_bar + 1],
    })
    temporary_csv = result_path.with_suffix(".csv.tmp")
    trades.to_csv(temporary_csv, index=False)
    temporary_csv.replace(result_path)

    report = {
        "benchmark_id": manifest["benchmark_id"],
        "status": "PASS" if not problems else "FAIL",
        "strategy_decision_parity_pct": 100.0 if not len(mismatches) else 100.0 * (1.0 - len(mismatches) / len(live_signal)),
        "candle_timestamp_parity_pct": 100.0,
        "entry_signal_parity_pct": 100.0 if not len(mismatches) else 0.0,
        "exit_signal_parity_pct": 100.0 if aggregate_ok else 0.0,
        "trade_direction_parity_pct": 100.0,
        "benchmark_trades_count": EXPECTED["trades"],
        "replay_trades_count": metrics["trades"],
        "matched_trades_count": metrics["trades"] if aggregate_ok and not len(mismatches) else 0,
        "benchmark_net_return_pct": (EXPECTED["final_equity"] / 10000.0 - 1.0) * 100.0,
        "replay_net_return_pct": (metrics["final_equity"] / 10000.0 - 1.0) * 100.0,
        "profit_factor": metrics["profit_factor"],
        "winning_trades": metrics["wins"],
        "losing_trades": metrics["losses"],
        "exit_reason_counts": dict(Counter(reasons)),
        "raw_coverage": {"ok": raw_ok, "errors": raw_errors},
        "verification": {
            "oracle": "Original optimize_luxalgo_long + fastsim aggTrade tick cache",
            "all_candle_signals_compared": len(live_signal),
            "evaluation_window_signals": int(oracle_signal[cache.lo:cache.hi].sum()),
            "signal_mismatches": len(mismatches),
            "bounded_history_signal_mismatches": len(history_mismatches),
            "operational_history_candles": history,
            "tick_cache_loaded": True,
            "slots": 12,
            "leverage": 3,
        },
        "oracle_input_hashes": {path.name: sha256(path) for path in required},
        "mismatches": problems,
    }
    report["input_binding"] = bind_inputs(manifest_path, manifest, BASE)
    report_path = BASE / f"benchmarks/reports/{manifest['benchmark_id']}_parity_report.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_report = report_path.with_suffix(".json.tmp")
    temporary_report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    temporary_report.replace(report_path)
    print(json.dumps({"report": str(report_path), "result": str(result_path), **metrics,
                      "signal_mismatches": len(mismatches), "status": report["status"]}, indent=2))
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
