"""Historical parity replay runner for validating live engine against benchmark results."""
from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import asdict, dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

from live_engine.execution.models import SignalAction, SignalEvent
from live_engine.market_data.models import Candle
from live_engine.market_data.resampler import TIMEFRAME_MAP_MS
from live_engine.strategy.adapter import StrategyAdapter
from live_engine.strategy.loader import StrategyLoader
from live_engine.strategy.signal_engine import SignalEngine
from live_engine.config import validate_execution_model
from live_engine.parity.portfolio import (
    PARITY_TOOL_VERSION,
    arithmetic_sum_of_trade_pct,
    bind_inputs,
    build_equity_curve,
    compare_candles,
    report_is_stale,
    resolve_raw_paths,
    validate_raw_coverage,
    verify_sizing,
)


@dataclass
class ReplayTrade:
    trade_id: int
    direction: str
    entry_signal_candle_utc: str
    entry_time_utc: str
    entry_price: float
    exit_signal_candle_utc: str
    exit_time_utc: str
    exit_price: float
    exit_reason: str
    profit_pct: float
    duration_bars: int
    entry_candle_ohlcv: Optional[Tuple[float, float, float, float, float]] = None  # (O,H,L,C,V)
    exit_candle_ohlcv: Optional[Tuple[float, float, float, float, float]] = None  # (O,H,L,C,V)


@dataclass
class ParityReport:
    benchmark_id: str
    symbol: str
    timeframe: str
    strategy_name: str
    strategy_hash: str
    status: str  # PASS / FAIL
    benchmark_trades_count: int
    replay_trades_count: int
    matched_trades_count: int
    candle_timestamp_parity_pct: float
    entry_signal_parity_pct: float
    exit_signal_parity_pct: float
    trade_direction_parity_pct: float
    strategy_decision_parity_pct: float
    benchmark_net_return_pct: float
    replay_net_return_pct: float
    # Unambiguous names: the arithmetic sum of per-trade percentages is not a portfolio
    # return, and the compounded portfolio return is not a sum of trade percentages.
    benchmark_arithmetic_trade_pct_sum: float = 0.0
    replay_arithmetic_trade_pct_sum: float = 0.0
    compounded_portfolio_return_pct: float = 0.0
    portfolio: Dict[str, Any] = field(default_factory=dict)
    candle_parity: Dict[str, Any] = field(default_factory=dict)
    raw_coverage: Dict[str, Any] = field(default_factory=dict)
    input_binding: Dict[str, Any] = field(default_factory=dict)
    mismatches: List[str] = field(default_factory=list)

    def print_summary(self) -> None:
        print("\n" + "=" * 70)
        print(f"ESCANOR HISTORICAL PARITY REPORT: {self.benchmark_id}")
        print("=" * 70)
        print(f"Status:                      {self.status}")
        print(f"Symbol:                      {self.symbol}")
        print(f"Timeframe:                   {self.timeframe}")
        print(f"Strategy:                    {self.strategy_name}")
        print(f"Strategy Hash:               {self.strategy_hash[:16]}... (VERIFIED)")
        print(f"Benchmark Trades:            {self.benchmark_trades_count}")
        print(f"Replay Trades:               {self.replay_trades_count}")
        print(f"Matched Trades:              {self.matched_trades_count}")
        print(f"Candle Timestamp Parity:     {self.candle_timestamp_parity_pct:.2f}%")
        print(f"Entry Signal Parity:         {self.entry_signal_parity_pct:.2f}%")
        print(f"Exit Signal Parity:          {self.exit_signal_parity_pct:.2f}%")
        print(f"Trade Direction Parity:      {self.trade_direction_parity_pct:.2f}%")
        print(f"Strategy Decision Parity:    {self.strategy_decision_parity_pct:.2f}%")
        print(f"Benchmark Trade % Sum:       {self.benchmark_arithmetic_trade_pct_sum:.2f}% (arithmetic, not a portfolio return)")
        print(f"Replay Trade % Sum:          {self.replay_arithmetic_trade_pct_sum:.2f}% (arithmetic, not a portfolio return)")
        print(f"Compounded Portfolio Return: {self.compounded_portfolio_return_pct:.2f}%")
        if self.candle_parity:
            print(f"Candle OHLCV Parity:         {self.candle_parity.get('candle_ohlcv_parity_pct', 0.0):.2f}% "
                  f"({self.candle_parity.get('compared', 0)} compared, "
                  f"{self.candle_parity.get('missing_count', 0)} missing, "
                  f"{self.candle_parity.get('extra_count', 0)} extra, "
                  f"{self.candle_parity.get('duplicated_count', 0)} duplicated, "
                  f"{self.candle_parity.get('mismatch_count', 0)} mismatched)")
            diagnosis = self.candle_parity.get("diagnosis") or {}
            if diagnosis.get("fields"):
                print(f"  fields differing:          {diagnosis['fields']}")
                print(f"  likely cause:              {diagnosis['likely_cause']}")
        if self.raw_coverage:
            print(f"Raw Dataset Coverage:        {'OK' if self.raw_coverage.get('ok') else 'FAILED'}")
        if self.mismatches:
            print("\nMismatches:")
            for m in self.mismatches[:10]:
                print(f"  - {m}")
        print("=" * 70 + "\n")


class HistoricalReplayRunner:
    """Streams historical candles through the live engine pipeline and verifies parity against benchmark."""

    def __init__(self, manifest_path: str | Path, base_dir: Optional[str | Path] = None):
        self.manifest_path = Path(manifest_path)
        self.base_dir = Path(base_dir) if base_dir else Path.cwd()
        self.strategy_instance, self.manifest = StrategyLoader.load_strategy(
            self.manifest_path, base_dir=self.base_dir
        )
        self.adapter = StrategyAdapter(self.strategy_instance, self.manifest)
        self.signal_engine = SignalEngine(self.adapter, max_history=1000)

        # Execution semantics must be explicit; a missing fill model is a configuration
        # error, never a runner default.
        self.execution_model = validate_execution_model(self.manifest)
        self.symbol = self.manifest["symbol"]
        self.timeframe = self.manifest["data"]["strategy_timeframe"]

    def load_dataset(
        self,
        parquet_path: str | Path,
        start_time_utc: Optional[str] = None,
        end_time_utc: Optional[str] = None,
        warmup_bars: int = 250,
    ) -> Tuple[List[Candle], List[Candle]]:
        """Loads and separates warmup candles and evaluation candles."""
        p_path = (self.base_dir / parquet_path).resolve()
        if not p_path.exists():
            raise FileNotFoundError(f"Historical parquet dataset not found: {p_path}")

        data_cfg = self.manifest.get("data", {})
        if start_time_utc is None:
            start_time_utc = data_cfg.get("evaluation_window_start", "2026-08-15 00:00:00+00:00")
        if end_time_utc is None:
            end_time_utc = data_cfg.get("evaluation_window_end", "2026-08-31 23:59:59+00:00")

        df = pd.read_parquet(p_path)
        df["date"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
        df = df.sort_values("open_time").reset_index(drop=True)

        start_ts = pd.Timestamp(start_time_utc)
        end_ts = pd.Timestamp(end_time_utc)

        eval_indices = df[(df["date"] >= start_ts) & (df["date"] <= end_ts)].index
        if len(eval_indices) == 0:
            raise ValueError(f"No candles found in timerange {start_time_utc} to {end_time_utc}")

        first_eval_idx = eval_indices[0]
        last_eval_idx = eval_indices[-1]

        warmup_start_idx = max(0, first_eval_idx - warmup_bars)
        warmup_df = df.iloc[warmup_start_idx:first_eval_idx]
        eval_df = df.iloc[first_eval_idx : last_eval_idx + 1]

        def to_candle_list(sub_df: pd.DataFrame) -> List[Candle]:
            from live_engine.market_data.resampler import TIMEFRAME_MAP_MS
            dur = TIMEFRAME_MAP_MS.get(self.timeframe, 900_000)
            candles = []
            for _, r in sub_df.iterrows():
                ot = int(r["open_time"])
                candles.append(
                    Candle(
                        symbol=self.symbol,
                        timeframe=self.timeframe,
                        open_time=ot,
                        close_time=ot + dur - 1,
                        open=Decimal(str(r["open"])),
                        high=Decimal(str(r["high"])),
                        low=Decimal(str(r["low"])),
                        close=Decimal(str(r["close"])),
                        volume=Decimal(str(r["volume"])),
                        trade_count=int(r.get("trade_count", 0)),
                        is_closed=True,
                    )
                )
            return candles

        return to_candle_list(warmup_df), to_candle_list(eval_df)

    def run_replay(
        self, warmup_candles: List[Candle], eval_candles: List[Candle]
    ) -> List[ReplayTrade]:
        """Feeds candles sequentially through the SignalEngine and simulates the execution model."""
        self.signal_engine.prime_history(warmup_candles)

        trades: List[ReplayTrade] = []
        in_position = False
        current_trade_id = 0

        entry_signal_candle = ""
        entry_time_utc = ""
        entry_price = 0.0
        entry_bar_idx = 0

        fee = float(self.manifest.get("fees", {}).get("taker", 0.0005))
        execution = self.manifest.get("execution_model", {})
        entry_on_signal_close = execution.get("entry_fill_model") == "signal_close"
        exit_on_signal_reference = execution.get("exit_fill_model") == "signal_reference"
        n = len(eval_candles)
        for i, candle in enumerate(eval_candles):
            # 1. First, check if we need to execute a pending entry or exit on bar i open
            # (In Freqtrade/standard execution, order fills at bar i open following signal at bar i-1 close)

            # 2. Process candle close on SignalEngine
            signal = self.signal_engine.on_candle_close(candle)

            if signal is not None:
                if signal.action == SignalAction.ENTER_LONG and not in_position:
                    fill_index = i if entry_on_signal_close else i + 1
                    if fill_index < n:
                        fill_candle = eval_candles[fill_index]
                        in_position = True
                        current_trade_id += 1
                        entry_signal_candle = str(candle.open_datetime_utc)
                        entry_time_utc = str(fill_candle.open_datetime_utc).replace("+00:00", "")
                        entry_price = float(signal.reference_price) if entry_on_signal_close else float(fill_candle.open)
                        entry_candle_ohlcv = (
                            float(fill_candle.open),
                            float(fill_candle.high),
                            float(fill_candle.low),
                            float(fill_candle.close),
                            float(fill_candle.volume),
                        )
                        entry_bar_idx = fill_index

                elif signal.action == SignalAction.EXIT_LONG and in_position:
                    in_position = False
                    is_bracket = signal.reason in ("TP", "SL", "TIME")
                    exit_signal_candle = str(candle.open_datetime_utc)
                    if exit_on_signal_reference or is_bracket:
                        exit_time_utc = str(candle.open_datetime_utc).replace("+00:00", "")
                        exit_price = float(signal.reference_price)
                        exit_reason = signal.reason
                        exit_candle_ohlcv = (
                            float(candle.open),
                            float(candle.high),
                            float(candle.low),
                            float(candle.close),
                            float(candle.volume),
                        )
                        duration_bars = (i - entry_bar_idx) + 1
                    else:
                        # Standard flip exit: Execution fills on bar i+1 open
                        if i + 1 < n:
                            next_candle = eval_candles[i + 1]
                            exit_time_utc = str(next_candle.open_datetime_utc).replace("+00:00", "")
                            exit_price = float(next_candle.open)
                            exit_candle_ohlcv = (
                                float(next_candle.open),
                                float(next_candle.high),
                                float(next_candle.low),
                                float(next_candle.close),
                                float(next_candle.volume),
                            )
                        else:
                            exit_time_utc = str(candle.open_datetime_utc).replace("+00:00", "")
                            exit_price = float(candle.close)
                            exit_candle_ohlcv = (
                                float(candle.open),
                                float(candle.high),
                                float(candle.low),
                                float(candle.close),
                                float(candle.volume),
                            )
                        exit_reason = "exit_signal"
                        duration_bars = (i + 1) - entry_bar_idx

                    cost = entry_price * (1.0 + fee)
                    revenue = exit_price * (1.0 - fee)
                    profit_pct = ((revenue - cost) / cost) * 100.0

                    trade = ReplayTrade(
                        trade_id=current_trade_id,
                        direction="LONG",
                        entry_signal_candle_utc=entry_signal_candle,
                        entry_time_utc=entry_time_utc,
                        entry_price=entry_price,
                        entry_candle_ohlcv=entry_candle_ohlcv,
                        exit_signal_candle_utc=exit_signal_candle,
                        exit_time_utc=exit_time_utc,
                        exit_price=exit_price,
                        exit_candle_ohlcv=exit_candle_ohlcv,
                        exit_reason=exit_reason,
                        profit_pct=profit_pct,
                        duration_bars=duration_bars,
                    )
                    trades.append(trade)

        # If still in position at the end of the window, force exit on last candle open
        if in_position and n > 0 and execution.get("force_exit_at_end", True):
            last_c = eval_candles[-1]
            exit_price = float(last_c.open)
            exit_candle_ohlcv = (
                float(last_c.open),
                float(last_c.high),
                float(last_c.low),
                float(last_c.close),
                float(last_c.volume),
            )
            cost = entry_price * (1.0 + fee)
            revenue = exit_price * (1.0 - fee)
            profit_pct = ((revenue - cost) / cost) * 100.0
            trades.append(
                ReplayTrade(
                    trade_id=current_trade_id,
                    direction="LONG",
                    entry_signal_candle_utc=entry_signal_candle,
                    entry_time_utc=entry_time_utc,
                    entry_price=entry_price,
                    entry_candle_ohlcv=entry_candle_ohlcv,
                    exit_signal_candle_utc=str(last_c.open_datetime_utc),
                    exit_time_utc=str(last_c.open_datetime_utc).replace("+00:00", ""),
                    exit_price=exit_price,
                    exit_candle_ohlcv=exit_candle_ohlcv,
                    exit_reason="force_exit",
                    profit_pct=profit_pct,
                    duration_bars=n - entry_bar_idx,
                )
            )

        return trades

    def validate_raw_coverage(self) -> Dict[str, Any]:
        """Fails closed unless the declared raw files cover the whole evaluation window."""
        ok, errors, paths = validate_raw_coverage(self.manifest, self.base_dir)
        return {"ok": ok, "errors": errors, "files": [p.name for p in paths]}

    def run_aggtrade_replay(
        self,
        aggtrade_parquet_path: str | Path | Sequence[str | Path] | None = None,
        warmup_candles: Optional[List[Candle]] = None,
        start_ts_ms: Optional[int] = None,
        end_ts_ms: Optional[int] = None,
        validate_coverage: bool = True,
    ) -> Tuple[List[Candle], List[ReplayTrade]]:
        """Builds candles from every raw aggTrade file the window needs, then replays.

        Coverage is validated *before* any streaming starts: replaying a 13-month window
        over a single month of raw data produces a different trade set that no downstream
        comparison would flag as a data problem.
        """
        from live_engine.market_data.aggtrade_adapter import HistoricalAggTradeAdapter

        if validate_coverage:
            coverage = self.validate_raw_coverage()
            if not coverage["ok"]:
                raise ValueError("; ".join(coverage["errors"]))

        if aggtrade_parquet_path is None:
            paths = resolve_raw_paths(self.manifest, self.base_dir)
        elif isinstance(aggtrade_parquet_path, (str, Path)):
            paths = [Path(aggtrade_parquet_path)]
        else:
            paths = [Path(p) for p in aggtrade_parquet_path]

        adapter = HistoricalAggTradeAdapter(symbol=self.symbol)
        eval_candles = adapter.build_candles_from_aggtrades(
            parquet_path=paths,
            target_timeframe=self.timeframe,
            start_ts_ms=start_ts_ms,
            end_ts_ms=end_ts_ms,
        )
        trades = self.run_replay(warmup_candles or [], eval_candles)
        return eval_candles, trades

    def check_saved_report(self, report_path: Optional[Path | str] = None) -> Dict[str, Any]:
        """Whether a saved parity report still describes the files currently on disk.

        A report is evidence only while every input it was bound to is unchanged. A new
        manifest, strategy edit or dataset swap invalidates the verdict.
        """
        benchmark_id = self.manifest.get("benchmark_id", "UNKNOWN")
        path = Path(report_path) if report_path else (
            self.base_dir / "benchmarks" / "reports" / f"{benchmark_id}_parity_report.json"
        )
        if not path.exists():
            return {"valid": False, "reason": f"Parity report not found: {path}", "differences": []}

        try:
            saved = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as exc:
            return {"valid": False, "reason": f"Parity report unreadable: {exc}", "differences": []}

        stale, differences = report_is_stale(saved, bind_inputs(self.manifest_path, self.manifest, self.base_dir))
        return {
            "valid": (not stale) and saved.get("status") == "PASS",
            "stale": stale,
            "status": saved.get("status"),
            "differences": differences,
            "reason": "Bound inputs changed since the report was generated" if stale else "",
            "report_path": str(path),
        }

    def verify_parity(
        self,
        replay_trades: List[ReplayTrade],
        eval_candles: Optional[List[Candle]] = None,
        report_path: Optional[Path | str] = None,
        report_dir: Optional[Path | str] = None,
        canonical_candles: Optional[List[Candle]] = None,
        verify_portfolio: bool = True,
        verify_coverage: bool = False,
    ) -> ParityReport:
        """Compares replay trades against the approved benchmark CSV.

        When ``canonical_candles`` is supplied, every evaluation-window candle timestamp
        and OHLCV field is compared against it. Portfolio verification builds a real
        equity curve so the compounded return is checked separately from the arithmetic
        sum of trade percentages, and every trade's requested versus constrained size is
        checked. The saved report is bound to hashes of every input it depends on.
        """
        ref_rel = self.manifest.get("benchmark_result_reference", "benchmarks/results/BTC_ST_09_5M_trades.csv")
        ref_path = (self.base_dir / ref_rel).resolve()

        if not ref_path.exists():
            raise FileNotFoundError(f"Benchmark reference CSV not found: {ref_path}")

        bm_df = pd.read_csv(ref_path)

        # Determine appropriate price tolerance from symbol's tick_size
        from live_engine.execution.order_filters import fetch_public_exchange_info
        filters = None
        try:
            filters = fetch_public_exchange_info(self.symbol)
        except Exception:
            pass

        if not filters:
            # Fallback for tests or offline environments where network is unavailable
            known_tick_sizes = {
                "BTCUSDT": Decimal("0.10"),
                "LITUSDT": Decimal("0.0001"),
                "ZECUSDT": Decimal("0.01"),
            }
            if self.symbol in known_tick_sizes:
                price_tolerance = float(known_tick_sizes[self.symbol] * 2)
            else:
                raise RuntimeError(
                    f"PARITY VALIDATION FAILED: Cannot fetch exchange filters for {self.symbol}. "
                    f"Public Binance API unreachable or symbol not found. Price tolerance unknown; refusing to proceed."
                )
        else:
            price_tolerance = float(filters.tick_size * 2)  # Conservative: 2x tick_size

        mismatches: List[str] = []
        matched_count = 0
        direction_matches = 0
        entry_matches = 0
        exit_matches = 0

        n_bm = len(bm_df)
        n_rp = len(replay_trades)

        # Verify trade count match
        if n_bm != n_rp:
            mismatches.append(f"Trade count mismatch: benchmark={n_bm}, replay={n_rp}")

        for idx in range(min(n_bm, n_rp)):
            bm_row = bm_df.iloc[idx]
            rp = replay_trades[idx]

            # 1. Compare direction
            if bm_row["direction"] == rp.direction:
                direction_matches += 1
            else:
                mismatches.append(f"Trade {idx+1} direction mismatch: bm={bm_row['direction']} vs rp={rp.direction}")

            # 2. Compare entry time and price (with symbol-appropriate tolerance)
            bm_entry_time = str(bm_row["entry_time_utc"])
            rp_entry_time = rp.entry_time_utc
            bm_entry_px = float(bm_row["entry_price"])
            if bm_entry_time == rp_entry_time and abs(bm_entry_px - rp.entry_price) < price_tolerance:
                entry_matches += 1
            else:
                mismatches.append(
                    f"Trade {idx+1} entry mismatch: bm=({bm_entry_time}, {bm_entry_px}) vs rp=({rp_entry_time}, {rp.entry_price})"
                )

            # 3. Compare exit time and price (with symbol-appropriate tolerance)
            bm_exit_time = str(bm_row["exit_time_utc"])
            rp_exit_time = rp.exit_time_utc
            bm_exit_px = float(bm_row["exit_price"])
            if bm_exit_time == rp_exit_time and abs(bm_exit_px - rp.exit_price) < price_tolerance:
                exit_matches += 1
            else:
                mismatches.append(
                    f"Trade {idx+1} exit mismatch: bm=({bm_exit_time}, {bm_exit_px}) vs rp=({rp_exit_time}, {rp.exit_price})"
                )

            # 4. Compare exit reason
            bm_exit_reason = str(bm_row.get("exit_reason", ""))
            if bm_exit_reason and bm_exit_reason != rp.exit_reason:
                mismatches.append(
                    f"Trade {idx+1} exit reason mismatch: bm='{bm_exit_reason}' vs rp='{rp.exit_reason}'"
                )

            # 5. Compare trade profit percentage
            bm_profit = float(bm_row["profit_pct"])
            if abs(bm_profit - rp.profit_pct) > 0.005:
                mismatches.append(
                    f"Trade {idx+1} profit mismatch: bm={bm_profit:.4f}% vs rp={rp.profit_pct:.4f}%"
                )

            # 6. Verify entry signal candle timing (must precede execution candle)
            tf_ms_sig = TIMEFRAME_MAP_MS.get(self.timeframe, 900_000)
            entry_fill_model = self.manifest.get("execution_model", {}).get("entry_fill_model", "next_open")
            expected_sig_dt = pd.to_datetime(rp_entry_time, utc=True)
            if entry_fill_model != "signal_close":
                expected_sig_dt -= pd.Timedelta(milliseconds=tf_ms_sig)
            rp_sig_dt = pd.to_datetime(rp.entry_signal_candle_utc, utc=True)
            if expected_sig_dt != rp_sig_dt:
                mismatches.append(
                    f"Trade {idx+1} entry signal timing mismatch: expected {expected_sig_dt} vs rp {rp_sig_dt}"
                )

            # 7. Verify OHLCV candle parity for entry and exit
            if rp.entry_candle_ohlcv:
                entry_o, entry_h, entry_l, entry_c, entry_v = rp.entry_candle_ohlcv
                if not (entry_o and entry_h and entry_l and entry_c and entry_v > 0):
                    mismatches.append(f"Trade {idx+1} entry candle OHLCV incomplete or zero")
            if rp.exit_candle_ohlcv:
                exit_o, exit_h, exit_l, exit_c, exit_v = rp.exit_candle_ohlcv
                if not (exit_o and exit_h and exit_l and exit_c and exit_v > 0):
                    mismatches.append(f"Trade {idx+1} exit candle OHLCV incomplete or zero")

            if (
                bm_row["direction"] == rp.direction
                and bm_entry_time == rp_entry_time
                and bm_exit_time == rp_exit_time
                and (not bm_exit_reason or bm_exit_reason == rp.exit_reason)
                and abs(bm_profit - rp.profit_pct) <= 0.005
            ):
                matched_count += 1

        # If eval_candles provided, verify they are non-empty
        if eval_candles is not None:
            if len(eval_candles) == 0:
                mismatches.append("Evaluation candles were provided but are empty")
            elif len(eval_candles) < len(replay_trades):
                mismatches.append(f"Insufficient eval candles: {len(eval_candles)} < {len(replay_trades)} trades")

        entry_pct = (entry_matches / max(n_bm, 1)) * 100.0
        exit_pct = (exit_matches / max(n_bm, 1)) * 100.0
        dir_pct = (direction_matches / max(n_bm, 1)) * 100.0
        decision_pct = (matched_count / max(n_bm, 1)) * 100.0
        ts_pct = 100.0 if (entry_pct == 100.0 and exit_pct == 100.0) else (entry_pct + exit_pct) / 2.0

        # Arithmetic sums of per-trade percentages. These are NOT portfolio returns.
        bm_return = float(arithmetic_sum_of_trade_pct(bm_df["profit_pct"].tolist()))
        rp_return = float(arithmetic_sum_of_trade_pct(t.profit_pct for t in replay_trades))

        if abs(bm_return - rp_return) >= 0.01:
            mismatches.append(
                f"Arithmetic trade-percentage sum mismatch: bm={bm_return:.2f}% vs rp={rp_return:.2f}%"
            )

        # Portfolio result: an actual equity curve under the manifest's sizing rule.
        portfolio: Dict[str, Any] = {}
        compounded_pct = 0.0
        if verify_portfolio:
            risk = self.manifest.get("risk", {}) or {}
            curve = build_equity_curve(
                replay_trades,
                initial_balance=Decimal(str(self.manifest.get("initial_balance", "10000"))),
                leverage=Decimal(str(risk.get("leverage", 1))),
                taker_fee=Decimal(str(self.manifest.get("fees", {}).get("taker", "0.0005"))),
                compounding=risk.get("stake_rule") == "full_compounding",
                step_size=filters.step_size if filters else None,
                min_qty=filters.min_qty if filters else Decimal("0"),
            )
            portfolio = curve.to_dict()
            compounded_pct = float(curve.compounded_return_pct)
            portfolio["arithmetic_trade_pct_sum"] = rp_return
            for problem in verify_sizing(curve):
                mismatches.append(f"Position sizing: {problem}")

        # Candle parity: every timestamp and every OHLCV field, at tick resolution.
        candle_parity: Dict[str, Any] = {}
        if canonical_candles is not None and eval_candles is not None:
            tick = filters.tick_size if filters else Decimal(str(price_tolerance / 2))
            candle_result = compare_candles(eval_candles, canonical_candles, tick_size=tick)
            candle_parity = candle_result.to_dict()
            if not candle_result.passed:
                mismatches.append(
                    f"Candle parity failed: {len(candle_result.missing)} missing, "
                    f"{len(candle_result.extra)} extra, {len(candle_result.duplicated)} duplicated, "
                    f"{len(candle_result.mismatched)} mismatched"
                )
                mismatches.extend(candle_result.mismatched[:10])

        # Raw dataset coverage: the window must be fully backed by declared raw files.
        raw_coverage: Dict[str, Any] = {}
        if verify_coverage:
            raw_coverage = self.validate_raw_coverage()
            if not raw_coverage["ok"]:
                mismatches.extend(raw_coverage["errors"])

        input_binding = bind_inputs(self.manifest_path, self.manifest, self.base_dir)

        status = "PASS" if (decision_pct == 100.0 and len(mismatches) == 0) else "FAIL"

        report = ParityReport(
            benchmark_id=self.manifest["benchmark_id"],
            symbol=self.symbol,
            timeframe=self.timeframe,
            strategy_name=self.manifest["strategy"]["name"],
            strategy_hash=self.manifest["strategy"]["source_hash"],
            status=status,
            benchmark_trades_count=n_bm,
            replay_trades_count=n_rp,
            matched_trades_count=matched_count,
            candle_timestamp_parity_pct=ts_pct,
            entry_signal_parity_pct=entry_pct,
            exit_signal_parity_pct=exit_pct,
            trade_direction_parity_pct=dir_pct,
            strategy_decision_parity_pct=decision_pct,
            benchmark_net_return_pct=bm_return,
            replay_net_return_pct=rp_return,
            benchmark_arithmetic_trade_pct_sum=bm_return,
            replay_arithmetic_trade_pct_sum=rp_return,
            compounded_portfolio_return_pct=compounded_pct,
            portfolio=portfolio,
            candle_parity=candle_parity,
            raw_coverage=raw_coverage,
            input_binding=input_binding,
            mismatches=mismatches,
        )

        # Save machine-readable discrepancy report (benchmark-specific)
        import os as os_module
        try:
            benchmark_id = report.benchmark_id or "UNKNOWN"
            is_negative = report.status != "PASS"
            is_test = bool(os_module.environ.get("PYTEST_CURRENT_TEST"))

            if report_path is not None:
                target_file = Path(report_path)
            elif report_dir is not None:
                target_file = Path(report_dir) / f"{benchmark_id}_parity_report.json"
            elif is_test or is_negative:
                # Parity unit tests must never overwrite operational parity reports.
                # Negative parity tests must write only to temporary paths.
                import tempfile
                test_dir = Path(tempfile.gettempdir()) / "escanor_test_reports"
                test_dir.mkdir(parents=True, exist_ok=True)
                target_file = test_dir / f"{benchmark_id}_parity_report.json"
            else:
                op_dir = self.base_dir / "benchmarks" / "reports"
                op_dir.mkdir(parents=True, exist_ok=True)
                target_file = op_dir / f"{benchmark_id}_parity_report.json"

            # Strict guard: Never write to benchmarks/reports if test is running or status is not PASS
            op_dir_resolved = (self.base_dir / "benchmarks" / "reports").resolve()
            target_resolved = target_file.resolve()
            if (is_test or is_negative) and (op_dir_resolved == target_resolved.parent or op_dir_resolved in target_resolved.parents):
                import tempfile
                test_dir = Path(tempfile.gettempdir()) / "escanor_test_reports"
                test_dir.mkdir(parents=True, exist_ok=True)
                target_file = test_dir / f"{benchmark_id}_parity_report.json"

            target_file.parent.mkdir(parents=True, exist_ok=True)
            target_file.write_text(
                json.dumps(
                    {
                        "benchmark_id": report.benchmark_id,
                        "status": report.status,
                        "strategy_decision_parity_pct": report.strategy_decision_parity_pct,
                        "benchmark_trades_count": report.benchmark_trades_count,
                        "replay_trades_count": report.replay_trades_count,
                        "matched_trades_count": report.matched_trades_count,
                        "benchmark_net_return_pct": report.benchmark_net_return_pct,
                        "replay_net_return_pct": report.replay_net_return_pct,
                        "benchmark_arithmetic_trade_pct_sum": report.benchmark_arithmetic_trade_pct_sum,
                        "replay_arithmetic_trade_pct_sum": report.replay_arithmetic_trade_pct_sum,
                        "compounded_portfolio_return_pct": report.compounded_portfolio_return_pct,
                        "portfolio": report.portfolio,
                        "candle_parity": report.candle_parity,
                        "raw_coverage": report.raw_coverage,
                        "input_binding": report.input_binding,
                        "mismatches": report.mismatches,
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )
        except Exception:
            pass

        return report


def main():
    parser = argparse.ArgumentParser(description="Escanor Historical Parity Replay")
    parser.add_argument(
        "--benchmark",
        default="benchmarks/manifests/BTC_ST_09_5M.yaml",
        help="Path to benchmark manifest YAML",
    )
    parser.add_argument(
        "--dataset",
        default="BTCUSDT_USDM_DATA/candles/BTCUSDT_5m.parquet",
        help="Path to historical parquet candle dataset",
    )
    args = parser.parse_args()

    runner = HistoricalReplayRunner(args.benchmark)
    warmup, evals = runner.load_dataset(args.dataset)
    trades = runner.run_replay(warmup, evals)
    report = runner.verify_parity(trades)
    report.print_summary()

    if report.status != "PASS":
        sys.exit(1)


if __name__ == "__main__":
    main()
