"""Adapter invoking the approved strategy code directly without duplication."""
from __future__ import annotations

from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from live_engine.execution.models import SignalAction
from live_engine.market_data.models import Candle


class StrategyAdapter:
    """Invokes the loaded strategy's native indicator and signal methods."""

    def __init__(self, strategy_instance: Any, manifest: Dict[str, Any]):
        self.strategy = strategy_instance
        self.manifest = manifest
        self.symbol = manifest["symbol"]
        self.timeframe = manifest["data"]["strategy_timeframe"]
        self.warmup_candles = manifest["signal"].get("warmup_candles", 250)

    def prepare_dataframe(self, candles: List[Candle]) -> pd.DataFrame:
        """Converts internal Candle list to standard strategy DataFrame."""
        records = []
        for c in candles:
            records.append({
                "date": c.open_datetime_utc,
                "open_time": c.open_time,
                "close_time": c.close_time,
                "open": float(c.open),
                "high": float(c.high),
                "low": float(c.low),
                "close": float(c.close),
                "volume": float(c.volume),
                "taker_buy_base_volume": float(c.taker_buy_base_volume),
            })
        df = pd.DataFrame(records)
        return df

    def evaluate_latest_candle(
        self, candles: List[Candle]
    ) -> Tuple[SignalAction, Decimal, str, Optional[Dict[str, float]]]:
        """Runs the approved strategy on the candle window and checks for signal on the latest closed candle.

        Returns (action, reference_price, reason, indicator_snapshot).
        """
        if len(candles) < self.warmup_candles:
            return SignalAction.NO_ACTION, Decimal("0"), f"WARMUP_INSUFFICIENT ({len(candles)}/{self.warmup_candles})", None

        df = self.prepare_dataframe(candles)
        metadata = {"pair": self.symbol}

        # 1. Run indicators
        df = self.strategy.populate_indicators(df, metadata)

        # 2. Run entry & exit trends
        df = self.strategy.populate_entry_trend(df, metadata)
        df = self.strategy.populate_exit_trend(df, metadata)

        last_row = df.iloc[-1]
        ref_price = Decimal(str(last_row["close"]))

        # Build indicator snapshot
        snapshot: Dict[str, float] = {}
        snapshot_fields = self.manifest.get("signal", {}).get("indicator_snapshot_fields") or [
            "supertrend", "supertrend_direction", "atr", "pullback_distance", "ema_fast", "ema_slow"
        ]
        for col in snapshot_fields:
            if col in last_row and pd.notna(last_row[col]):
                snapshot[col] = float(last_row[col])

        enter_long = bool(last_row.get("enter_long", 0) == 1)
        exit_long = bool(last_row.get("exit_long", 0) == 1)
        enter_short = bool(last_row.get("enter_short", 0) == 1)
        exit_short = bool(last_row.get("exit_short", 0) == 1)

        if enter_long:
            return SignalAction.ENTER_LONG, ref_price, "Approved Strategy Entry Long", snapshot
        elif exit_long:
            exit_px = Decimal(str(last_row["exit_price"])) if "exit_price" in last_row and pd.notna(last_row["exit_price"]) else ref_price
            exit_reason = str(last_row.get("exit_reason")) if "exit_reason" in last_row and last_row.get("exit_reason") else "Approved Strategy Exit Long"
            return SignalAction.EXIT_LONG, exit_px, exit_reason, snapshot
        elif enter_short:
            return SignalAction.ENTER_SHORT, ref_price, "Approved Strategy Entry Short", snapshot
        elif exit_short:
            return SignalAction.EXIT_SHORT, ref_price, "Approved Strategy Exit Short", snapshot

        return SignalAction.NO_ACTION, ref_price, "No Signal", snapshot
