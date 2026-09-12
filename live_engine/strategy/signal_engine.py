"""Deterministic Signal Engine generating immutable SignalEvents."""
from __future__ import annotations

from decimal import Decimal
from typing import Dict, List, Optional, Set

from live_engine.execution.models import SignalAction, SignalEvent
from live_engine.market_data.models import Candle
from live_engine.strategy.adapter import StrategyAdapter


class SignalEngine:
    """Manages strategy candle history and produces deterministic SignalEvents."""

    def __init__(self, adapter: StrategyAdapter, max_history: int = 1000):
        self.adapter = adapter
        self.manifest = adapter.manifest
        self.benchmark_id = self.manifest["benchmark_id"]
        self.strategy_hash = self.manifest["strategy"]["source_hash"]
        self.symbol = self.manifest["symbol"]
        self.timeframe = self.manifest["data"]["strategy_timeframe"]
        self.max_history = max_history

        self.candle_history: List[Candle] = []
        self._emitted_signal_keys: Set[str] = set()

    def prime_history(self, candles: List[Candle]) -> None:
        """Prime the engine with historical warm-up candles."""
        self.candle_history = candles[-self.max_history :].copy()

    def on_candle_close(self, candle: Candle) -> Optional[SignalEvent]:
        """Process a newly completed strategy candle.

        Returns SignalEvent if an entry or exit action is triggered, else None.
        """
        if candle.timeframe != self.timeframe:
            return None

        # Append to rolling history
        self.candle_history.append(candle)
        if len(self.candle_history) > self.max_history:
            self.candle_history.pop(0)

        action, ref_price, reason, snapshot = self.adapter.evaluate_latest_candle(self.candle_history)

        if action == SignalAction.NO_ACTION:
            return None

        # Deterministic deduplication key
        sig_key = f"{self.symbol}_{self.timeframe}_{candle.open_time}_{action.value}"
        if sig_key in self._emitted_signal_keys:
            return None
        self._emitted_signal_keys.add(sig_key)

        sig_id = f"SIG-{self.symbol}-{self.timeframe}-{candle.open_time}-{action.value}"
        signal = SignalEvent(
            signal_id=sig_id,
            benchmark_id=self.benchmark_id,
            strategy_hash=self.strategy_hash,
            symbol=self.symbol,
            timeframe=self.timeframe,
            candle_open_time=candle.open_time,
            candle_close_time=candle.close_time,
            generated_at=candle.close_time + 1,
            action=action,
            reference_price=ref_price,
            reason=reason,
            indicator_snapshot=snapshot,
        )

        return signal
