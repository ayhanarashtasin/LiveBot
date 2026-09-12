"""Timeframe aggregation from canonical 1-minute candles into strategy timeframes."""
from __future__ import annotations

from decimal import Decimal
from typing import Dict, List, Optional

from live_engine.market_data.models import Candle

TIMEFRAME_MAP_MS: Dict[str, int] = {
    "1m": 60_000,
    "3m": 180_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "2h": 7_200_000,
    "4h": 14_400_000,
    "6h": 21_600_000,
    "8h": 28_800_000,
    "12h": 43_200_000,
    "1d": 86_400_000,
}


class TimeframeResampler:
    """Aggregates canonical 1-minute candles into higher timeframes (e.g. 15m, 1h).

    Ensures exact UTC boundary alignment matching historical benchmark formation.
    """

    def __init__(self, symbol: str, target_timeframe: str):
        self.symbol = symbol.upper()
        self.target_timeframe = target_timeframe.lower()

        if self.target_timeframe not in TIMEFRAME_MAP_MS:
            raise ValueError(
                f"Unsupported timeframe '{target_timeframe}'. Must be one of {list(TIMEFRAME_MAP_MS.keys())}"
            )

        self.tf_ms = TIMEFRAME_MAP_MS[self.target_timeframe]
        self.current_bucket_ms: Optional[int] = None

        self._open: Optional[Decimal] = None
        self._high: Optional[Decimal] = None
        self._low: Optional[Decimal] = None
        self._close: Optional[Decimal] = None
        self._volume: Decimal = Decimal("0")
        self._taker_buy_base_volume: Decimal = Decimal("0")
        self._trade_count: int = 0
        self._constituent_count: int = 0
        self._bucket_whole: bool = False

    def get_bucket_open_time(self, open_time_ms: int) -> int:
        """Returns the floor UTC bucket open time in milliseconds."""
        return (open_time_ms // self.tf_ms) * self.tf_ms

    def add_1m_candle(self, candle: Candle) -> Optional[Candle]:
        """Incorporate a completed 1-minute candle.

        If candle crosses into a new higher-timeframe bucket, the completed HTF candle is finalized and returned.
        """
        if candle.timeframe != "1m":
            raise ValueError(f"Expected 1m candle, got '{candle.timeframe}'")

        bucket = self.get_bucket_open_time(candle.open_time)
        completed_htf: Optional[Candle] = None

        # First candle ever
        if self.current_bucket_ms is None:
            self.current_bucket_ms = bucket
            self._init_bucket(candle)
            return None

        # Candle belongs to next bucket -> finalize previous and start new
        if bucket > self.current_bucket_ms:
            completed_htf = self.finalize_current_candle()
            self.current_bucket_ms = bucket
            self._init_bucket(candle)
            return completed_htf

        # Candle belongs to current bucket
        if bucket == self.current_bucket_ms:
            self._update_bucket(candle)
            # If this candle finishes the higher-timeframe bucket window, finalize immediately
            if candle.close_time >= self.current_bucket_ms + self.tf_ms - 1:
                return self.finalize_current_candle()
            return None

        # Candle belongs to an older bucket
        return None

    def _init_bucket(self, candle: Candle) -> None:
        # A bucket is only whole if we have its leading minute and every constituent was itself
        # whole. Joining the stream mid-bucket otherwise yields a wrong open/high/low/volume.
        # ponytail: a genuinely trade-less leading minute on an illiquid symbol also trips this
        # (bar is skipped, not wrong). Compare against REST klines if that ever matters.
        self._bucket_whole = candle.is_closed and candle.open_time == self.current_bucket_ms
        self._open = candle.open
        self._high = candle.high
        self._low = candle.low
        self._close = candle.close
        self._volume = candle.volume
        self._taker_buy_base_volume = candle.taker_buy_base_volume
        self._trade_count = candle.trade_count
        self._constituent_count = 1

    def _update_bucket(self, candle: Candle) -> None:
        assert self._high is not None
        assert self._low is not None

        if candle.high > self._high:
            self._high = candle.high
        if candle.low < self._low:
            self._low = candle.low

        self._close = candle.close
        self._volume += candle.volume
        self._taker_buy_base_volume += candle.taker_buy_base_volume
        self._trade_count += candle.trade_count
        self._constituent_count += 1
        self._bucket_whole = self._bucket_whole and candle.is_closed

    def finalize_current_candle(self) -> Optional[Candle]:
        """Finalize and return the current higher-timeframe candle."""
        if self.current_bucket_ms is None or self._constituent_count == 0:
            return None

        assert self._open is not None
        assert self._high is not None
        assert self._low is not None
        assert self._close is not None

        htf_candle = Candle(
            symbol=self.symbol,
            timeframe=self.target_timeframe,
            open_time=self.current_bucket_ms,
            close_time=self.current_bucket_ms + self.tf_ms - 1,
            open=self._open,
            high=self._high,
            low=self._low,
            close=self._close,
            volume=self._volume,
            trade_count=self._trade_count,
            is_closed=self._bucket_whole,
            taker_buy_base_volume=self._taker_buy_base_volume,
        )

        self.current_bucket_ms = None
        self._open = None
        self._high = None
        self._low = None
        self._close = None
        self._volume = Decimal("0")
        self._taker_buy_base_volume = Decimal("0")
        self._trade_count = 0
        self._constituent_count = 0
        self._bucket_whole = False

        return htf_candle
