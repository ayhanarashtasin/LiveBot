"""Deterministic canonical 1-minute candle builder from Binance aggTrades."""
from __future__ import annotations

from decimal import Decimal
from typing import Dict, Iterator, List, Optional, Set, Tuple

from live_engine.market_data.models import AggTrade, Candle

CANDLE_1M_MS = 60_000


class MinuteCandleBuilder:
    """Reconstructs exact canonical 1-minute OHLCV candles from aggregate trade stream.

    Rules strictly mirror historical benchmark candle formation:
    - UTC minute boundaries: [T_start, T_start + 60,000) ms
    - open: first chronological trade price
    - high: maximum trade price
    - low: minimum trade price
    - close: final chronological trade price
    - volume: sum of trade base quantities
    - trade_count: number of accepted trades
    """

    def __init__(self, symbol: str):
        self.symbol = symbol.upper()
        self.current_bucket_ms: Optional[int] = None

        self._open: Optional[Decimal] = None
        self._high: Optional[Decimal] = None
        self._low: Optional[Decimal] = None
        self._close: Optional[Decimal] = None
        self._volume: Decimal = Decimal("0")
        self._taker_buy_base_volume: Decimal = Decimal("0")
        self._trade_count: int = 0

        self._first_trade_key: Optional[Tuple[int, int]] = None
        self._last_trade_key: Optional[Tuple[int, int]] = None
        self._seen_trade_ids: Set[int] = set()

    @staticmethod
    def get_bucket_open_time(timestamp_ms: int) -> int:
        """Returns the floor UTC minute open time in milliseconds."""
        return (timestamp_ms // CANDLE_1M_MS) * CANDLE_1M_MS

    def add_trade(self, trade: AggTrade) -> Optional[Candle]:
        """Process incoming aggTrade.

        If the trade crosses into a new 1m bucket, the previous completed candle is finalized and returned.
        Returns None if trade is accumulated into the current candle or ignored as duplicate.
        """
        # Deduplication
        if trade.agg_trade_id in self._seen_trade_ids:
            return None
        self._seen_trade_ids.add(trade.agg_trade_id)

        # Keep set bounded
        if len(self._seen_trade_ids) > 50_000:
            self._seen_trade_ids.clear()
            self._seen_trade_ids.add(trade.agg_trade_id)

        trade_bucket = self.get_bucket_open_time(trade.trade_time)
        trade_key = (trade.trade_time, trade.agg_trade_id)

        completed_candle: Optional[Candle] = None

        # First trade ever
        if self.current_bucket_ms is None:
            self.current_bucket_ms = trade_bucket
            self._init_bucket(trade, trade_key)
            return None

        # Trade belongs to a future bucket -> finalize and emit previous
        if trade_bucket > self.current_bucket_ms:
            completed_candle = self.finalize_current_candle()
            self.current_bucket_ms = trade_bucket
            self._init_bucket(trade, trade_key)
            return completed_candle

        # Trade belongs to current bucket
        if trade_bucket == self.current_bucket_ms:
            self._update_bucket(trade, trade_key)
            return None

        # Late trade belonging to an already-closed past bucket
        # According to standard closed-candle integrity, late arrivals for closed bars cannot mutate closed bars.
        return None

    def _init_bucket(self, trade: AggTrade, trade_key: Tuple[int, int]) -> None:
        self._open = trade.price
        self._high = trade.price
        self._low = trade.price
        self._close = trade.price
        self._volume = trade.quantity
        self._taker_buy_base_volume = trade.quantity if not trade.buyer_is_market_maker else Decimal("0")
        self._trade_count = 1
        self._first_trade_key = trade_key
        self._last_trade_key = trade_key

    def _update_bucket(self, trade: AggTrade, trade_key: Tuple[int, int]) -> None:
        assert self._high is not None
        assert self._low is not None

        if trade.price > self._high:
            self._high = trade.price
        if trade.price < self._low:
            self._low = trade.price

        self._volume += trade.quantity
        if not trade.buyer_is_market_maker:
            self._taker_buy_base_volume += trade.quantity
        self._trade_count += 1

        # Check if this trade is chronologically earlier than current first
        if self._first_trade_key is None or trade_key < self._first_trade_key:
            self._first_trade_key = trade_key
            self._open = trade.price

        # Check if this trade is chronologically later than current last
        if self._last_trade_key is None or trade_key >= self._last_trade_key:
            self._last_trade_key = trade_key
            self._close = trade.price

    def finalize_current_candle(self) -> Optional[Candle]:
        """Finalize and return the current 1m candle, resetting internal state."""
        if self.current_bucket_ms is None or self._trade_count == 0:
            return None

        assert self._open is not None
        assert self._high is not None
        assert self._low is not None
        assert self._close is not None

        candle = Candle(
            symbol=self.symbol,
            timeframe="1m",
            open_time=self.current_bucket_ms,
            close_time=self.current_bucket_ms + CANDLE_1M_MS - 1,
            open=self._open,
            high=self._high,
            low=self._low,
            close=self._close,
            volume=self._volume,
            trade_count=self._trade_count,
            is_closed=True,
            taker_buy_base_volume=self._taker_buy_base_volume,
        )

        # Clear state
        self.current_bucket_ms = None
        self._open = None
        self._high = None
        self._low = None
        self._close = None
        self._volume = Decimal("0")
        self._taker_buy_base_volume = Decimal("0")
        self._trade_count = 0
        self._first_trade_key = None
        self._last_trade_key = None

        return candle

    def flush(self) -> Optional[Candle]:
        """Alias for finalize_current_candle."""
        return self.finalize_current_candle()
