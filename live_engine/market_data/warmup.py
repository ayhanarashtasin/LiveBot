"""Historical warm-up data loader and buffer management."""
from __future__ import annotations

import os
import time
from decimal import Decimal
from typing import List, Optional

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from live_engine.market_data.models import Candle
from live_engine.market_data.resampler import TIMEFRAME_MAP_MS


def fetch_closed_klines(
    symbol: str,
    timeframe: str,
    start_ms: int,
    end_ms: Optional[int] = None,
    base_url: str = "https://fapi.binance.com",
) -> List[Candle]:
    """Fetch fully-closed exchange klines in [start_ms, end_ms), oldest first.

    Bars still forming are never returned, so the result is always safe to treat as history.
    """
    import requests

    tf_ms = TIMEFRAME_MAP_MS[timeframe]
    now_ms = int(time.time() * 1000)
    stop = min(end_ms, now_ms) if end_ms is not None else now_ms
    out: List[Candle] = []
    cursor = start_ms

    while cursor + tf_ms <= stop:
        # This link drops connections regularly in practice; a single timeout must not be
        # allowed to leave a hole in the history we are fetching precisely to remove holes.
        for attempt in range(4):
            try:
                resp = requests.get(
                    f"{base_url}/fapi/v1/klines",
                    params={"symbol": symbol.upper(), "interval": timeframe,
                            "startTime": str(cursor), "limit": 1500},
                    timeout=20,
                )
                if resp.status_code == 451 and "testnet" not in base_url:
                    base_url = "https://testnet.binancefuture.com"
                    continue
                resp.raise_for_status()
                rows = resp.json()
                break
            except Exception:
                if attempt == 3:
                    raise
                time.sleep(1.5 * (attempt + 1))
        if not rows:
            break
        for r in rows:
            open_time, close_time = int(r[0]), int(r[6])
            if close_time >= stop or open_time < start_ms:
                continue
            out.append(Candle(
                symbol=symbol.upper(), timeframe=timeframe,
                open_time=open_time, close_time=close_time,
                open=Decimal(r[1]), high=Decimal(r[2]), low=Decimal(r[3]),
                close=Decimal(r[4]), volume=Decimal(r[5]),
                trade_count=int(r[8]), is_closed=True,
                taker_buy_base_volume=Decimal(r[9]),
                provenance="rest_kline",
            ))
        nxt = int(rows[-1][0]) + tf_ms
        if nxt <= cursor:
            break
        cursor = nxt

    return out


class WarmupManager:
    """Loads and validates historical candle buffers required to prime strategy indicators."""

    def __init__(self, symbol: str, timeframe: str, required_candles: int = 250):
        self.symbol = symbol.upper()
        self.timeframe = timeframe.lower()
        self.required_candles = required_candles
        self.candle_buffer: List[Candle] = []
        self.is_ready: bool = False

    def load_from_parquet(
        self, parquet_path: str, end_timestamp_ms: Optional[int] = None
    ) -> int:
        """Load candles from a Parquet file up to an optional end timestamp."""
        if not os.path.exists(parquet_path):
            raise FileNotFoundError(f"Warm-up file not found: {parquet_path}")

        df = pd.read_parquet(parquet_path)
        if end_timestamp_ms is not None:
            df = df[df["open_time"] < end_timestamp_ms]

        # Take the most recent required_candles
        if len(df) > self.required_candles:
            df = df.iloc[-self.required_candles :]

        self.candle_buffer.clear()
        for _, row in df.iterrows():
            c = Candle(
                symbol=self.symbol,
                timeframe=self.timeframe,
                open_time=int(row["open_time"]),
                close_time=int(row.get("close_time", int(row["open_time"]) + 60_000 - 1)),
                open=Decimal(str(row["open"])),
                high=Decimal(str(row["high"])),
                low=Decimal(str(row["low"])),
                close=Decimal(str(row["close"])),
                volume=Decimal(str(row["volume"])),
                trade_count=int(row.get("trade_count", 0)),
                is_closed=True,
                taker_buy_base_volume=Decimal(str(row.get("taker_buy_base_volume", 0))),
            )
            self.candle_buffer.append(c)

        if len(self.candle_buffer) >= self.required_candles:
            self.is_ready = True

        return len(self.candle_buffer)

    def backfill_gap_from_rest(self, base_url: str = "https://fapi.binance.com") -> int:
        """Append closed exchange klines covering the gap between the Parquet tail and now.

        The Parquet dataset is a static file, so by the time the engine starts it is usually
        hours or days behind. Appending live candles straight onto it hands the strategy a
        history with a hole in it, and every indicator derived from that history is wrong.
        Returns the number of bars appended. Raises if the result is not contiguous.
        """
        if not self.candle_buffer:
            raise RuntimeError("Cannot backfill an empty warmup buffer; load Parquet first.")

        tf_ms = TIMEFRAME_MAP_MS[self.timeframe]
        appended = fetch_closed_klines(
            self.symbol, self.timeframe,
            start_ms=self.candle_buffer[-1].open_time + tf_ms,
            base_url=base_url,
        )
        merged = self.candle_buffer + appended
        gaps = [
            (merged[i - 1].open_time, merged[i].open_time)
            for i in range(1, len(merged))
            if merged[i].open_time - merged[i - 1].open_time != tf_ms
        ]
        if gaps:
            raise RuntimeError(
                f"WARMUP VALIDATION FAILED: warmup history is not contiguous after backfill. "
                f"{len(gaps)} gap(s), first at open_time {gaps[0][0]} -> {gaps[0][1]}."
            )

        self.candle_buffer = merged[-self.required_candles:]
        self.is_ready = len(self.candle_buffer) >= self.required_candles
        return len(appended)

    def to_dataframe(self) -> pd.DataFrame:
        """Convert warmed-up buffer to DataFrame matching strategy input format."""
        if not self.candle_buffer:
            return pd.DataFrame()

        records = [c.to_dict() for c in self.candle_buffer]
        df = pd.DataFrame(records)
        df["date"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
        return df
