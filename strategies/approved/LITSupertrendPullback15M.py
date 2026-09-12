"""Supertrend V2 TradingView - LITUSDT 15m (Rank 2: V2.TV.02)
Pullback Long-Only Strategy & Backtest Engine.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from freqtrade.strategy import IStrategy


# -----------------------------------------------------------------------------
# Core Indicators (Pine Script / TradingView Formulations)
# -----------------------------------------------------------------------------
def rma(series: np.ndarray, period: int) -> np.ndarray:
    """TradingView RMA (Running Moving Average / Wilder's MA)."""
    n = len(series)
    out = np.full(n, np.nan)
    if n < period:
        return out
    cur = float(np.mean(series[:period]))
    out[period - 1] = cur
    alpha = 1.0 / period
    for i in range(period, n):
        cur = cur * (1.0 - alpha) + series[i] * alpha
        out[i] = cur
    return out


def true_range(high: np.ndarray, low: np.ndarray, close: np.ndarray) -> np.ndarray:
    """True Range calculation."""
    n = len(close)
    tr = np.empty(n)
    tr[0] = high[0] - low[0]
    tr[1:] = np.maximum.reduce([
        high[1:] - low[1:],
        np.abs(high[1:] - close[:-1]),
        np.abs(low[1:] - close[:-1])
    ])
    return tr


def compute_atr(high: np.ndarray, low: np.ndarray, close: np.ndarray, period: int) -> np.ndarray:
    """ATR computed via RMA(TrueRange, period)."""
    return rma(true_range(high, low, close), period)


def compute_supertrend(
    high: np.ndarray, low: np.ndarray, close: np.ndarray, period: int, multiplier: float
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Standard Supertrend calculation matching TradingView bar rules.
    Returns: (st_line, st_dir [+1 for bull, -1 for bear], atr)
    """
    n = len(close)
    atr = compute_atr(high, low, close, period)
    hl2 = (high + low) * 0.5
    lower_band = hl2 - multiplier * atr
    upper_band = hl2 + multiplier * atr

    lower = np.full(n, np.nan)
    upper = np.full(n, np.nan)
    st_line = np.full(n, np.nan)
    direction = np.zeros(n, dtype=np.int8)

    if n < period:
        return st_line, direction, atr

    s = period - 1
    lower[s], upper[s] = lower_band[s], upper_band[s]
    direction[s] = 1 if close[s] > upper_band[s] else (-1 if close[s] < lower_band[s] else 1)
    st_line[s] = lower[s] if direction[s] == 1 else upper[s]

    for i in range(period, n):
        lower[i] = max(lower_band[i], lower[i - 1]) if close[i - 1] > lower[i - 1] else lower_band[i]
        upper[i] = min(upper_band[i], upper[i - 1]) if close[i - 1] < upper[i - 1] else upper_band[i]

        prev_dir = direction[i - 1]
        if prev_dir == -1 and close[i] > upper[i - 1]:
            direction[i] = 1
        elif prev_dir == 1 and close[i] < lower[i - 1]:
            direction[i] = -1
        else:
            direction[i] = prev_dir

        st_line[i] = lower[i] if direction[i] == 1 else upper[i]

    return st_line, direction, atr


class LITSupertrendPullback15M(IStrategy):
    INTERFACE_VERSION = 3
    timeframe = "15m"
    can_short = False
    stoploss = -0.99
    minimal_roi = {"0": 100.0}
    process_only_new_candles = True
    use_exit_signal = True
    exit_profit_only = False
    ignore_roi_if_entry_signal = False
    startup_candle_count = 250
    trailing_stop = False

    # Strategy parameters
    st_atr_length = 28
    st_multiplier = 2.0
    atr_period = 7
    pb_atr = 0.5
    pb_lb = 3
    sl_multiplier = 1.5
    tp_multiplier = 3.0
    max_hold = 128
    flip_exit = False

    def populate_indicators(self, dataframe: pd.DataFrame, metadata: dict) -> pd.DataFrame:
        high = dataframe["high"].to_numpy(dtype=np.float64)
        low = dataframe["low"].to_numpy(dtype=np.float64)
        close = dataframe["close"].to_numpy(dtype=np.float64)
        n = len(dataframe)

        st_line, st_dir, _ = compute_supertrend(
            high, low, close, period=self.st_atr_length, multiplier=self.st_multiplier
        )
        atr = compute_atr(high, low, close, period=self.atr_period)

        dataframe["supertrend"] = st_line
        dataframe["supertrend_direction"] = st_dir
        dataframe["atr"] = atr
        dataframe["atr_7"] = atr

        # Pullback proximity test: Close within pb_atr * ATR of ST line
        near_st = np.abs(close - st_line) <= (self.pb_atr * atr)

        # Trailing window of pb_lb bars
        near_rec = np.zeros(n, dtype=bool)
        for shift in range(self.pb_lb):
            if shift == 0:
                near_rec |= near_st
            else:
                shifted = np.concatenate((np.zeros(shift, dtype=bool), near_st[:-shift]))
                near_rec |= shifted

        # Bar breakout confirmation: Close > High[1]
        prev_h = np.concatenate(([high[0]], high[:-1]))
        brk_up = close > prev_h

        entry_candidate = (st_dir == 1) & near_rec & brk_up
        if "volume" in dataframe.columns:
            entry_candidate &= dataframe["volume"].to_numpy(dtype=np.float64) > 0

        dataframe["near_recent"] = near_rec
        dataframe["entry_candidate"] = entry_candidate
        return dataframe

    def populate_entry_trend(self, dataframe: pd.DataFrame, metadata: dict) -> pd.DataFrame:
        self._simulate_trades(dataframe)
        return dataframe

    def populate_exit_trend(self, dataframe: pd.DataFrame, metadata: dict) -> pd.DataFrame:
        if "exit_long" not in dataframe.columns:
            self._simulate_trades(dataframe)
        return dataframe

    def _simulate_trades(self, dataframe: pd.DataFrame) -> None:
        n = len(dataframe)
        if n == 0:
            dataframe["enter_long"] = 0
            dataframe["exit_long"] = 0
            dataframe["exit_reason"] = ""
            dataframe["exit_price"] = np.nan
            return

        o = dataframe["open"].to_numpy(dtype=np.float64)
        h = dataframe["high"].to_numpy(dtype=np.float64)
        l = dataframe["low"].to_numpy(dtype=np.float64)
        atr7 = dataframe["atr_7"].to_numpy(dtype=np.float64)
        entry_cand = dataframe["entry_candidate"].to_numpy(dtype=bool)

        enter_long = np.zeros(n, dtype=int)
        exit_long = np.zeros(n, dtype=int)
        exit_reasons = [""] * n
        exit_prices = np.full(n, np.nan)

        j = 0
        while j < n:
            if not entry_cand[j]:
                j += 1
                continue

            e = j + 1
            if e >= n:
                enter_long[j] = 1
                break

            ep = o[e]
            a0 = atr7[j]
            if not np.isfinite(a0) or a0 <= 0 or not np.isfinite(ep) or ep <= 0:
                j += 1
                continue

            enter_long[j] = 1
            sl = ep - self.sl_multiplier * a0
            tp = ep + self.tp_multiplier * a0
            limit = min(n - 1, e + self.max_hold)

            exit_bar = -1
            exit_px = np.nan
            reason = ""

            for i in range(e, limit + 1):
                if i > e and i >= e + self.max_hold:
                    exit_bar, exit_px, reason = i, o[i], "TIME"
                    break

                hit_tp = h[i] >= tp
                hit_sl = l[i] <= sl

                # Pessimistic convention: if both touched in the same bar, assume Stop Loss hit first
                if hit_tp and hit_sl:
                    exit_bar, exit_px, reason = i, sl, "SL"
                    break
                elif hit_tp:
                    exit_bar, exit_px, reason = i, tp, "TP"
                    break
                elif hit_sl:
                    exit_bar, exit_px, reason = i, sl, "SL"
                    break

            if exit_bar != -1:
                exit_long[exit_bar] = 1
                exit_reasons[exit_bar] = reason
                exit_prices[exit_bar] = exit_px
                j = exit_bar
            else:
                break

        dataframe["enter_long"] = enter_long
        dataframe["exit_long"] = exit_long
        dataframe["exit_reason"] = exit_reasons
        dataframe["exit_price"] = exit_prices
