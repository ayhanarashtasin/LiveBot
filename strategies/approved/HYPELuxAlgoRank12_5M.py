"""Frozen HYPEUSDT LuxAlgo Rank 12 long-only entry model."""
from __future__ import annotations

import numpy as np
import pandas as pd
from freqtrade.strategy import IStrategy


def _rma(values: np.ndarray, period: int) -> np.ndarray:
    out = np.full(len(values), np.nan, dtype=np.float64)
    if len(values) < period:
        return out
    current = float(np.mean(values[:period]))
    out[period - 1] = current
    alpha = 1.0 / period
    for i in range(period, len(values)):
        current = current * (1.0 - alpha) + values[i] * alpha
        out[i] = current
    return out


def _rolling_mean(values: np.ndarray, period: int) -> np.ndarray:
    sums = np.concatenate(([0.0], np.cumsum(np.nan_to_num(values))))
    out = np.full(len(values), np.nan)
    out[period - 1:] = (sums[period:] - sums[:-period]) / period
    return out


def _true_range(high: np.ndarray, low: np.ndarray, close: np.ndarray) -> np.ndarray:
    out = np.empty(len(close), dtype=np.float64)
    if not len(close):
        return out
    out[0] = high[0] - low[0]
    out[1:] = np.maximum.reduce((
        high[1:] - low[1:],
        np.abs(high[1:] - close[:-1]),
        np.abs(low[1:] - close[:-1]),
    ))
    return out


def _atr(high: np.ndarray, low: np.ndarray, close: np.ndarray, period: int) -> np.ndarray:
    return _rma(_true_range(high, low, close), period)


def _supertrend(
    high: np.ndarray, low: np.ndarray, close: np.ndarray, period: int, multiplier: float
) -> tuple[np.ndarray, np.ndarray]:
    n = len(close)
    atr = _atr(high, low, close, period)
    middle = (high + low) * 0.5
    basic_lower, basic_upper = middle - multiplier * atr, middle + multiplier * atr
    lower, upper = np.full(n, np.nan), np.full(n, np.nan)
    line, direction = np.full(n, np.nan), np.zeros(n, dtype=np.int8)
    if n < period:
        return line, direction

    start = period - 1
    lower[start], upper[start] = basic_lower[start], basic_upper[start]
    direction[start] = 1 if close[start] > basic_upper[start] else (-1 if close[start] < basic_lower[start] else 1)
    line[start] = lower[start] if direction[start] == 1 else upper[start]
    for i in range(period, n):
        lower[i] = max(basic_lower[i], lower[i - 1]) if close[i - 1] > lower[i - 1] else basic_lower[i]
        upper[i] = min(basic_upper[i], upper[i - 1]) if close[i - 1] < upper[i - 1] else basic_upper[i]
        if direction[i - 1] == -1 and close[i] > upper[i - 1]:
            direction[i] = 1
        elif direction[i - 1] == 1 and close[i] < lower[i - 1]:
            direction[i] = -1
        else:
            direction[i] = direction[i - 1]
        line[i] = lower[i] if direction[i] == 1 else upper[i]
    return line, direction


def _adx(
    high: np.ndarray, low: np.ndarray, close: np.ndarray, period: int
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    n = len(close)
    plus_dm, minus_dm = np.zeros(n), np.zeros(n)
    up, down = high[1:] - high[:-1], low[:-1] - low[1:]
    plus_dm[1:] = np.where((up > down) & (up > 0), up, 0.0)
    minus_dm[1:] = np.where((down > up) & (down > 0), down, 0.0)
    average_range = _rma(_true_range(high, low, close), period)
    plus_di = 100.0 * _rma(plus_dm, period) / np.maximum(average_range, 1e-12)
    minus_di = 100.0 * _rma(minus_dm, period) / np.maximum(average_range, 1e-12)
    dx = 100.0 * np.abs(plus_di - minus_di) / np.maximum(plus_di + minus_di, 1e-12)
    adx = np.full(n, np.nan)
    offset = period - 1
    if n > offset:
        adx[offset:] = _rma(dx[offset:], period)
    return adx, plus_di, minus_di


def _rsi(close: np.ndarray, period: int) -> np.ndarray:
    delta = np.diff(close)
    gain = np.insert(np.where(delta > 0, delta, 0.0), 0, 0.0)
    loss = np.insert(np.where(delta < 0, -delta, 0.0), 0, 0.0)
    relative_strength = _rma(gain, period) / np.maximum(_rma(loss, period), 1e-12)
    return 100.0 - 100.0 / (1.0 + relative_strength)


class HYPELuxAlgoRank12_5M(IStrategy):
    INTERFACE_VERSION = 3
    timeframe = "5m"
    can_short = False
    stoploss = -0.99
    minimal_roi = {"0": 100.0}
    process_only_new_candles = True
    use_exit_signal = False
    startup_candle_count = 1000
    trailing_stop = False

    atr_period = 14
    sl_multiplier = 6.0
    tp_multiplier = 4.5
    max_hold = 384
    slots = 12
    leverage = 3

    def populate_indicators(self, dataframe: pd.DataFrame, metadata: dict) -> pd.DataFrame:
        high = dataframe["high"].to_numpy(dtype=np.float64)
        low = dataframe["low"].to_numpy(dtype=np.float64)
        close = dataframe["close"].to_numpy(dtype=np.float64)
        volume = dataframe["volume"].to_numpy(dtype=np.float64)

        st10_2_line, st10_2 = _supertrend(high, low, close, 10, 2.0)
        _, st10_3 = _supertrend(high, low, close, 10, 3.0)
        _, st20_4 = _supertrend(high, low, close, 20, 4.0)
        st20_4_mature = (st20_4 == 1) & (_rolling_mean((st20_4 == 1).astype(float), 10) > 0.9)

        atr14 = _atr(high, low, close, self.atr_period)
        atr_rank = pd.Series(atr14 / np.maximum(close, 1e-9)).rolling(500).rank(pct=True).to_numpy()
        atr_pct_low = np.nan_to_num((atr_rank >= 0.0) & (atr_rank < 0.25)).astype(bool)

        adx14, _, _ = _adx(high, low, close, 14)
        _, plus48, minus48 = _adx(high, low, close, 48)
        rsi2 = _rsi(close, 2)
        fast = pd.Series(close).ewm(span=12, adjust=False).mean().to_numpy(dtype=np.float64)
        slow = pd.Series(close).ewm(span=26, adjust=False).mean().to_numpy(dtype=np.float64)
        macd = fast - slow
        macd_signal = pd.Series(macd).ewm(span=9, adjust=False).mean().to_numpy(dtype=np.float64)
        histogram = macd - macd_signal
        histogram_rising = np.zeros(len(close), dtype=bool)
        histogram_rising[1:] = histogram[1:] > histogram[:-1]

        taker_buy = dataframe.get("taker_buy_base_volume", pd.Series(0.0, index=dataframe.index)).to_numpy(dtype=np.float64)
        taker_buy_ratio = taker_buy / np.maximum(volume, 1e-12)
        taker_buy_ma_gt_50 = np.nan_to_num(_rolling_mean(taker_buy_ratio, 20) > 0.50).astype(bool)

        adx_gt_15 = np.nan_to_num(adx14 > 15).astype(bool)
        di48_bull = np.nan_to_num(plus48 > minus48).astype(bool)
        st10_2_bull, st10_3_bull = st10_2 == 1, st10_3 == 1
        rsi2_lt_60 = np.nan_to_num(rsi2 < 60).astype(bool)

        group_1 = ~adx_gt_15 & di48_bull & ~st10_2_bull & ~st10_3_bull & ~taker_buy_ma_gt_50
        group_2 = ~adx_gt_15 & ~atr_pct_low & ~rsi2_lt_60 & ~st10_2_bull & ~st20_4_mature
        group_3 = ~adx_gt_15 & ~atr_pct_low & histogram_rising & ~st10_2_bull & ~st20_4_mature

        dataframe["atr"] = atr14
        dataframe["supertrend"] = st10_2_line
        dataframe["supertrend_direction"] = st10_2
        dataframe["adx"] = adx14
        dataframe["di48_bull"] = di48_bull
        dataframe["atrpct_lo"] = atr_pct_low
        dataframe["rsi2"] = rsi2
        dataframe["macd_hist_rising"] = histogram_rising
        dataframe["takerbuy_ma_gt_50"] = taker_buy_ma_gt_50
        dataframe["entry_group_1"] = group_1
        dataframe["entry_group_2"] = group_2
        dataframe["entry_group_3"] = group_3
        dataframe["entry_candidate"] = group_1 | group_2 | group_3
        return dataframe

    def populate_entry_trend(self, dataframe: pd.DataFrame, metadata: dict) -> pd.DataFrame:
        dataframe["enter_long"] = dataframe["entry_candidate"].astype(np.int8)
        return dataframe

    def populate_exit_trend(self, dataframe: pd.DataFrame, metadata: dict) -> pd.DataFrame:
        # The runtime slot book owns per-entry first-touch and time-stop exits.
        dataframe["exit_long"] = 0
        dataframe["exit_reason"] = ""
        dataframe["exit_price"] = np.nan
        return dataframe
