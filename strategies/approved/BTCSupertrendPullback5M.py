"""ST_09 Supertrend pullback re-entry strategy for BTCUSDT 5m."""
from __future__ import annotations

import numpy as np
import pandas as pd
from freqtrade.strategy import IStrategy


class BTCSupertrendPullback5M(IStrategy):
    INTERFACE_VERSION = 3
    timeframe = "5m"
    can_short = False
    stoploss = -0.99
    minimal_roi = {"0": 100.0}
    process_only_new_candles = True
    use_exit_signal = True
    exit_profit_only = False
    startup_candle_count = 250
    trailing_stop = False

    atr_period = st_atr_length = 10
    st_multiplier = 3.0
    pb_atr = 0.5
    sl_multiplier = 1.5
    tp_multiplier = 3.0

    def populate_indicators(self, dataframe: pd.DataFrame, metadata: dict) -> pd.DataFrame:
        high = dataframe["high"].to_numpy(dtype=np.float64)
        low = dataframe["low"].to_numpy(dtype=np.float64)
        close = dataframe["close"].to_numpy(dtype=np.float64)
        opened = dataframe["open"].to_numpy(dtype=np.float64)
        n = len(dataframe)

        atr = np.full(n, np.nan)
        supertrend = np.full(n, np.nan)
        direction = np.zeros(n, dtype=np.int8)
        if n >= self.atr_period:
            tr = np.empty(n)
            tr[0] = high[0] - low[0]
            tr[1:] = np.maximum.reduce([
                high[1:] - low[1:],
                np.abs(high[1:] - close[:-1]),
                np.abs(low[1:] - close[:-1]),
            ])
            atr[self.atr_period - 1] = np.mean(tr[:self.atr_period])
            alpha = 1.0 / self.atr_period
            for i in range(self.atr_period, n):
                atr[i] = atr[i - 1] * (1.0 - alpha) + tr[i] * alpha

            hl2 = (high + low) * 0.5
            lower_basic = hl2 - self.st_multiplier * atr
            upper_basic = hl2 + self.st_multiplier * atr
            lower = np.full(n, np.nan)
            upper = np.full(n, np.nan)
            start = self.atr_period - 1
            lower[start], upper[start] = lower_basic[start], upper_basic[start]
            direction[start] = 1 if close[start] > upper_basic[start] else -1
            supertrend[start] = lower[start] if direction[start] == 1 else upper[start]

            for i in range(self.atr_period, n):
                lower[i] = max(lower_basic[i], lower[i - 1]) if close[i - 1] > lower[i - 1] else lower_basic[i]
                upper[i] = min(upper_basic[i], upper[i - 1]) if close[i - 1] < upper[i - 1] else upper_basic[i]
                if direction[i - 1] == -1 and close[i] > upper[i - 1]:
                    direction[i] = 1
                elif direction[i - 1] == 1 and close[i] < lower[i - 1]:
                    direction[i] = -1
                else:
                    direction[i] = direction[i - 1]
                supertrend[i] = lower[i] if direction[i] == 1 else upper[i]

        distance = (close - supertrend) / np.maximum(atr, 1e-8)
        pullback = (direction == 1) & (distance < self.pb_atr) & (close > supertrend) & (close > opened)
        entry_candidate = np.zeros(n, dtype=bool)
        if n > 1:
            entry_candidate[1:] = pullback[1:] & ~pullback[:-1]

        dataframe["atr"] = atr
        dataframe["supertrend"] = supertrend
        dataframe["supertrend_direction"] = direction
        dataframe["pullback_distance"] = distance
        dataframe["entry_candidate"] = entry_candidate
        return dataframe

    def populate_entry_trend(self, dataframe: pd.DataFrame, metadata: dict) -> pd.DataFrame:
        self._simulate_trades(dataframe)
        return dataframe

    def populate_exit_trend(self, dataframe: pd.DataFrame, metadata: dict) -> pd.DataFrame:
        if "exit_long" not in dataframe:
            self._simulate_trades(dataframe)
        return dataframe

    def _simulate_trades(self, dataframe: pd.DataFrame) -> None:
        n = len(dataframe)
        high = dataframe["high"].to_numpy(dtype=np.float64)
        low = dataframe["low"].to_numpy(dtype=np.float64)
        close = dataframe["close"].to_numpy(dtype=np.float64)
        atr = dataframe["atr"].to_numpy(dtype=np.float64)
        direction = dataframe["supertrend_direction"].to_numpy(dtype=np.int8)
        candidates = dataframe["entry_candidate"].to_numpy(dtype=bool)
        enter_long = np.zeros(n, dtype=np.int8)
        exit_long = np.zeros(n, dtype=np.int8)
        exit_price = np.full(n, np.nan)
        exit_reason = [""] * n
        in_position = False
        stop = target = 0.0

        for i in range(self.atr_period, n):
            if not in_position and candidates[i] and np.isfinite(atr[i]) and atr[i] > 0:
                in_position = True
                enter_long[i] = 1
                stop = close[i] - self.sl_multiplier * atr[i]
                target = close[i] + self.tp_multiplier * atr[i]
            elif in_position:
                if low[i] <= stop:
                    reason, price = "SL", stop
                elif high[i] >= target:
                    reason, price = "TP", target
                elif direction[i] == -1:
                    reason, price = "TrendFlip", close[i]
                else:
                    continue
                exit_long[i] = 1
                exit_reason[i] = reason
                exit_price[i] = price
                in_position = False

        dataframe["enter_long"] = enter_long
        dataframe["exit_long"] = exit_long
        dataframe["exit_reason"] = exit_reason
        dataframe["exit_price"] = exit_price
