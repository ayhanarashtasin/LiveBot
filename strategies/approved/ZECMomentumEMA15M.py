from datetime import datetime
import pandas as pd
import numpy as np
from freqtrade.strategy import IStrategy


class ZECMomentumEMA15M(IStrategy):
    """
    Momentum M03 - EMA Cross (10, 100) on 15m OHLCV data for ZECUSDT.
    
    Fast EMA (span=10) and Slow EMA (span=100) trend crossover model with
    bar close price confirmation filter (close > EMA(10) for Long).
    Exit on opposing crossover (EMA(10) crossing below EMA(100)).
    """
    INTERFACE_VERSION = 3
    timeframe = '15m'
    can_short = False
    stoploss = -0.99
    minimal_roi = {'0': 100.0}
    process_only_new_candles = True
    use_exit_signal = True
    exit_profit_only = False
    ignore_roi_if_entry_signal = False
    startup_candle_count = 250
    trailing_stop = False

    # Strategy parameters
    fast_period = 10
    slow_period = 100

    def populate_indicators(self, dataframe: pd.DataFrame, metadata: dict) -> pd.DataFrame:
        dataframe['ema_fast'] = dataframe['close'].ewm(span=self.fast_period, adjust=False).mean()
        dataframe['ema_slow'] = dataframe['close'].ewm(span=self.slow_period, adjust=False).mean()
        return dataframe

    def populate_entry_trend(self, dataframe: pd.DataFrame, metadata: dict) -> pd.DataFrame:
        prev_fast = dataframe['ema_fast'].shift(1)
        prev_slow = dataframe['ema_slow'].shift(1)
        bull_cross = (dataframe['ema_fast'] > dataframe['ema_slow']) & (prev_fast <= prev_slow)
        
        conditions = [
            bull_cross,
            (dataframe['close'] > dataframe['ema_fast']),
        ]
        if 'volume' in dataframe.columns:
            conditions.append(dataframe['volume'] > 0)

        from functools import reduce
        dataframe.loc[reduce(lambda x, y: x & y, conditions), 'enter_long'] = 1
        return dataframe

    def populate_exit_trend(self, dataframe: pd.DataFrame, metadata: dict) -> pd.DataFrame:
        prev_fast = dataframe['ema_fast'].shift(1)
        prev_slow = dataframe['ema_slow'].shift(1)
        bear_cross = (dataframe['ema_fast'] < dataframe['ema_slow']) & (prev_fast >= prev_slow)

        conditions = [
            bear_cross,
        ]
        if 'volume' in dataframe.columns:
            conditions.append(dataframe['volume'] > 0)

        from functools import reduce
        dataframe.loc[reduce(lambda x, y: x & y, conditions), 'exit_long'] = 1
        return dataframe
