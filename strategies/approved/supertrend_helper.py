import numpy as np
import pandas as pd
import talib

def supertrend(dataframe: pd.DataFrame, length: int = 10, multiplier: float = 3.0) -> tuple[pd.Series, pd.Series]:
    """
    Canonical Supertrend Implementation (TradingView / Olivier Seban formulation)
    Returns:
        (trend_values, direction_series)
        direction_series: 1 for Bullish (Green), -1 for Bearish (Red)
    """
    high = dataframe['high'].values
    low = dataframe['low'].values
    close = dataframe['close'].values
    n = len(dataframe)
    
    if n == 0:
        return pd.Series(dtype=np.float64), pd.Series(dtype=np.int32)
        
    atr = talib.ATR(high, low, close, timeperiod=length)
    hl2 = (high + low) / 2.0
    matr = multiplier * atr
    upperband = hl2 + matr
    lowerband = hl2 - matr
    
    dir_ = np.ones(n, dtype=np.int32)
    trend = np.zeros(n, dtype=np.float64)
    
    ub = np.copy(upperband)
    lb = np.copy(lowerband)
    
    for i in range(1, n):
        if np.isnan(atr[i]):
            continue
        c = close[i]
        prev_ub = ub[i-1]
        prev_lb = lb[i-1]
        
        if c > prev_ub:
            dir_[i] = 1
        elif c < prev_lb:
            dir_[i] = -1
        else:
            dir_[i] = dir_[i-1]
            if dir_[i] > 0 and lb[i] < prev_lb:
                lb[i] = prev_lb
            if dir_[i] < 0 and ub[i] > prev_ub:
                ub[i] = prev_ub
                
        if dir_[i] > 0:
            trend[i] = lb[i]
        else:
            trend[i] = ub[i]
            
    return pd.Series(trend, index=dataframe.index), pd.Series(dir_, index=dataframe.index)
