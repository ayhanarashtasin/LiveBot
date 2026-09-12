"""Tests for HistoricalAggTradeAdapter streaming and candle reconstruction."""
from pathlib import Path
from decimal import Decimal
import pytest

from live_engine.market_data.aggtrade_adapter import HistoricalAggTradeAdapter
from live_engine.market_data.models import AggTrade, Candle

PARQUET_FILE = Path("BTCUSDT_USDM_DATA/aggTrades/2026-08.parquet")


@pytest.mark.skipif(not PARQUET_FILE.exists(), reason="2026-08 aggTrades parquet not available")
def test_aggtrade_adapter_streaming():
    """Verify that HistoricalAggTradeAdapter streams valid typed AggTrade objects."""
    adapter = HistoricalAggTradeAdapter(symbol="BTCUSDT")

    # Read first 500 trades
    trades = []
    for trade in adapter.stream_trades(PARQUET_FILE, batch_size=500):
        trades.append(trade)
        if len(trades) >= 500:
            break

    assert len(trades) == 500
    first = trades[0]
    assert isinstance(first, AggTrade)
    assert first.symbol == "BTCUSDT"
    assert first.price > Decimal("0")
    assert first.quantity > Decimal("0")
    assert first.trade_time > 0
    assert isinstance(first.buyer_is_market_maker, bool)


@pytest.mark.skipif(not PARQUET_FILE.exists(), reason="2026-08 aggTrades parquet not available")
def test_aggtrade_adapter_candle_reconstruction():
    """Verify that streaming aggTrades through adapter constructs canonical candles."""
    adapter = HistoricalAggTradeAdapter(symbol="BTCUSDT")

    # Read the first batch of trades and construct 1m & 15m candles
    # Aug 1, 2026 00:00:00 UTC = 1785542400000 ms
    # Read first 50,000 trades
    completed_1m = []
    candles_15m = []
    trade_count = 0
    from live_engine.market_data.candle_builder import MinuteCandleBuilder
    from live_engine.market_data.resampler import TimeframeResampler

    builder = MinuteCandleBuilder("BTCUSDT")
    resampler = TimeframeResampler("BTCUSDT", "15m")

    for trade in adapter.stream_trades(PARQUET_FILE, batch_size=10000):
        trade_count += 1
        c1m = builder.add_trade(trade)
        if c1m:
            completed_1m.append(c1m)
            c15m = resampler.add_1m_candle(c1m)
            if c15m:
                candles_15m.append(c15m)
        if trade_count >= 30000:
            break

    assert trade_count == 30000
    assert len(completed_1m) > 0
    for c in completed_1m:
        assert c.open_time % 60000 == 0
        assert c.close_time == c.open_time + 59999
        assert c.high >= c.low
        assert c.volume > Decimal("0")

