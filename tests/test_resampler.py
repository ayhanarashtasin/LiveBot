"""Unit tests for TimeframeResampler."""
from decimal import Decimal
import pytest

from live_engine.market_data.models import Candle
from live_engine.market_data.resampler import TimeframeResampler


def make_1m_candle(minute_offset: int, o: str, h: str, l: str, c: str, vol: str) -> Candle:
    base = 1786752000000  # aligned to 15m and 1h
    t_open = base + minute_offset * 60_000
    return Candle(
        symbol="BTCUSDT",
        timeframe="1m",
        open_time=t_open,
        close_time=t_open + 59_999,
        open=Decimal(o),
        high=Decimal(h),
        low=Decimal(l),
        close=Decimal(c),
        volume=Decimal(vol),
        trade_count=10,
        is_closed=True,
    )


def test_resample_1m_to_15m_immediate_emission_on_boundary():
    resampler = TimeframeResampler("BTCUSDT", "15m")

    # Add candles up to minute 10: all return None
    assert resampler.add_1m_candle(make_1m_candle(0, "63000.00", "63050.00", "62990.00", "63010.00", "1.0")) is None
    assert resampler.add_1m_candle(make_1m_candle(5, "63010.00", "63200.00", "63000.00", "63150.00", "2.0")) is None
    assert resampler.add_1m_candle(make_1m_candle(10, "63150.00", "63180.00", "62900.00", "62950.00", "3.0")) is None

    # Minute 14 is the final constituent of the 15m bar (closing at 899999).
    # It must emit the completed 15m candle IMMEDIATELY, without waiting for minute 15.
    c14 = make_1m_candle(14, "62950.00", "63080.00", "62940.00", "63045.00", "4.0")
    htf = resampler.add_1m_candle(c14)

    assert htf is not None
    assert htf.symbol == "BTCUSDT"
    assert htf.timeframe == "15m"
    assert htf.open_time == 1786752000000
    assert htf.close_time == 1786752000000 + 900_000 - 1
    assert htf.open == Decimal("63000.00")
    assert htf.high == Decimal("63200.00")
    assert htf.low == Decimal("62900.00")
    assert htf.close == Decimal("63045.00")
    assert htf.volume == Decimal("10.0")
    assert htf.trade_count == 40
    assert htf.is_closed is True

    # Minute 15 arrives -> starts new bucket, returns None
    c15 = make_1m_candle(15, "63045.00", "63100.00", "63040.00", "63090.00", "1.0")
    assert resampler.add_1m_candle(c15) is None


def test_resample_gap_boundary_emission():
    resampler = TimeframeResampler("BTCUSDT", "15m")
    resampler.add_1m_candle(make_1m_candle(0, "63000.00", "63050.00", "62990.00", "63010.00", "1.0"))
    resampler.add_1m_candle(make_1m_candle(10, "63010.00", "63100.00", "63000.00", "63050.00", "2.0"))

    # Minute 14 was missing due to gap. Minute 15 arrives in next bucket.
    # It must finalize the previous bucket and return it.
    c15 = make_1m_candle(15, "63050.00", "63100.00", "63040.00", "63090.00", "1.0")
    htf = resampler.add_1m_candle(c15)
    assert htf is not None
    assert htf.open_time == 1786752000000
    assert htf.close_time == 1786752000000 + 900_000 - 1
