"""A bar the engine only partially witnessed must never reach the strategy.

Joining the stream mid-bucket yields a bar with a wrong open/high/low/volume. Feeding one into
strategy history corrupts every indicator derived from it, so it has to be flagged as not closed.
"""
from decimal import Decimal

from live_engine.market_data.models import Candle
from live_engine.market_data.resampler import TimeframeResampler

BASE = 1786752000000  # aligned to 15m


def one_minute(minute_offset: int, closed: bool = True) -> Candle:
    t = BASE + minute_offset * 60_000
    return Candle(
        symbol="BTCUSDT", timeframe="1m", open_time=t, close_time=t + 59_999,
        open=Decimal("100"), high=Decimal("101"), low=Decimal("99"), close=Decimal("100"),
        volume=Decimal("1"), trade_count=5, is_closed=closed,
    )


def test_resampler_flags_bucket_whose_leading_minute_was_partial():
    """The orchestrator marks the first 1m candle after connecting as not closed.

    Connecting part-way through the bucket's own first minute still fills all 15 slots, so the
    count alone would not catch it — the partial flag has to propagate.
    """
    r = TimeframeResampler("BTCUSDT", "15m")
    out = None
    for m in range(15):
        out = r.add_1m_candle(one_minute(m, closed=(m != 0))) or out
    assert out is not None and out.is_closed is False


def test_resampler_flags_bucket_joined_late():
    """Missing the leading minute means a wrong open — the bar must not be marked closed."""
    r = TimeframeResampler("BTCUSDT", "15m")
    out = None
    for m in range(4, 15):  # first 4 minutes of the bucket never arrived
        out = r.add_1m_candle(one_minute(m)) or out
    assert out is not None and out.is_closed is False


def test_resampler_flags_bucket_containing_a_partial_minute():
    r = TimeframeResampler("BTCUSDT", "15m")
    out = None
    for m in range(15):
        out = r.add_1m_candle(one_minute(m, closed=(m != 7))) or out
    assert out is not None and out.is_closed is False


def test_resampler_marks_whole_bucket_closed():
    r = TimeframeResampler("BTCUSDT", "15m")
    out = None
    for m in range(15):
        out = r.add_1m_candle(one_minute(m)) or out
    assert out is not None and out.is_closed is True
    assert out.open_time == BASE and out.volume == Decimal("15")


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_"):
            fn()
            print(f"ok  {name}")
    print("all partial-bar integrity checks passed")
