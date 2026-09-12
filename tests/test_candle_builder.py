"""Unit tests for canonical 1-minute candle builder."""
from decimal import Decimal
import pytest

from live_engine.market_data.candle_builder import MinuteCandleBuilder
from live_engine.market_data.models import AggTrade


def make_trade(agg_id: int, price: str, qty: str, timestamp_ms: int) -> AggTrade:
    return AggTrade(
        event_type="aggTrade",
        event_time=timestamp_ms,
        symbol="BTCUSDT",
        agg_trade_id=agg_id,
        price=Decimal(price),
        quantity=Decimal(qty),
        first_trade_id=agg_id * 10,
        last_trade_id=agg_id * 10 + 1,
        trade_time=timestamp_ms,
        buyer_is_market_maker=False,
        received_at=timestamp_ms + 10,
    )


def test_candle_builder_single_minute():
    builder = MinuteCandleBuilder("BTCUSDT")

    # Minute 12:00:00.000 to 12:00:59.999
    base_t = 1786752000000  # aligned to minute

    t1 = make_trade(1, "63000.00", "0.500", base_t + 100)
    t2 = make_trade(2, "63100.00", "1.200", base_t + 5000)
    t3 = make_trade(3, "62950.00", "0.800", base_t + 20000)
    t4 = make_trade(4, "63050.00", "0.300", base_t + 59990)

    # All in same minute, no candle emitted yet
    assert builder.add_trade(t1) is None
    assert builder.add_trade(t2) is None
    assert builder.add_trade(t3) is None
    assert builder.add_trade(t4) is None

    # Next minute trade arrives -> triggers finalization of previous minute
    t_next = make_trade(5, "63060.00", "0.100", base_t + 60000)
    c1 = builder.add_trade(t_next)

    assert c1 is not None
    assert c1.symbol == "BTCUSDT"
    assert c1.timeframe == "1m"
    assert c1.open_time == base_t
    assert c1.close_time == base_t + 59999
    assert c1.open == Decimal("63000.00")
    assert c1.high == Decimal("63100.00")
    assert c1.low == Decimal("62950.00")
    assert c1.close == Decimal("63050.00")
    assert c1.volume == Decimal("2.800")
    assert c1.trade_count == 4
    assert c1.is_closed is True


def test_candle_builder_duplicate_ignored():
    builder = MinuteCandleBuilder("BTCUSDT")
    base_t = 1786752000000

    t1 = make_trade(1, "63000.00", "1.000", base_t + 1000)
    t1_dup = make_trade(1, "63000.00", "1.000", base_t + 1000)

    builder.add_trade(t1)
    builder.add_trade(t1_dup)

    c = builder.finalize_current_candle()
    assert c is not None
    assert c.volume == Decimal("1.000")
    assert c.trade_count == 1


def test_candle_builder_same_timestamp_ordering():
    builder = MinuteCandleBuilder("BTCUSDT")
    base_t = 1786752000000

    # Multiple trades at exact same timestamp ms, ordered by trade ID
    t1 = make_trade(10, "63010.00", "0.1", base_t + 500)
    t2 = make_trade(11, "63020.00", "0.2", base_t + 500)
    t3 = make_trade(12, "63030.00", "0.3", base_t + 500)

    builder.add_trade(t1)
    builder.add_trade(t2)
    builder.add_trade(t3)

    c = builder.finalize_current_candle()
    assert c is not None
    assert c.open == Decimal("63010.00")
    assert c.close == Decimal("63030.00")
    assert c.high == Decimal("63030.00")
    assert c.low == Decimal("63010.00")
    assert c.volume == Decimal("0.6")
