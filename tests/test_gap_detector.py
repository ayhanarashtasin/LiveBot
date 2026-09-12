"""Unit tests for aggTrade gap detector."""
from decimal import Decimal
import pytest

from live_engine.market_data.gap_detector import AggTradeGapDetector, TradeSequenceStatus
from live_engine.market_data.models import AggTrade


def make_trade(agg_id: int, timestamp_ms: int) -> AggTrade:
    return AggTrade(
        event_type="aggTrade",
        event_time=timestamp_ms,
        symbol="BTCUSDT",
        agg_trade_id=agg_id,
        price=Decimal("63000.00"),
        quantity=Decimal("0.100"),
        first_trade_id=agg_id * 10,
        last_trade_id=agg_id * 10 + 1,
        trade_time=timestamp_ms,
        buyer_is_market_maker=False,
        received_at=timestamp_ms + 5,
    )


def test_gap_detector_normal_sequence():
    detector = AggTradeGapDetector("BTCUSDT")

    assert detector.process_trade(make_trade(100, 1000)) == TradeSequenceStatus.VALID
    assert detector.process_trade(make_trade(101, 1050)) == TradeSequenceStatus.VALID
    assert detector.process_trade(make_trade(102, 1100)) == TradeSequenceStatus.VALID
    assert not detector.is_desynced
    assert detector.gap_count == 0


def test_gap_detector_detects_gap():
    detector = AggTradeGapDetector("BTCUSDT")

    detector.process_trade(make_trade(100, 1000))
    # Jump to 105 (missing 101, 102, 103, 104)
    status = detector.process_trade(make_trade(105, 1200))

    assert status == TradeSequenceStatus.GAP
    assert detector.is_desynced is True
    assert detector.gap_count == 1
    assert detector.missing_ids_total == 4
    assert len(detector.active_gaps) == 1
    assert detector.active_gaps[0].from_id == 101
    assert detector.active_gaps[0].to_id == 104


def test_gap_detector_duplicate_and_reversed():
    detector = AggTradeGapDetector("BTCUSDT")

    detector.process_trade(make_trade(100, 1000))
    assert detector.process_trade(make_trade(100, 1000)) == TradeSequenceStatus.DUPLICATE
    assert detector.process_trade(make_trade(99, 990)) == TradeSequenceStatus.REVERSED


def test_gap_detector_recovery():
    detector = AggTradeGapDetector("BTCUSDT")

    detector.process_trade(make_trade(100, 1000))
    detector.process_trade(make_trade(103, 1200))
    assert detector.is_desynced is True

    detector.mark_gap_recovered(101, 102)
    assert detector.is_desynced is False
