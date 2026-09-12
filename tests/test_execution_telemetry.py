"""Section 9 regressions: immutable execution-quality telemetry."""
import time
from decimal import Decimal

import pytest

from live_engine.execution.fill_applier import FillApplier
from live_engine.execution.models import Order, OrderSide, OrderStatus, OrderType, SignalAction, SignalEvent
from live_engine.execution.order_manager import OrderManager
from live_engine.execution.position_manager import PositionManager
from live_engine.monitoring.telemetry import ExecutionTelemetry, build_execution_comparison
from live_engine.persistence.event_store import EventStore

SYMBOL = "BTCUSDT"

REQUIRED_FIELDS = (
    "benchmark_id", "strategy_hash", "signal_id", "signal_time_ms", "expected_action",
    "reference_price", "requested_quantity", "expected_execution_time_ms",
    "guard_decision", "block_reason", "client_order_id", "exchange_order_id",
    "submitted_quantity", "filled_quantity", "fill_prices", "vwap", "fees", "fee_asset",
    "side", "reduce_only", "position_side", "realized_pnl", "exit_reason",
)


def _signal(action=SignalAction.ENTER_LONG):
    return SignalEvent(
        signal_id="SIG-1", benchmark_id="BTC_ST_09_5M", strategy_hash="abc", symbol=SYMBOL,
        timeframe="5m", candle_open_time=1_700_000_000_000, candle_close_time=1_700_000_299_999,
        generated_at=1_700_000_300_000, action=action, reference_price=Decimal("100"),
        reason="test",
    )


@pytest.fixture
def rig(tmp_path):
    store = EventStore(tmp_path / "telemetry.db")
    om, pm = OrderManager(), PositionManager(SYMBOL)
    applier = FillApplier(om, pm, store, SYMBOL)
    tel = ExecutionTelemetry(store, benchmark_id="BTC_ST_09_5M", mode="PAPER", strategy_hash="abc")
    return tel, store, om, pm, applier


def _fill(applier, om, store, cid="ESC-1", qty="1", price="101", fee="0.05"):
    order = Order(client_order_id=cid, symbol=SYMBOL, side=OrderSide.BUY,
                  order_type=OrderType.MARKET, quantity=Decimal(qty), price=Decimal("100"),
                  status=OrderStatus.SUBMITTING, created_at=1, signal_id="SIG-1")
    om.upsert_order(order)
    store.save_order(order)
    applier.apply(client_order_id=cid, symbol=SYMBOL, side=OrderSide.BUY,
                  status=OrderStatus.FILLED, cumulative_qty=Decimal(qty), last_qty=Decimal(qty),
                  last_price=Decimal(price), avg_price=Decimal(price), commission=Decimal(fee),
                  commission_asset="USDT", exchange_order_id="777", trade_id="9", source="TEST")
    return om.get_order_by_client_id(cid)


def test_every_required_field_is_captured(rig):
    tel, store, om, pm, applier = rig
    record = tel.begin(_signal(), Decimal("1"), Decimal("100"), "ESC-1",
                       1_700_000_300_000, OrderSide.BUY)
    order = _fill(applier, om, store)
    tel.mark(record, "T1")
    tel.mark(record, "T2")
    tel.mark(record, "T3")
    tel.mark(record, "T4")
    finished = tel.finish(record, order, realized_pnl=Decimal("0"), exit_reason=None)

    for field in REQUIRED_FIELDS:
        assert field in finished, field
    assert finished["fee_asset"] == "USDT"
    assert Decimal(finished["vwap"]) == Decimal("101")
    assert finished["fill_prices"] == ["101"]


def test_durations_use_a_monotonic_clock_and_utc_is_kept_for_correlation(rig):
    tel, store, om, pm, applier = rig
    record = tel.begin(_signal(), Decimal("1"), Decimal("100"), "ESC-1",
                       1_700_000_300_000, OrderSide.BUY)
    time.sleep(0.01)
    tel.mark(record, "T1")
    tel.mark(record, "T2")
    finished = tel.finish(record, None)

    assert set(record["monotonic_ns"]) >= {"T0", "T1", "T2"}
    assert all(v > 0 for v in record["utc_ms"].values())
    assert finished["latency_ms"]["submit_dispatch_ms"] > 0
    assert all(v >= 0 for v in finished["latency_ms"].values())


def test_first_fill_milestone_is_not_overwritten_by_later_fills(rig):
    tel, store, om, pm, applier = rig
    record = tel.begin(_signal(), Decimal("1"), Decimal("100"), "ESC-1",
                       1_700_000_300_000, OrderSide.BUY)
    tel.mark(record, "T3")
    first = record["monotonic_ns"]["T3"]
    time.sleep(0.005)
    tel.mark(record, "T3")
    assert record["monotonic_ns"]["T3"] == first


def test_slippage_and_shortfall_are_derived_from_actual_fills(rig):
    tel, store, om, pm, applier = rig
    record = tel.begin(_signal(), Decimal("1"), Decimal("100"), "ESC-1",
                       1_700_000_300_000, OrderSide.BUY)
    order = _fill(applier, om, store, price="101", fee="0.05")
    finished = tel.finish(record, order)

    assert Decimal(finished["entry_slippage"]) == Decimal("1")   # paid 101 vs 100 reference
    assert Decimal(finished["implementation_shortfall"]) == Decimal("1.05")


def test_no_measurement_is_claimed_that_was_not_captured(rig):
    tel, store, om, pm, applier = rig
    record = tel.begin(_signal(), Decimal("1"), Decimal("100"), "ESC-1",
                       1_700_000_300_000, OrderSide.BUY)
    finished = tel.finish(record, None, block_reason="BLOCKED_BY_GUARD")

    assert finished["guard_decision"] == "BLOCKED"
    assert finished["block_reason"] == "BLOCKED_BY_GUARD"
    # Nothing was submitted, so nothing about fills, spread or latency past T0 is invented.
    assert finished["vwap"] is None
    assert finished["spread_crossing_cost"] is None
    assert finished["filled_quantity"] is None
    assert finished["latency_ms"] == {}


def test_records_are_append_only_and_readable_as_a_projection(rig):
    tel, store, om, pm, applier = rig
    for i in range(3):
        record = tel.begin(_signal(), Decimal("1"), Decimal("100"), f"ESC-{i}",
                           1_700_000_300_000, OrderSide.BUY)
        tel.finish(record, None, block_reason="probe")

    rows = store.get_telemetry()
    assert len(rows) == 3
    assert [r["client_order_id"] for r in rows] == ["ESC-0", "ESC-1", "ESC-2"]


def test_comparison_report_separates_strategy_from_execution_divergence(rig):
    tel, store, om, pm, applier = rig
    entry = tel.begin(_signal(), Decimal("1"), Decimal("100"), "ESC-1",
                      1_700_000_300_000, OrderSide.BUY)
    order = _fill(applier, om, store, price="101")
    tel.finish(entry, order)

    report = build_execution_comparison(store.get_telemetry(), benchmark_trades=[{"signal_id": "SIG-1"}])
    assert report["benchmark_trades"] == 1
    assert report["entry_signals"] == 1
    assert report["missing_signals"] == []
    assert report["divergence_attribution"] == "EXECUTION"
    assert Decimal(report["avg_entry_slippage_pct"]) == Decimal("1")

    missing = build_execution_comparison([], benchmark_trades=[{"signal_id": "SIG-NEVER-SEEN"}])
    assert missing["missing_signals"] == ["SIG-NEVER-SEEN"]
    assert missing["divergence_attribution"] == "STRATEGY/DATA"
