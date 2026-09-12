"""Section 1 regressions: delta-based, idempotent fill application.

Every case here corresponds to a way the old code doubled or lost a position:
the REST response and the user stream both applying the same cumulative `z`,
a replayed execution ID re-charging fees, or an oversize exit reversing the account.
"""
from decimal import Decimal

import pytest

from live_engine.execution.fill_applier import FillApplier
from live_engine.execution.models import Order, OrderSide, OrderStatus, OrderType
from live_engine.execution.order_manager import OrderManager
from live_engine.execution.position_manager import PositionManager
from live_engine.persistence.event_store import EventStore

SYMBOL = "BTCUSDT"


@pytest.fixture
def rig(tmp_path):
    """Fresh managers over a throwaway database (never touches data/*.db)."""
    store = EventStore(tmp_path / "fills.db")
    om, pm = OrderManager(), PositionManager(SYMBOL)
    return FillApplier(om, pm, store, SYMBOL), om, pm, store


def _intent(om, store, cid, side=OrderSide.BUY, qty="1", price="100", reduce_only=False):
    order = Order(
        client_order_id=cid, symbol=SYMBOL, side=side, order_type=OrderType.MARKET,
        quantity=Decimal(qty), price=Decimal(price), status=OrderStatus.SUBMITTING,
        created_at=1, reduce_only=reduce_only,
    )
    om.upsert_order(order)
    store.save_order(order)
    return order


def _stream(cid, *, side="BUY", status="FILLED", z="1", l="1", L="100", n="0", t=None, R=False):
    o = {"c": cid, "s": SYMBOL, "S": side, "X": status, "z": z, "l": l, "L": L,
         "ap": L, "n": n, "N": "USDT", "i": "555", "R": R}
    if t is not None:
        o["t"] = t
    return {"e": "ORDER_TRADE_UPDATE", "E": 1700000000000, "T": 1700000000000, "o": o}


def test_rest_fill_then_stream_fill_does_not_double_position(rig):
    applier, om, pm, store = rig
    order = _intent(om, store, "ESC-1")

    order.status = OrderStatus.FILLED
    order.filled_quantity = Decimal("1")
    order.avg_fill_price = Decimal("100")
    assert applier.apply_order_response(order, source="REST_RESPONSE") == Decimal("1")
    assert pm.quantity == Decimal("1")

    # The exchange now reports the same fill on the private stream as cumulative z=1.
    assert applier.apply_stream_event(_stream("ESC-1", t="9001")) == Decimal("0")
    assert pm.quantity == Decimal("1")


def test_partial_fill_sequence_applies_only_deltas(rig):
    applier, om, pm, store = rig
    _intent(om, store, "ESC-2")

    deltas = [
        applier.apply_stream_event(_stream("ESC-2", status="PARTIALLY_FILLED", z="0.2", l="0.2", t="1")),
        applier.apply_stream_event(_stream("ESC-2", status="PARTIALLY_FILLED", z="0.5", l="0.3", t="2")),
        applier.apply_stream_event(_stream("ESC-2", status="PARTIALLY_FILLED", z="0.5", l="0.3", t="2")),
        applier.apply_stream_event(_stream("ESC-2", status="FILLED", z="1.0", l="0.5", t="3")),
    ]
    assert deltas == [Decimal("0.2"), Decimal("0.3"), Decimal("0"), Decimal("0.5")]
    assert pm.quantity == Decimal("1.0")


def test_duplicate_execution_id_does_not_duplicate_fees_or_pnl(rig):
    applier, om, pm, store = rig
    _intent(om, store, "ESC-3")

    applier.apply_stream_event(_stream("ESC-3", z="1", l="1", n="0.05", t="42"))
    fees_once, qty_once = pm.total_fees_paid, pm.quantity

    applier.apply_stream_event(_stream("ESC-3", z="1", l="1", n="0.05", t="42"))
    assert pm.total_fees_paid == fees_once
    assert pm.quantity == qty_once
    assert len(store.get_fills("ESC-3")) == 1


def test_stale_out_of_order_event_changes_nothing(rig):
    applier, om, pm, store = rig
    _intent(om, store, "ESC-4")

    applier.apply_stream_event(_stream("ESC-4", status="FILLED", z="1", l="1", t="7"))
    before = pm.to_dict()
    # An older partial arrives late with a lower cumulative quantity.
    assert applier.apply_stream_event(_stream("ESC-4", status="PARTIALLY_FILLED", z="0.4", l="0.4", t="6")) == Decimal("0")
    assert pm.to_dict() == before


def test_reduce_only_fill_cannot_reverse_position(rig):
    applier, om, pm, store = rig
    _intent(om, store, "ESC-5")
    applier.apply_stream_event(_stream("ESC-5", z="1", l="1", t="10"))
    assert pm.quantity == Decimal("1")

    # Local state was stale: the exit is for 3 but only 1 is actually open.
    _intent(om, store, "ESC-5X", side=OrderSide.SELL, qty="3", reduce_only=True)
    applied = applier.apply_stream_event(_stream("ESC-5X", side="SELL", z="3", l="3", t="11", R=True))
    assert applied == Decimal("1")
    assert pm.is_flat
    assert pm.quantity == Decimal("0")


def test_replay_of_stored_fills_reconstructs_identical_state(rig, tmp_path):
    applier, om, pm, store = rig
    _intent(om, store, "ESC-6")
    applier.apply_stream_event(_stream("ESC-6", status="PARTIALLY_FILLED", z="0.4", l="0.4", L="100", n="0.02", t="1"))
    applier.apply_stream_event(_stream("ESC-6", status="FILLED", z="1", l="0.6", L="110", n="0.03", t="2"))
    original = pm.to_dict()

    fresh_om, fresh_pm = OrderManager(), PositionManager(SYMBOL)
    replayer = FillApplier(fresh_om, fresh_pm, store, SYMBOL)
    assert replayer.replay_from_store() == 2
    assert fresh_pm.to_dict() == original
    assert fresh_om.get_order_by_client_id("ESC-6").applied_cumulative_qty == Decimal("1")


def test_terminal_order_still_accepts_a_proven_new_execution(rig):
    applier, om, pm, store = rig
    _intent(om, store, "ESC-7", qty="2")
    applier.apply_stream_event(_stream("ESC-7", status="FILLED", z="1", l="1", t="1"))
    assert om.get_order_by_client_id("ESC-7").status is OrderStatus.FILLED

    # Reconciliation surfaces an execution the engine never saw.
    assert applier.apply_stream_event(_stream("ESC-7", status="FILLED", z="2", l="1", t="2")) == Decimal("1")
    assert pm.quantity == Decimal("2")


def test_position_manager_rejects_reduce_only_fill_when_flat():
    pm = PositionManager(SYMBOL)
    pm.on_fill(OrderSide.SELL, Decimal("5"), Decimal("100"), Decimal("0"), reduce_only=True)
    assert pm.is_flat and pm.quantity == Decimal("0")


def test_broker_response_upsert_does_not_double_the_commission(rig):
    """The paper/live submit path upserts the broker's filled order, then applies it.

    The broker reports the order's *cumulative* fee. When upsert_order also wrote that
    onto the tracked order, apply() added it to itself and every order was stored at 2x
    its real commission — which is what the dashboard and telemetry read.
    """
    applier, om, pm, store = rig
    _intent(om, store, "ESC-FEE", qty="0.127", price="78180.10")

    fee = Decimal("0.127") * Decimal("78180.10") * Decimal("0.0005")
    filled = Order(
        client_order_id="ESC-FEE", symbol=SYMBOL, side=OrderSide.BUY,
        order_type=OrderType.MARKET, quantity=Decimal("0.127"), price=Decimal("78180.10"),
        status=OrderStatus.FILLED, filled_quantity=Decimal("0.127"),
        avg_fill_price=Decimal("78180.10"), accumulated_fees=fee, created_at=1,
    )
    om.upsert_order(filled)
    assert applier.apply_order_response(filled, source="REST_RESPONSE") == Decimal("0.127")

    assert om.get_order_by_client_id("ESC-FEE").accumulated_fees == fee
    assert pm.total_fees_paid == fee
