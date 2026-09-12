"""Tests for ShadowBroker and SHADOW mode expected position tracking."""
from decimal import Decimal
from live_engine.execution.broker import ShadowBroker
from live_engine.execution.models import OrderSide, OrderType, OrderStatus
from live_engine.execution.position_manager import PositionManager
from live_engine.account.reconciliation import AccountReconciler


def test_shadow_broker_position_tracking():
    """Verify ShadowBroker tracks positions locally without exchange interaction."""
    broker = ShadowBroker()
    assert broker.mode == "SHADOW"

    order = broker.place_order(
        symbol="BTCUSDT",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=Decimal("0.02"),
        price=Decimal("62000.0"),
    )
    assert order.status == OrderStatus.FILLED

    side, qty, entry_px = broker.get_position("BTCUSDT")
    assert side == OrderSide.BUY
    assert qty == Decimal("0.02")
    assert entry_px == Decimal("62000.0")


def test_shadow_reconciliation_preserves_expected_position():
    """Verify AccountReconciler does not wipe expected position in SHADOW mode."""
    broker = ShadowBroker()
    pm = PositionManager("BTCUSDT")

    # Order entry
    broker.place_order(
        symbol="BTCUSDT",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=Decimal("0.015"),
        price=Decimal("63000.0"),
    )
    pm.on_fill(OrderSide.BUY, Decimal("0.015"), Decimal("63000.0"))

    reconciler = AccountReconciler(broker=broker, position_manager=pm)
    res = reconciler.reconcile_position()

    assert res["matched"] is True
    assert pm.quantity == Decimal("0.015")
    assert pm.side == OrderSide.BUY
    assert not pm.is_flat
