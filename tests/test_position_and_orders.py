"""Unit and invariant tests for PositionManager, OrderManager, and ClientOrderId generation."""
import pytest
from decimal import Decimal
from live_engine.execution.models import Order, OrderSide, OrderStatus, OrderType
from live_engine.execution.position_manager import PositionManager
from live_engine.execution.order_manager import OrderManager
from live_engine.execution.idempotency import generate_client_order_id


def test_client_order_id_determinism_and_length():
    # Determinism
    id1 = generate_client_order_id("BTCUSDT", "5m", 1723818600000, "BUY")
    id2 = generate_client_order_id("BTCUSDT", "5m", 1723818600000, "BUY")
    assert id1 == id2
    assert len(id1) <= 36
    assert id1.startswith("ESC-BTCUSDT-5M-1723818600-BUY")

    # Long symbol handling (< 36 chars)
    long_sym = "LONGTOKENUSDTFUTURES12345"
    id_long = generate_client_order_id(long_sym, "15m", 1723818600000, "BUY")
    assert len(id_long) <= 36


def test_position_manager_lifecycle():
    pm = PositionManager("BTCUSDT")
    assert pm.is_flat is True

    # 1. Open Long 0.5 @ 60,000
    pm.on_fill(OrderSide.BUY, Decimal("0.5"), Decimal("60000.0"), fee=Decimal("15.0"))
    assert pm.is_long is True
    assert pm.quantity == Decimal("0.5")
    assert pm.entry_price == Decimal("60000.0")
    assert pm.realized_pnl == Decimal("0")

    # Unrealized PnL at 62,000
    unrealized = pm.update_unrealized_pnl(Decimal("62000.0"))
    assert unrealized == Decimal("1000.0")  # (62000 - 60000) * 0.5 = 1000

    # 2. Add to Long 0.5 @ 64,000 -> Avg price 62,000, total qty 1.0
    pm.on_fill(OrderSide.BUY, Decimal("0.5"), Decimal("64000.0"), fee=Decimal("16.0"))
    assert pm.quantity == Decimal("1.0")
    assert pm.entry_price == Decimal("62000.0")

    # 3. Partial exit 0.4 @ 65,000
    pm.on_fill(OrderSide.SELL, Decimal("0.4"), Decimal("65000.0"), fee=Decimal("13.0"))
    assert pm.quantity == Decimal("0.6")
    # Realized PnL: (65000 - 62000) * 0.4 - 13 = 1200 - 13 = 1187
    assert pm.realized_pnl == Decimal("1187.0")

    # 4. Full close remaining 0.6 @ 63,000
    pm.on_fill(OrderSide.SELL, Decimal("0.6"), Decimal("63000.0"), fee=Decimal("18.9"))
    assert pm.is_flat is True
    assert pm.quantity == Decimal("0")
    assert pm.trade_count == 1
    # Additional PnL: (63000 - 62000) * 0.6 - 18.9 = 600 - 18.9 = 581.1
    # Total PnL: 1187 + 581.1 = 1768.1
    assert pm.realized_pnl == Decimal("1768.1")


def test_order_manager_lifecycle():
    om = OrderManager()
    order = Order(
        client_order_id="ESC-TEST-1",
        symbol="BTCUSDT",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=Decimal("0.1"),
        status=OrderStatus.CREATED,
    )

    om.register_order(order)
    assert om.get_order_by_client_id("ESC-TEST-1") == order
    assert len(om.get_open_orders()) == 1

    # Transition to SUBMITTING
    om.transition_status(order, OrderStatus.SUBMITTING)
    assert order.status == OrderStatus.SUBMITTING

    # Transition to NEW with exchange_order_id
    om.transition_status(order, OrderStatus.NEW, exchange_order_id="EXCH-999")
    assert order.status == OrderStatus.NEW
    assert om.get_order_by_exchange_id("EXCH-999") == order

    # Transition to FILLED
    om.transition_status(
        order,
        OrderStatus.FILLED,
        filled_qty=Decimal("0.1"),
        avg_price=Decimal("62500.0"),
        fee=Decimal("3.125"),
    )
    assert order.status == OrderStatus.FILLED
    assert len(om.get_open_orders()) == 0


def test_order_manager_repeated_partial_fills():
    """Verify partial fill accumulation - repeated PARTIALLY_FILLED updates should accumulate."""
    om = OrderManager()
    order = Order(
        client_order_id="ESC-PARTIAL-1",
        symbol="BTCUSDT",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=Decimal("1.0"),
        status=OrderStatus.CREATED,
    )

    om.register_order(order)

    # First partial fill: 0.2 quantity
    om.transition_status(
        order,
        OrderStatus.PARTIALLY_FILLED,
        exchange_order_id="EXCH-123",
        filled_qty=Decimal("0.2"),
        avg_price=Decimal("60000.0"),
        fee=Decimal("6.0"),
    )
    assert order.status == OrderStatus.PARTIALLY_FILLED
    assert order.filled_quantity == Decimal("0.2")
    assert order.avg_fill_price == Decimal("60000.0")
    assert order.accumulated_fees == Decimal("6.0")

    # Second partial fill: 0.5 cumulative quantity (0.2 + 0.3)
    om.transition_status(
        order,
        OrderStatus.PARTIALLY_FILLED,
        filled_qty=Decimal("0.5"),
        avg_price=Decimal("60100.0"),
        fee=Decimal("15.0"),
    )
    assert order.status == OrderStatus.PARTIALLY_FILLED
    assert order.filled_quantity == Decimal("0.5")  # Updated to new cumulative
    assert order.avg_fill_price == Decimal("60100.0")  # Updated to new avg
    assert order.accumulated_fees == Decimal("15.0")  # Updated to new total fee

    # Final fill: complete order
    om.transition_status(
        order,
        OrderStatus.FILLED,
        filled_qty=Decimal("1.0"),
        avg_price=Decimal("60050.0"),
        fee=Decimal("30.0"),
    )
    assert order.status == OrderStatus.FILLED
    assert order.filled_quantity == Decimal("1.0")
    assert order.accumulated_fees == Decimal("30.0")


def test_order_manager_terminal_absorbing_state():
    """Verify terminal order cannot regress to NEW or PARTIALLY_FILLED on stale event."""
    om = OrderManager()
    order = Order(
        client_order_id="ESC-TERM-1",
        symbol="BTCUSDT",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=Decimal("1.0"),
        status=OrderStatus.SUBMITTING,
    )
    om.register_order(order)

    # Order is FILLED
    om.transition_status(order, OrderStatus.FILLED, filled_qty=Decimal("1.0"), avg_price=Decimal("60000.0"))
    assert order.status == OrderStatus.FILLED

    # Stale/delayed NEW event arrives
    om.transition_status(order, OrderStatus.NEW, filled_qty=Decimal("0"))
    assert order.status == OrderStatus.FILLED
    assert order.filled_quantity == Decimal("1.0")

    # Stale PARTIALLY_FILLED event arrives
    om.transition_status(order, OrderStatus.PARTIALLY_FILLED, filled_qty=Decimal("0.5"))
    assert order.status == OrderStatus.FILLED
    assert order.filled_quantity == Decimal("1.0")


def test_order_manager_monotonic_fills():
    """Verify stale lower cumulative quantity is ignored."""
    om = OrderManager()
    order = Order(
        client_order_id="ESC-MONO-1",
        symbol="BTCUSDT",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=Decimal("1.0"),
        status=OrderStatus.SUBMITTING,
    )
    om.register_order(order)

    # Fill 0.7
    om.transition_status(order, OrderStatus.PARTIALLY_FILLED, filled_qty=Decimal("0.7"), avg_price=Decimal("60000.0"))
    assert order.filled_quantity == Decimal("0.7")

    # Delayed/stale event with cumulative 0.3 arrives
    om.transition_status(order, OrderStatus.PARTIALLY_FILLED, filled_qty=Decimal("0.3"))
    assert order.filled_quantity == Decimal("0.7")


def test_order_manager_unknown_resolution():
    """Verify UNKNOWN status transition and subsequent resolution to FILLED."""
    om = OrderManager()
    order = Order(
        client_order_id="ESC-UNK-1",
        symbol="BTCUSDT",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=Decimal("0.5"),
        status=OrderStatus.SUBMITTING,
    )
    om.register_order(order)

    # Transition to UNKNOWN on timeout
    om.transition_status(order, OrderStatus.UNKNOWN, error_msg="Network timeout")
    assert order.status == OrderStatus.UNKNOWN

    # Reconciled to FILLED from exchange query
    om.transition_status(order, OrderStatus.FILLED, filled_qty=Decimal("0.5"), avg_price=Decimal("61000.0"))
    assert order.status == OrderStatus.FILLED
    assert order.filled_quantity == Decimal("0.5")
