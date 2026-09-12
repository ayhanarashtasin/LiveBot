"""Tests for PaperBroker margin checks, fee accounting, and persistence recovery."""
from decimal import Decimal
import tempfile
from pathlib import Path
import pytest

from live_engine.execution.broker import PaperBroker
from live_engine.execution.models import OrderSide, OrderType, OrderStatus
from live_engine.persistence.event_store import EventStore


def test_paper_broker_margin_rejection():
    """Verify that PaperBroker rejects orders when balance is insufficient."""
    broker = PaperBroker(initial_balance=Decimal("100.0"), leverage=1)

    # 1 BTC @ $60,000 requires $60,000 margin + fee, but balance is only $100
    order = broker.place_order(
        symbol="BTCUSDT",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=Decimal("1.0"),
        price=Decimal("60000.0"),
    )

    assert order.status == OrderStatus.REJECTED
    assert "Insufficient margin" in (order.rejection_reason or "")
    assert broker.balance == Decimal("100.0")


def test_paper_broker_successful_trade_and_fees():
    """Verify margin deduction, position tracking, realized PnL, and fees."""
    broker = PaperBroker(initial_balance=Decimal("10000.0"), taker_fee=Decimal("0.0005"))

    # Buy 0.1 BTC @ 50,000 USD (Notional = 5000 USD, Fee = 2.5 USD)
    entry_order = broker.place_order(
        symbol="BTCUSDT",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=Decimal("0.1"),
        price=Decimal("50000.0"),
    )
    assert entry_order.status == OrderStatus.FILLED
    assert entry_order.accumulated_fees == Decimal("2.5")

    side, qty, entry_px = broker.get_position("BTCUSDT")
    assert side == OrderSide.BUY
    assert qty == Decimal("0.1")
    assert entry_px == Decimal("50000.0")

    # Sell 0.1 BTC @ 55,000 USD (Profit = 500 USD, Fee = 2.75 USD)
    exit_order = broker.place_order(
        symbol="BTCUSDT",
        side=OrderSide.SELL,
        order_type=OrderType.MARKET,
        quantity=Decimal("0.1"),
        price=Decimal("55000.0"),
    )
    assert exit_order.status == OrderStatus.FILLED

    side, qty, _ = broker.get_position("BTCUSDT")
    assert qty == Decimal("0")
    # Initial 10000 + 500 (PnL) - 2.5 (Entry fee) - 2.75 (Exit fee) = 10494.75
    assert broker.balance == Decimal("10494.75")


def test_paper_broker_restart_recovery():
    """Verify that PaperBroker restores account balance and positions across restarts."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        db_path = Path(tmp_dir) / "test_events.db"
        store = EventStore(db_path)

        # Instance 1: Buy 0.05 BTC and open position
        broker1 = PaperBroker(initial_balance=Decimal("5000.0"), event_store=store)
        broker1.place_order(
            symbol="BTCUSDT",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=Decimal("0.05"),
            price=Decimal("60000.0"),
        )
        assert broker1.positions["BTCUSDT"].quantity == Decimal("0.05")

        # Instance 2: Start new broker on same database
        broker2 = PaperBroker(initial_balance=Decimal("5000.0"), event_store=store)
        side, qty, entry_px = broker2.get_position("BTCUSDT")
        assert side == OrderSide.BUY
        assert qty == Decimal("0.05")
        assert entry_px == Decimal("60000.0")


def test_full_compounding_order_margin_fits():
    """Verify that full-compounding order with fee reservation never exceeds balance."""
    from live_engine.execution.order_filters import SymbolFilters

    filters = SymbolFilters(
        symbol="LITUSDT",
        tick_size=Decimal("0.0001"),
        step_size=Decimal("0.1"),
        min_qty=Decimal("0.1"),
        max_qty=Decimal("100000.0"),
        min_notional=Decimal("5.0"),
    )
    price = Decimal("4.5044")
    balance = Decimal("10000.0")
    broker = PaperBroker(initial_balance=balance, taker_fee=Decimal("0.0005"))

    fee_rate = broker.taker_fee
    allocated_stake = balance / (Decimal("1") + fee_rate)
    target_qty = filters.round_quantity(allocated_stake / price)

    order = broker.place_order("LITUSDT", OrderSide.BUY, OrderType.MARKET, target_qty, price)
    assert order.status == OrderStatus.FILLED
    assert order.filled_quantity == target_qty
    assert order.rejection_reason is None

