"""Unit and invariant tests for SymbolFilters."""
import pytest
from decimal import Decimal
from live_engine.execution.order_filters import SymbolFilters


def test_symbol_filters_rounding():
    filters = SymbolFilters(
        symbol="BTCUSDT",
        tick_size=Decimal("0.10"),
        step_size=Decimal("0.001"),
        min_qty=Decimal("0.001"),
        max_qty=Decimal("1000.0"),
        min_notional=Decimal("5.0"),
    )

    # Quantity step rounding (down)
    assert filters.round_quantity(Decimal("0.0019999")) == Decimal("0.001")
    assert filters.round_quantity(Decimal("1.2345")) == Decimal("1.234")
    assert filters.round_quantity(Decimal("0.0005")) == Decimal("0.000")

    # Price tick rounding
    assert filters.round_price(Decimal("63045.62")) == Decimal("63045.60")
    assert filters.round_price(Decimal("63045.67")) == Decimal("63045.70")


def test_symbol_filters_validation():
    filters = SymbolFilters(
        symbol="BTCUSDT",
        tick_size=Decimal("0.10"),
        step_size=Decimal("0.001"),
        min_qty=Decimal("0.001"),
        max_qty=Decimal("1000.0"),
        min_notional=Decimal("5.0"),
    )

    # Valid order: 0.01 BTC @ 60000 = $600 > $5
    valid, reason = filters.validate_order(Decimal("0.010"), Decimal("60000.00"))
    assert valid is True
    assert reason is None

    # Below min notional: 0.001 BTC @ 4000 = $4 < $5
    valid_notional, reason_notional = filters.validate_order(Decimal("0.001"), Decimal("4000.00"))
    assert valid_notional is False
    assert "below minNotional" in reason_notional

    # Below min quantity: 0.0005 < 0.001
    valid_qty, reason_qty = filters.validate_order(Decimal("0.0005"), Decimal("60000.00"))
    assert valid_qty is False
    assert "below minQty" in reason_qty
