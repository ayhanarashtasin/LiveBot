"""Section 2 regressions: futures exits cannot open a reverse position.

The old broker submitted SELL exits with no reduceOnly and no positionSide, sized purely
from local state. Every case below is a way that reversed a real account.
"""
from decimal import Decimal

import pytest

from live_engine.execution.broker import (
    BaseBinanceBroker,
    PaperBroker,
    ShadowBroker,
    UnsupportedPositionModeError,
)
from live_engine.execution.models import OrderSide, OrderStatus, OrderType


class FakeBinanceBroker(BaseBinanceBroker):
    """Authenticated broker with the network replaced by a scripted response map."""

    def __init__(self, responses):
        self.responses = responses
        self.sent = []
        self._time_offset_ms = 0
        self.api_key, self.api_secret = "k", "s"
        self.base_url = "https://testnet.binancefuture.com"
        self.mode = "TESTNET"

    def _request(self, method, endpoint, params=None, attempt=0):
        self.sent.append((method, endpoint, dict(params or {})))
        value = self.responses[endpoint]
        if isinstance(value, Exception):
            raise value
        return value


def test_one_way_mode_exit_is_reduce_only():
    broker = FakeBinanceBroker({
        "/fapi/v1/positionSide/dual": {"dualSidePosition": False},
        "/fapi/v1/order": {"orderId": 1, "status": "FILLED", "executedQty": "1", "avgPrice": "100"},
    })
    assert broker.detect_position_mode() == "ONE_WAY"

    broker.place_order("BTCUSDT", OrderSide.SELL, OrderType.MARKET, Decimal("1"),
                       Decimal("100"), "ESC-X", reduce_only=True)
    params = [p for m, e, p in broker.sent if e == "/fapi/v1/order"][0]
    assert params["reduceOnly"] == "true"
    assert "positionSide" not in params


def test_hedge_mode_sends_explicit_position_side_and_no_reduce_only():
    broker = FakeBinanceBroker({
        "/fapi/v1/positionSide/dual": {"dualSidePosition": True},
        "/fapi/v1/order": {"orderId": 2, "status": "FILLED", "executedQty": "1", "avgPrice": "100"},
    })
    assert broker.detect_position_mode() == "HEDGE"

    broker.place_order("BTCUSDT", OrderSide.SELL, OrderType.MARKET, Decimal("1"),
                       Decimal("100"), "ESC-Y", reduce_only=True, position_side="LONG")
    params = [p for m, e, p in broker.sent if e == "/fapi/v1/order"][0]
    assert params["positionSide"] == "LONG"
    assert "reduceOnly" not in params


def test_undeterminable_position_mode_fails_closed():
    broker = FakeBinanceBroker({"/fapi/v1/positionSide/dual": RuntimeError("network down")})
    with pytest.raises(UnsupportedPositionModeError, match="UNSUPPORTED POSITION MODE"):
        broker.detect_position_mode()

    malformed = FakeBinanceBroker({"/fapi/v1/positionSide/dual": {"unexpected": 1}})
    with pytest.raises(UnsupportedPositionModeError):
        malformed.detect_position_mode()


def test_exit_before_position_mode_detected_is_refused():
    broker = FakeBinanceBroker({"/fapi/v1/order": {}})
    broker.position_mode = "UNVERIFIED"
    with pytest.raises(UnsupportedPositionModeError):
        broker.exit_order_params("LONG")


@pytest.mark.parametrize("broker_factory", [ShadowBroker, lambda: PaperBroker(event_store=None)])
def test_simulated_exit_when_flat_is_blocked(broker_factory):
    broker = broker_factory()
    order = broker.place_order("BTCUSDT", OrderSide.SELL, OrderType.MARKET,
                               Decimal("1"), Decimal("100"), "ESC-FLAT", reduce_only=True)
    side, qty, _ = broker.get_position("BTCUSDT")
    assert qty == Decimal("0")
    assert order.status in (OrderStatus.REJECTED, OrderStatus.FILLED)
    assert order.filled_quantity == Decimal("0")


@pytest.mark.parametrize("broker_factory", [ShadowBroker, lambda: PaperBroker(event_store=None)])
def test_stale_oversize_exit_cannot_reverse_simulated_account(broker_factory):
    broker = broker_factory()
    broker.place_order("BTCUSDT", OrderSide.BUY, OrderType.MARKET, Decimal("1"), Decimal("100"), "IN")
    # Local state believed 5 were open; only 1 is.
    broker.place_order("BTCUSDT", OrderSide.SELL, OrderType.MARKET, Decimal("5"),
                       Decimal("110"), "OUT", reduce_only=True)
    side, qty, _ = broker.get_position("BTCUSDT")
    assert qty == Decimal("0")
    assert side is None


def test_orchestrator_clamps_exit_to_authoritative_exchange_position(monkeypatch):
    """A stale local quantity larger than the exchange position is clamped, never reversed."""
    from live_engine.execution.order_filters import SymbolFilters

    class Stub:
        filters = SymbolFilters(
            symbol="BTCUSDT", tick_size=Decimal("0.1"), step_size=Decimal("0.001"),
            min_qty=Decimal("0.001"), max_qty=Decimal("1000"), min_notional=Decimal("5"),
        )

        class config:
            symbol = "BTCUSDT"

        class broker:
            @staticmethod
            def get_position(symbol):
                return OrderSide.BUY, Decimal("0.4"), Decimal("100")

        class event_store:
            @staticmethod
            def record_incident(*a, **k):
                return None

    from live_engine.orchestrator import LiveEngineOrchestrator

    qty, block = LiveEngineOrchestrator._authoritative_exit_quantity(Stub(), Decimal("2.0"))
    assert block is None
    assert qty == Decimal("0.400")

    class Flat(Stub):
        class broker:
            @staticmethod
            def get_position(symbol):
                return None, Decimal("0"), Decimal("0")

    qty, block = LiveEngineOrchestrator._authoritative_exit_quantity(Flat(), Decimal("1"))
    assert qty == Decimal("0")
    assert "flat" in block.lower()
