"""Section 11 regressions: PAPER stays deterministic but stops implying perfect fills."""
from decimal import Decimal

import pytest

from live_engine.execution.broker import PaperBroker, PaperExecutionModel
from live_engine.execution.models import OrderSide, OrderStatus, OrderType
from live_engine.execution.order_filters import SymbolFilters
from live_engine.persistence.event_store import EventStore

SYMBOL = "BTCUSDT"
FILTERS = SymbolFilters(symbol=SYMBOL, tick_size=Decimal("0.1"), step_size=Decimal("0.001"),
                        min_qty=Decimal("0.001"), max_qty=Decimal("100"),
                        min_notional=Decimal("5"))


def test_default_model_is_zero_impact_and_labelled_benchmark_replay():
    model = PaperExecutionModel()
    assert model.is_zero_impact is True
    assert model.label == "benchmark_replay"
    # A zero-impact model is a benchmark replay, not a claim about real execution.
    assert model.to_dict()["simulated"] is True


def test_configured_spread_and_slippage_move_the_arrival_price():
    model = PaperExecutionModel(label="realistic", spread_bps=Decimal("4"), slippage_bps=Decimal("1"))
    buy = model.arrival_price(OrderSide.BUY, Decimal("100"))
    sell = model.arrival_price(OrderSide.SELL, Decimal("100"))

    assert buy > Decimal("100") > sell
    assert buy == Decimal("100") * (Decimal("1") + Decimal("3") / Decimal("10000"))
    assert model.is_zero_impact is False


def test_fill_price_comes_from_the_submitted_reference_not_a_later_known_price():
    broker = PaperBroker(execution_model=PaperExecutionModel(spread_bps=Decimal("2")))
    order = broker.place_order(SYMBOL, OrderSide.BUY, OrderType.MARKET,
                               Decimal("1"), Decimal("100"), "ESC-1")
    # The fill is derived from the price the order was actually submitted with.
    assert order.avg_fill_price == broker.execution_model.arrival_price(OrderSide.BUY, Decimal("100"))


def test_exchange_filters_are_modelled_not_assumed_away():
    broker = PaperBroker(filters=FILTERS)
    tiny = broker.place_order(SYMBOL, OrderSide.BUY, OrderType.MARKET,
                              Decimal("0.0001"), Decimal("100"), "ESC-TINY")
    assert tiny.status is OrderStatus.REJECTED
    assert "filter" in tiny.rejection_reason.lower()

    below_notional = broker.place_order(SYMBOL, OrderSide.BUY, OrderType.MARKET,
                                        Decimal("0.001"), Decimal("100"), "ESC-NOTIONAL")
    assert below_notional.status is OrderStatus.REJECTED
    assert "notional" in below_notional.rejection_reason.lower()


def test_insufficient_margin_is_modelled():
    broker = PaperBroker(initial_balance=Decimal("10"), leverage=1)
    order = broker.place_order(SYMBOL, OrderSide.BUY, OrderType.MARKET,
                               Decimal("1"), Decimal("1000"), "ESC-MARGIN")
    assert order.status is OrderStatus.REJECTED
    assert "margin" in order.rejection_reason.lower()


def test_positions_are_marked_to_the_current_market_price():
    broker = PaperBroker()
    broker.place_order(SYMBOL, OrderSide.BUY, OrderType.MARKET, Decimal("2"), Decimal("100"), "ESC-1")
    unrealized = broker.mark_position(SYMBOL, Decimal("110"))
    assert unrealized == Decimal("20")


def test_funding_affects_the_simulated_account_over_the_holding_period():
    broker = PaperBroker()
    broker.place_order(SYMBOL, OrderSide.BUY, OrderType.MARKET, Decimal("2"), Decimal("100"), "ESC-1")
    before = broker.balance

    payment = broker.apply_funding(SYMBOL, Decimal("0.0001"), Decimal("100"))
    assert payment == Decimal("0.02")           # 2 * 100 * 0.0001, paid by the long
    assert broker.balance == before - payment
    assert broker.funding_paid == payment


def test_simulated_values_are_labelled_in_persistence(tmp_path):
    store = EventStore(tmp_path / "paper.db")
    broker = PaperBroker(event_store=store)
    broker.place_order(SYMBOL, OrderSide.BUY, OrderType.MARKET, Decimal("1"), Decimal("100"), "ESC-1")

    snapshots = store.get_events_by_type("PAPER_ACCOUNT_SNAPSHOT")
    payload = snapshots[-1]["payload"]
    assert payload["simulated"] is True
    assert payload["value_source"] == "SIMULATED"
    assert payload["execution_model"]["label"] == "benchmark_replay"


def test_reduce_only_exit_still_cannot_reverse_in_paper():
    broker = PaperBroker()
    broker.place_order(SYMBOL, OrderSide.BUY, OrderType.MARKET, Decimal("1"), Decimal("100"), "IN")
    broker.place_order(SYMBOL, OrderSide.SELL, OrderType.MARKET, Decimal("9"), Decimal("110"),
                       "OUT", reduce_only=True)
    side, qty, _ = broker.get_position(SYMBOL)
    assert qty == Decimal("0") and side is None
