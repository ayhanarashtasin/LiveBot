"""Section 7 regressions: protective-exit semantics, state and failure safety."""
from decimal import Decimal

import pytest
import yaml

from live_engine.execution.models import Order, OrderSide, OrderStatus, OrderType
from live_engine.persistence.event_store import EventStore
from live_engine.risk.health import HealthMonitor
from live_engine.risk.protective import ProtectiveExitManager, parse_multiplier

SYMBOL = "BTCUSDT"


def manifest(name):
    return yaml.safe_load(open(f"benchmarks/manifests/{name}.yaml", encoding="utf-8"))


@pytest.fixture
def manager(tmp_path):
    store = EventStore(tmp_path / "protective.db")
    health = HealthMonitor(event_store=store)
    return ProtectiveExitManager(store, manifest("BTC_ST_09_5M"), SYMBOL, health=health), store, health


def _order(cid="ESC-IN", qty="1", side=OrderSide.BUY):
    return Order(client_order_id=cid, symbol=SYMBOL, side=side, order_type=OrderType.MARKET,
                 quantity=Decimal(qty), status=OrderStatus.FILLED,
                 filled_quantity=Decimal(qty), created_at=1, exchange_order_id="999",
                 signal_id="SIG-IN")


# --- documented semantics ----------------------------------------------------

def test_same_bar_collision_uses_the_benchmark_conservative_priority():
    for name in ("BTC_ST_09_5M", "LIT_SUPERTREND_15M"):
        mgr = ProtectiveExitManager(None, manifest(name), SYMBOL)
        # Both strategies test the stop before the take profit on the same bar.
        assert mgr.same_bar_priority == "STOP"
        assert mgr.describe()["trigger_resolution"] == "candle_close"


def test_zec_declares_no_protective_levels():
    mgr = ProtectiveExitManager(None, manifest("ZEC_MOMENTUM_M03_15M"), "ZECUSDT")
    assert mgr.has_protective_levels is False
    assert mgr.same_bar_priority is None


def test_native_protection_is_refused_because_it_changes_semantics():
    for name in ("BTC_ST_09_5M", "LIT_SUPERTREND_15M", "ZEC_MOMENTUM_M03_15M"):
        mgr = ProtectiveExitManager(None, manifest(name), SYMBOL)
        assert mgr.native_equivalent is False
        assert mgr.describe()["managed_by"] == "STRATEGY"


def test_multiplier_parsing_matches_the_frozen_models():
    assert parse_multiplier("1.5_atr") == Decimal("1.5")
    assert parse_multiplier("bracket_3.0_atr7") == Decimal("3.0")
    assert parse_multiplier("strategy_flip") is None
    assert parse_multiplier(None) is None


# --- lifecycle and restart ---------------------------------------------------

def test_entry_records_levels_linked_to_the_entry_order(manager):
    mgr, store, health = manager
    state = mgr.on_entry_filled(_order(), Decimal("100"), atr=2.0, signal_id="SIG-IN")

    assert Decimal(state["stop_price"]) == Decimal("97")           # 100 - 1.5 * 2
    assert Decimal(state["take_profit_price"]) == Decimal("106")   # 100 + 3.0 * 2
    assert state["entry_client_order_id"] == "ESC-IN"
    assert state["signal_id"] == "SIG-IN"
    assert store.get_events_by_type("PROTECTIVE_STATE_CREATED")


def test_replacement_and_trigger_are_both_persisted(manager):
    mgr, store, _ = manager
    mgr.on_entry_filled(_order(), Decimal("100"), atr=2.0)
    mgr.on_replaced({"stop_price": "98.0"})
    mgr.on_exit_filled(_order("ESC-OUT", side=OrderSide.SELL), reason="SL")

    assert store.get_events_by_type("PROTECTIVE_STATE_REPLACED")
    triggered = store.get_events_by_type("PROTECTIVE_STATE_TRIGGERED")
    assert triggered[-1]["payload"]["exit_reason"] == "SL"
    assert triggered[-1]["payload"]["entry_client_order_id"] == "ESC-IN"
    assert mgr.restore() is None


def test_restart_with_open_position_restores_protective_state(manager, tmp_path):
    mgr, store, health = manager
    mgr.on_entry_filled(_order(), Decimal("100"), atr=2.0)

    restarted = ProtectiveExitManager(store, manifest("BTC_ST_09_5M"), SYMBOL,
                                      health=HealthMonitor(event_store=store))
    report = restarted.reconcile(open_position=True, position_quantity=Decimal("1"))
    assert report["has_protective_state"] is True
    assert Decimal(report["restored"]["stop_price"]) == Decimal("97")
    assert report["issues"] == []


def test_orphaned_protective_state_is_detected_and_cleared(manager):
    mgr, store, _ = manager
    mgr.on_entry_filled(_order(), Decimal("100"), atr=2.0)
    report = mgr.reconcile(open_position=False)
    assert "ORPHANED_PROTECTIVE_STATE" in report["issues"]
    assert mgr.restore() is None


def test_missing_protective_state_after_restart_is_reported(manager):
    mgr, store, _ = manager
    report = mgr.reconcile(open_position=True, position_quantity=Decimal("1"))
    assert "MISSING_PROTECTIVE_STATE" in report["issues"]


def test_open_position_is_reported_as_an_unprotected_interval(manager):
    mgr, store, health = manager
    mgr.on_entry_filled(_order(), Decimal("100"), atr=2.0)
    assert mgr.is_unprotected(open_position=True) is True
    assert health._beats["protective_state"]["status"] == "UNPROTECTED"

    mgr.on_exit_filled(_order("ESC-OUT", side=OrderSide.SELL), reason="TP")
    assert mgr.is_unprotected(open_position=False) is False


# --- execution price ---------------------------------------------------------

def test_paper_does_not_receive_a_retrospective_perfect_fill():
    """The stop level is the benchmark reference; PAPER fills at the market price."""
    from live_engine.orchestrator import LiveEngineOrchestrator

    class Stub:
        class config:
            mode = "PAPER"

    price = LiveEngineOrchestrator._executable_exit_price(Stub(), Decimal("97"), Decimal("95.5"))
    assert price == Decimal("95.5")

    class ShadowStub:
        class config:
            mode = "SHADOW"

    # SHADOW proves decision parity against the benchmark and keeps its reference.
    assert LiveEngineOrchestrator._executable_exit_price(ShadowStub(), Decimal("97"), Decimal("95.5")) == Decimal("97")


def test_protective_exit_cannot_reverse_a_position():
    """Exits stay reduce-only through the shared path regardless of protective state."""
    from live_engine.execution.position_manager import PositionManager

    pm = PositionManager(SYMBOL)
    pm.on_fill(OrderSide.BUY, Decimal("1"), Decimal("100"))
    pm.on_fill(OrderSide.SELL, Decimal("4"), Decimal("97"), reduce_only=True)
    assert pm.is_flat and pm.quantity == Decimal("0")
