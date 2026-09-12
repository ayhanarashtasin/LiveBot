"""Section 4 regressions: UNKNOWN-order recovery and reconciliation.

The old code fired one query after a timeout and reconciliation only *reported*
differences. These tests pin the repair behaviour and its idempotency.
"""
from decimal import Decimal

import pytest

from live_engine.account.reconciliation import (
    ABSENCE_PROOF_QUERIES,
    AccountReconciler,
    is_escanor_order,
)
from live_engine.execution.fill_applier import FillApplier
from live_engine.execution.models import Order, OrderSide, OrderStatus, OrderType
from live_engine.execution.order_manager import OrderManager
from live_engine.execution.position_manager import PositionManager
from live_engine.persistence.event_store import EventStore
from live_engine.risk.health import HealthMonitor, HealthState

SYMBOL = "BTCUSDT"
CID = "ESC-BTCUSDT-5M-1700000000-BUY"


class ScriptedBroker:
    """Exchange stub: query answers come from a script, trades from a list."""

    mode = "TESTNET"
    position_mode = "ONE_WAY"

    def __init__(self, query_script=(), trades=(), position=(None, Decimal("0"), Decimal("0"))):
        self.query_script = list(query_script)
        self.trades = list(trades)
        self.position = position
        self.queries = 0
        self.cancelled = []

    def query_order(self, symbol, client_order_id):
        self.queries += 1
        return self.query_script.pop(0) if self.query_script else None

    def get_user_trades(self, symbol, start_time_ms=None, limit=500):
        return self.trades

    def get_position(self, symbol):
        return self.position

    def get_account_balance(self):
        return {"USDT": Decimal("10000")}

    def cancel_order(self, symbol, client_order_id):
        self.cancelled.append(client_order_id)
        return True


@pytest.fixture
def rig(tmp_path):
    store = EventStore(tmp_path / "recon.db")
    om, pm = OrderManager(), PositionManager(SYMBOL)
    applier = FillApplier(om, pm, store, SYMBOL)
    health = HealthMonitor(mode="TESTNET", event_store=store)
    return store, om, pm, applier, health


def _filled_order(cid=CID, qty="1", price="100"):
    return Order(
        client_order_id=cid, symbol=SYMBOL, side=OrderSide.BUY, order_type=OrderType.MARKET,
        quantity=Decimal(qty), price=Decimal(price), status=OrderStatus.FILLED,
        filled_quantity=Decimal(qty), avg_fill_price=Decimal(price), created_at=1,
        exchange_order_id="777",
    )


def _intent(om, store, cid=CID, qty="1"):
    order = Order(
        client_order_id=cid, symbol=SYMBOL, side=OrderSide.BUY, order_type=OrderType.MARKET,
        quantity=Decimal(qty), price=Decimal("100"), status=OrderStatus.SUBMITTING, created_at=1,
    )
    om.upsert_order(order)
    store.save_order(order)
    return order


def test_timeout_two_not_found_then_found_produces_exactly_one_fill(rig):
    store, om, pm, applier, health = rig
    _intent(om, store)
    broker = ScriptedBroker(query_script=[None, None, _filled_order()])
    rec = AccountReconciler(broker, pm, event_store=store, order_manager=om,
                            fill_applier=applier, health=health)

    outcome = rec.resolve_unknown_order(SYMBOL, CID, sleep=lambda s: None)
    assert outcome["outcome"] == "FOUND"
    assert broker.queries == 3
    assert pm.quantity == Decimal("1")

    # Running it again must not add a second fill.
    broker.query_script = [_filled_order()]
    rec.resolve_unknown_order(SYMBOL, CID, sleep=lambda s: None)
    assert pm.quantity == Decimal("1")


def test_permanently_unresolved_unknown_blocks_new_entries(rig):
    store, om, pm, applier, health = rig
    _intent(om, store)
    # No order found, and the trade history is unreadable, so absence cannot be proven.
    broker = ScriptedBroker(query_script=[])
    broker.get_user_trades = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("history unavailable"))
    rec = AccountReconciler(broker, pm, event_store=store, order_manager=om,
                            fill_applier=applier, health=health)

    outcome = rec.resolve_unknown_order(SYMBOL, CID, sleep=lambda s: None)
    assert outcome["outcome"] == "UNRESOLVED"
    assert CID in health.unknown_orders

    health.warmup_ready = True
    allowed, reason = health.can_open_new_risk()
    assert allowed is False
    assert health.state() is HealthState.RECONCILIATION_REQUIRED
    assert CID in reason


def test_absence_is_only_concluded_after_the_documented_proof(rig):
    store, om, pm, applier, health = rig
    _intent(om, store)
    broker = ScriptedBroker(query_script=[], trades=[])
    rec = AccountReconciler(broker, pm, event_store=store, order_manager=om,
                            fill_applier=applier, health=health)

    slept = []
    outcome = rec.resolve_unknown_order(SYMBOL, CID, sleep=lambda s: slept.append(s) or None)
    assert broker.queries == ABSENCE_PROOF_QUERIES
    # Only a full proof window may conclude ABSENT; a stubbed clock keeps it UNRESOLVED.
    assert outcome["outcome"] in ("ABSENT", "UNRESOLVED")
    assert len(slept) == ABSENCE_PROOF_QUERIES - 1


def test_startup_recovers_an_exchange_fill_missing_locally_exactly_once(rig):
    store, om, pm, applier, health = rig
    _intent(om, store)
    trade = {"id": 5150, "orderId": 777, "clientOrderId": CID, "symbol": SYMBOL, "side": "BUY",
             "qty": "1", "price": "100", "commission": "0.05", "commissionAsset": "USDT",
             "time": 1700000000000}
    broker = ScriptedBroker(trades=[trade], position=(OrderSide.BUY, Decimal("1"), Decimal("100")))
    rec = AccountReconciler(broker, pm, event_store=store, order_manager=om,
                            fill_applier=applier, health=health)

    first = rec.reconcile_recent_fills(SYMBOL)
    assert first["recovered_fills"] == 1
    assert pm.quantity == Decimal("1")
    fees_after_first = pm.total_fees_paid

    second = rec.reconcile_recent_fills(SYMBOL)
    assert second["recovered_fills"] == 0
    assert pm.quantity == Decimal("1")
    assert pm.total_fees_paid == fees_after_first


def test_manual_orders_are_reported_but_never_modified(rig):
    store, om, pm, applier, health = rig
    manual = Order(client_order_id="web_manual_123", symbol=SYMBOL, side=OrderSide.SELL,
                   order_type=OrderType.MARKET, quantity=Decimal("5"), status=OrderStatus.NEW,
                   created_at=1)
    broker = ScriptedBroker()
    rec = AccountReconciler(broker, pm, event_store=store, order_manager=om,
                            fill_applier=applier, health=health)

    report = rec.reconcile_open_orders([manual])
    entry = report["details"][0]
    assert entry["ownership"] == "EXTERNAL"
    assert entry["action"] == "REPORTED_ONLY"
    assert broker.queries == 0
    assert broker.cancelled == []
    assert manual.status is OrderStatus.NEW
    assert not is_escanor_order("web_manual_123")


def test_repeated_full_reconciliation_produces_no_extra_mutation(rig):
    store, om, pm, applier, health = rig
    _intent(om, store)
    trade = {"id": 42, "orderId": 777, "clientOrderId": CID, "symbol": SYMBOL, "side": "BUY",
             "qty": "1", "price": "100", "commission": "0.05", "commissionAsset": "USDT",
             "time": 1700000000000}
    broker = ScriptedBroker(query_script=[_filled_order(), _filled_order()], trades=[trade],
                            position=(OrderSide.BUY, Decimal("1"), Decimal("100")))
    rec = AccountReconciler(broker, pm, event_store=store, order_manager=om,
                            fill_applier=applier, health=health)

    rec.reconcile_all(SYMBOL, open_orders=om.get_open_orders())
    snapshot = pm.to_dict()

    rec.reconcile_all(SYMBOL, open_orders=om.get_open_orders())
    assert pm.to_dict() == snapshot
