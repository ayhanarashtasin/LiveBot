"""Tests for system crash, restart, and state recovery."""
import pytest
from decimal import Decimal
from pathlib import Path

from live_engine.config import LiveEngineConfig
from live_engine.orchestrator import LiveEngineOrchestrator
from live_engine.execution.models import Order, OrderSide, OrderStatus, OrderType
from live_engine.execution.position_manager import PositionManager
from live_engine.execution.order_manager import OrderManager
from live_engine.execution.idempotency import generate_client_order_id
from live_engine.persistence.event_store import EventStore


def test_recovery_restart_with_position(tmp_path):
    db_path = tmp_path / "recovery_events.db"
    ks_path = tmp_path / ".recovery_ks"

    cfg = LiveEngineConfig(
        mode="PAPER",
        symbol="BTCUSDT",
        timeframe="5m",
        manifest_path="benchmarks/manifests/BTC_ST_09_5M.yaml",
        event_store_path=str(db_path),
        kill_switch_path=str(ks_path),
        warmup_candles=250,

    )

    # 1. First engine run: establish position
    engine1 = LiveEngineOrchestrator(cfg)
    engine1.initialize()
    # Simulate fill
    engine1.position_manager.on_fill(OrderSide.BUY, Decimal("0.5"), Decimal("61000.0"))
    engine1.event_store.log_event("POSITION_SNAPSHOT", engine1.position_manager.to_dict())
    assert engine1.position_manager.quantity == Decimal("0.5")

    # 2. Simulate process crash and restart (Engine 2)
    engine2 = LiveEngineOrchestrator(cfg)
    # Recover state from event store snapshot
    events = engine2.event_store.get_events_by_type("POSITION_SNAPSHOT")
    assert len(events) == 1
    last_pos = events[-1]["payload"]
    engine2.position_manager.sync_from_exchange(
        exchange_side=OrderSide(last_pos["side"]) if last_pos["side"] else None,
        exchange_qty=Decimal(last_pos["quantity"]),
        exchange_entry_price=Decimal(last_pos["entry_price"]),
    )

    # Verify state restored
    assert engine2.position_manager.is_long is True
    assert engine2.position_manager.quantity == Decimal("0.5")
    assert engine2.position_manager.entry_price == Decimal("61000.0")


def test_order_idempotency_recovery():
    # Verify exact same client order ID across restart/re-evaluation
    cid1 = generate_client_order_id("BTCUSDT", "5m", 1723818600000, "BUY")
    cid2 = generate_client_order_id("BTCUSDT", "5m", 1723818600000, "BUY")
    assert cid1 == cid2

    om = OrderManager()
    order = Order(
        client_order_id=cid1,
        symbol="BTCUSDT",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=Decimal("0.1"),
        status=OrderStatus.FILLED,
    )
    om.register_order(order)

    # Attempting to re-register the same client_order_id raises duplicate error
    with pytest.raises(ValueError, match="Duplicate client_order_id"):
        om.register_order(order)
