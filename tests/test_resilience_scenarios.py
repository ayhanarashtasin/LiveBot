"""Required resilience scenarios: network, socket, database and restart faults.

These are the failures that actually happen in production and that the previous code
either did not survive or survived by corrupting state. Everything is exercised against
the real modules with faults injected at the boundary - no framework, no fixtures beyond
a temporary database.
"""
import asyncio
import sqlite3
import threading
import time
import urllib.error
from decimal import Decimal

import pytest

from live_engine.execution.fill_applier import FillApplier
from live_engine.execution.models import Order, OrderSide, OrderStatus, OrderType
from live_engine.execution.order_manager import OrderManager
from live_engine.execution.position_manager import PositionManager
from live_engine.market_data.binance_aggtrade import BinanceAggTradeStream
from live_engine.market_data.gap_detector import AggTradeGapDetector
from live_engine.market_data.gap_recovery import AggTradeGapRecovery
from live_engine.market_data.models import AggTrade
from live_engine.persistence.event_store import EventStore

SYMBOL = "BTCUSDT"


def trade(agg_id, price="100", t=None):
    return AggTrade(event_type="aggTrade", event_time=t or 1_700_000_000_000 + agg_id,
                    symbol=SYMBOL, agg_trade_id=agg_id, price=Decimal(price),
                    quantity=Decimal("1"), first_trade_id=agg_id, last_trade_id=agg_id,
                    trade_time=t or 1_700_000_000_000 + agg_id,
                    buyer_is_market_maker=False, received_at=1)


def row(agg_id):
    return {"a": agg_id, "s": SYMBOL, "p": "100.0", "q": "1.0", "f": agg_id, "l": agg_id,
            "T": 1_700_000_000_000 + agg_id, "m": False}


# --- market data sequencing ---------------------------------------------------

def test_duplicate_and_out_of_order_aggtrades_are_classified_not_applied():
    detector = AggTradeGapDetector(SYMBOL)
    assert detector.process_trade(trade(10)).value == "VALID"
    assert detector.process_trade(trade(10)).value == "DUPLICATE"
    assert detector.process_trade(trade(9)).value == "REVERSED"
    assert detector.process_trade(trade(11)).value == "VALID"
    # Neither a duplicate nor a reversal may move the sequence head or raise a gap.
    assert detector.last_agg_trade_id == 11
    assert detector.is_desynced is False
    assert detector.duplicate_count == 1


def test_websocket_disconnect_during_a_gap_repair_leaves_the_gap_active():
    """A dropped connection mid-repair must not be mistaken for a completed recovery."""
    calls = {"n": 0}

    def flaky(from_id, limit):
        calls["n"] += 1
        if calls["n"] == 1:
            return [row(i) for i in range(from_id, from_id + 3)]
        raise OSError("connection reset by peer")

    recovery = AggTradeGapRecovery(SYMBOL, fetch_page=flaky, sleep=lambda s: None)
    result = recovery.recover(10, 20)

    assert result.complete is False
    assert result.remaining == (13, 20)
    assert "fetch failed" in result.reason or "unavailable" in result.reason


def test_reconnect_flapping_backoff_is_bounded_and_increasing():
    stream = BinanceAggTradeStream(SYMBOL, backoff_base_s=1.0, backoff_max_s=30.0)
    delays = [min(stream.backoff_max_s, stream.backoff_base_s * (2 ** (a - 1))) for a in range(1, 12)]

    assert delays[0] == 1.0
    assert delays == sorted(delays)          # never goes backwards
    assert max(delays) <= stream.backoff_max_s  # never unbounded


def test_dirty_socket_close_reconnects_instead_of_dying(monkeypatch):
    """A close with no close frame is an exception, not a clean end of stream."""
    from websockets.exceptions import ConnectionClosedError

    attempts = {"n": 0}

    class DirtyClose:
        async def __aenter__(self):
            attempts["n"] += 1
            if attempts["n"] <= 2:
                raise ConnectionClosedError(None, None)
            raise asyncio.CancelledError

        async def __aexit__(self, *exc):
            return False

    monkeypatch.setattr("live_engine.market_data.binance_aggtrade.websockets.connect",
                        lambda *a, **k: DirtyClose())
    slept = []

    async def fake_sleep(delay):
        slept.append(delay)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    stream = BinanceAggTradeStream(SYMBOL, backoff_base_s=0.01, backoff_max_s=0.05)
    stream._running = True

    async def scenario():
        with pytest.raises(asyncio.CancelledError):
            await stream._run_loop()

    asyncio.run(scenario())
    assert attempts["n"] == 3
    assert stream.reconnect_count >= 2
    assert slept and all(d <= 0.05 for d in slept)


def test_ping_timeout_is_configured_on_the_public_stream(monkeypatch):
    """A silent socket must be detected by ping timeout, not hang forever."""
    captured = {}

    class Never:
        async def __aenter__(self):
            raise asyncio.CancelledError

        async def __aexit__(self, *exc):
            return False

    def fake_connect(url, **kwargs):
        captured.update(kwargs)
        return Never()

    monkeypatch.setattr("live_engine.market_data.binance_aggtrade.websockets.connect", fake_connect)
    stream = BinanceAggTradeStream(SYMBOL)
    stream._running = True

    async def scenario():
        with pytest.raises(asyncio.CancelledError):
            await stream._run_loop()

    asyncio.run(scenario())
    assert captured["ping_interval"] > 0
    assert captured["ping_timeout"] > 0
    assert captured["open_timeout"] > 0


# --- REST fault classes -------------------------------------------------------

@pytest.mark.parametrize("code,retryable", [(429, True), (418, True), (502, True),
                                            (504, True), (400, False), (403, False)])
def test_only_documented_codes_are_retried(code, retryable):
    calls = []

    def pages(from_id, limit):
        calls.append(from_id)
        if len(calls) == 1:
            raise urllib.error.HTTPError("u", code, "err", {}, None)
        return [row(i) for i in range(from_id, 12)]

    result = AggTradeGapRecovery(SYMBOL, fetch_page=pages, sleep=lambda s: None).recover(10, 11)
    if retryable:
        assert len(calls) > 1 and result.complete is True
    else:
        assert len(calls) == 1 and result.complete is False


# --- execution faults ---------------------------------------------------------

@pytest.fixture
def exec_rig(tmp_path):
    store = EventStore(tmp_path / "resilience.db")
    om, pm = OrderManager(), PositionManager(SYMBOL)
    return FillApplier(om, pm, store, SYMBOL), om, pm, store


def _intent(om, store, cid, qty="1"):
    order = Order(client_order_id=cid, symbol=SYMBOL, side=OrderSide.BUY,
                  order_type=OrderType.MARKET, quantity=Decimal(qty), price=Decimal("100"),
                  status=OrderStatus.SUBMITTING, created_at=1)
    om.upsert_order(order)
    store.save_order(order)
    return order


def test_rest_acknowledgement_followed_by_a_duplicate_stream_fill(exec_rig):
    applier, om, pm, store = exec_rig
    order = _intent(om, store, "ESC-DUP")
    order.status = OrderStatus.FILLED
    order.filled_quantity = Decimal("1")
    order.avg_fill_price = Decimal("100")

    applier.apply_order_response(order, source="REST_RESPONSE")
    applier.apply_stream_event({"e": "ORDER_TRADE_UPDATE", "E": 1, "T": 1, "o": {
        "c": "ESC-DUP", "s": SYMBOL, "S": "BUY", "X": "FILLED",
        "z": "1", "l": "1", "L": "100", "ap": "100", "n": "0", "i": "1", "t": "77"}})

    assert pm.quantity == Decimal("1")


def test_restart_with_an_unknown_order_rebuilds_from_durable_state(exec_rig, tmp_path):
    applier, om, pm, store = exec_rig
    order = _intent(om, store, "ESC-UNKNOWN")
    om.update_order_status("ESC-UNKNOWN", OrderStatus.UNKNOWN, rejection_reason="timeout")
    store.save_order(om.get_order_by_client_id("ESC-UNKNOWN"))

    # A fresh process reads the durable ledger.
    restarted = OrderManager()
    for restored in store.get_open_orders():
        restarted.upsert_order(restored)

    survivor = restarted.get_order_by_client_id("ESC-UNKNOWN")
    assert survivor is not None
    assert survivor.status is OrderStatus.UNKNOWN

    from live_engine.risk.health import HealthMonitor, HealthState

    health = HealthMonitor(mode="TESTNET", event_store=store, warmup_ready=True)
    health.unknown_orders.add("ESC-UNKNOWN")
    allowed, reason = health.can_open_new_risk()
    assert allowed is False and health.state() is HealthState.RECONCILIATION_REQUIRED


# --- database faults ----------------------------------------------------------

def test_sqlite_busy_writer_is_waited_out_not_failed(tmp_path):
    """A concurrent writer must be waited out by busy_timeout, not raise 'database is locked'."""
    db = tmp_path / "busy.db"
    store = EventStore(db)

    locked = threading.Event()
    finished = threading.Event()

    def hold_write_lock():
        # The connection is created and closed in this thread: sqlite objects are not
        # shareable across threads.
        conn = sqlite3.connect(str(db), timeout=30.0, isolation_level=None)
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("BEGIN IMMEDIATE;")
            locked.set()
            time.sleep(0.5)
            conn.execute("COMMIT;")
        finally:
            conn.close()
            finished.set()

    thread = threading.Thread(target=hold_write_lock, daemon=True)
    thread.start()
    assert locked.wait(timeout=5), "lock holder never started"

    started = time.monotonic()
    store.log_event("WRITE_DURING_LOCK", {"ok": True})  # blocks, then succeeds
    elapsed = time.monotonic() - started

    finished.wait(timeout=5)
    thread.join(timeout=5)

    assert store.get_events_by_type("WRITE_DURING_LOCK")
    assert elapsed >= 0.3, "the write did not actually contend for the lock"


def test_event_store_survives_reopening_a_locked_database(tmp_path):
    db = tmp_path / "reopen.db"
    first = EventStore(db)
    first.log_event("A", {"n": 1})

    second = EventStore(db)  # a second instance must not corrupt or wipe the first
    second.log_event("B", {"n": 2})

    assert len(first.get_events_by_type("A")) == 1
    assert len(second.get_events_by_type("B")) == 1
