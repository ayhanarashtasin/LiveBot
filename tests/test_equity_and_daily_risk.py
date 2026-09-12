"""Section 8 regressions: equity, mark price and daily risk accounting.

The old guard summed local realized PnL with an unrealized value nothing updated, so an
open losing position could never trip the breaker and a restart reset the day.
"""
import time
from decimal import Decimal

import pytest

from live_engine.execution.models import OrderSide
from live_engine.execution.position_manager import PositionManager
from live_engine.persistence.event_store import EventStore
from live_engine.risk.equity import DAY_MS, EquityTracker, utc_day_key, utc_day_start_ms
from live_engine.risk.kill_switch import KillSwitch

SYMBOL = "BTCUSDT"


class FakeAccountBroker:
    """Authenticated broker stub with a scriptable account and mark price."""

    mode = "TESTNET"

    def __init__(self, margin_balance="10000", available="10000", mark="100"):
        self.margin_balance = Decimal(margin_balance)
        self.available = Decimal(available)
        self.mark = Decimal(mark)
        self.mark_ts = int(time.time() * 1000)

    def get_account_state(self):
        return {
            "total_margin_balance": self.margin_balance,
            "available_balance": self.available,
            "total_unrealized_pnl": Decimal("0"),
            "positions": [],
        }

    def get_mark_price(self, symbol):
        return self.mark, self.mark_ts

    def get_account_balance(self):
        return {"USDT": self.margin_balance}


@pytest.fixture
def rig(tmp_path):
    store = EventStore(tmp_path / "equity.db")
    pm = PositionManager(SYMBOL)
    broker = FakeAccountBroker()
    ks = KillSwitch(tmp_path / ".kill_switch")
    tracker = EquityTracker(store, broker, pm, max_daily_drawdown_usd=Decimal("500"), kill_switch=ks)
    return tracker, store, pm, broker, ks


# --- UTC day boundary --------------------------------------------------------

def test_utc_day_boundary_and_rollover_creates_one_snapshot(rig):
    tracker, store, _, _, _ = rig
    day0 = 1_700_000_000_000
    start = utc_day_start_ms(day0)
    assert start % DAY_MS == 0
    assert utc_day_key(start) == utc_day_key(start + DAY_MS - 1)

    tracker.refresh(now_ms=start)
    first = tracker.ensure_daily_snapshot(now_ms=start)
    same_day = tracker.ensure_daily_snapshot(now_ms=start + DAY_MS - 1)
    assert same_day == first

    tracker.refresh(now_ms=start + DAY_MS)
    rolled = tracker.ensure_daily_snapshot(now_ms=start + DAY_MS)
    assert rolled["day"] != first["day"]
    assert len(store.get_events_by_type("DAILY_EQUITY_SNAPSHOT")) == 2


def test_restart_does_not_reset_the_daily_loss_limit(rig, tmp_path):
    tracker, store, pm, broker, ks = rig
    now = utc_day_start_ms() + 3_600_000
    tracker.refresh(now_ms=now)
    tracker.ensure_daily_snapshot(now_ms=now)

    broker.margin_balance = Decimal("9400")  # down 600 against a 500 limit
    tracker.refresh(now_ms=now)
    assert tracker.check_daily_loss(now_ms=now) is not None
    assert tracker.breaker_engaged is True

    restarted = EquityTracker(store, broker, PositionManager(SYMBOL),
                              max_daily_drawdown_usd=Decimal("500"))
    snapshot = restarted.restore()
    assert snapshot["day"] == utc_day_key(now)
    assert restarted.breaker_engaged is True
    assert Decimal(snapshot["equity"]) == Decimal("10000")


def test_open_unrealized_loss_can_trigger_the_breaker(rig):
    tracker, store, pm, broker, ks = rig
    now = utc_day_start_ms() + 60_000
    tracker.refresh(now_ms=now)
    tracker.ensure_daily_snapshot(now_ms=now)

    # Position still open: the loss lives entirely in the account's margin balance.
    pm.on_fill(OrderSide.BUY, Decimal("10"), Decimal("100"))
    broker.margin_balance = Decimal("9300")
    tracker.refresh(now_ms=now)

    assert tracker.daily_drawdown(now_ms=now) == Decimal("700")
    reason = tracker.check_daily_loss(now_ms=now)
    assert reason is not None and "Daily loss limit" in reason
    assert ks.is_engaged() is True


def test_fees_and_funding_affect_equity_exactly_once(rig):
    """Equity comes from one authoritative source; nothing is added on top of it."""
    tracker, store, pm, broker, _ = rig
    now = utc_day_start_ms() + 60_000
    tracker.refresh(now_ms=now)
    tracker.ensure_daily_snapshot(now_ms=now)

    # The exchange has already netted commissions and funding into margin balance.
    broker.margin_balance = Decimal("9950")
    pm.total_fees_paid = Decimal("50")   # local bookkeeping only
    pm.realized_pnl = Decimal("-50")
    tracker.refresh(now_ms=now)

    assert tracker.daily_drawdown(now_ms=now) == Decimal("50")


# --- staleness and margin ----------------------------------------------------

def test_stale_mark_or_account_data_blocks_new_entries(rig):
    from live_engine.market_data.models import Candle
    from live_engine.risk.guards import RiskGuardEngine

    tracker, store, pm, broker, ks = rig
    now = utc_day_start_ms() + 60_000
    tracker.refresh(now_ms=now)

    guards = RiskGuardEngine(kill_switch=ks, equity_tracker=tracker,
                             require_fresh_market_data=True, filters=None)

    # Age the readings well past their limits.
    tracker.mark_price.observed_at_ms -= 10 * 60 * 1000
    tracker.account_equity.observed_at_ms -= 60 * 60 * 1000

    res = guards.evaluate_entry(SYMBOL, OrderSide.BUY, Decimal("1"), Decimal("100"),
                                pm, latest_candle=None)
    assert res.passed is False
    assert "stale" in res.reason.lower()


def test_insufficient_available_margin_blocks_entry(rig):
    from live_engine.risk.guards import RiskGuardEngine

    tracker, store, pm, broker, ks = rig
    broker.available = Decimal("10")
    tracker.refresh()

    guards = RiskGuardEngine(kill_switch=ks, equity_tracker=tracker,
                             require_fresh_market_data=True, filters=None,
                             leverage=Decimal("1"))
    res = guards.evaluate_entry(SYMBOL, OrderSide.BUY, Decimal("5"), Decimal("100"),
                                pm, latest_candle=None)
    assert res.passed is False
    assert "margin" in res.reason.lower()


def test_price_sanity_uses_an_independent_benchmark_not_the_same_candle_close(rig):
    from live_engine.risk.guards import RiskGuardEngine

    tracker, store, pm, broker, ks = rig
    broker.mark = Decimal("100")
    tracker.refresh()

    guards = RiskGuardEngine(kill_switch=ks, equity_tracker=tracker,
                             require_fresh_market_data=True, filters=None,
                             max_price_deviation_pct=Decimal("2.0"))

    # An order price 10% away from the live mark is refused, even though it equals the
    # candle close it was derived from.
    res = guards.evaluate_entry(SYMBOL, OrderSide.BUY, Decimal("1"), Decimal("110"),
                                pm, latest_candle=None)
    assert res.passed is False
    assert "deviation" in res.reason.lower()

    ok = guards.evaluate_entry(SYMBOL, OrderSide.BUY, Decimal("1"), Decimal("100.5"),
                               pm, latest_candle=None)
    assert ok.passed is True
