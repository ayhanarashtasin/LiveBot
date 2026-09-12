"""Authoritative equity, mark price and daily-loss accounting.

The old daily-loss guard summed local realized PnL with an unrealized figure the engine
never updated, so the circuit breaker could not fire on an open losing position and reset
itself on every restart.

This tracker instead:

- reads an authoritative equity source (Binance account state when authenticated, the
  simulated account otherwise) and a Binance mark price,
- tracks the freshness of both and refuses to answer with stale data,
- snapshots start-of-day equity at the UTC day boundary and persists it, so a restart
  resumes the same trading day rather than starting a fresh one,
- measures drawdown as ``start_of_day_equity - current_equity``.

Equity is taken from one source per mode, which is what prevents double counting:
realized PnL, unrealized PnL, commissions and funding are all already inside the
authoritative balance. Nothing is added on top of it.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

SNAPSHOT_KEY = "daily_equity_snapshot"
DAY_MS = 86_400_000

# Maximum age before a price or account reading may no longer justify new risk.
DEFAULT_MARK_MAX_AGE_S = 60.0
DEFAULT_ACCOUNT_MAX_AGE_S = 300.0


def utc_day_start_ms(now_ms: Optional[int] = None) -> int:
    """Start of the UTC trading day containing now_ms."""
    now_ms = int(now_ms if now_ms is not None else time.time() * 1000)
    return (now_ms // DAY_MS) * DAY_MS


def utc_day_key(now_ms: Optional[int] = None) -> str:
    return datetime.fromtimestamp(utc_day_start_ms(now_ms) / 1000, tz=timezone.utc).strftime("%Y-%m-%d")


@dataclass
class Reading:
    """A value with the moment it was observed."""

    value: Decimal
    observed_at_ms: int

    def age_s(self, now_ms: Optional[int] = None) -> float:
        now_ms = int(now_ms if now_ms is not None else time.time() * 1000)
        return max(0.0, (now_ms - self.observed_at_ms) / 1000.0)


class EquityTracker:
    """Tracks authoritative equity, mark price freshness and the daily loss limit."""

    def __init__(
        self,
        event_store,
        broker,
        position_manager,
        max_daily_drawdown_usd: Decimal = Decimal("2000.0"),
        mark_max_age_s: float = DEFAULT_MARK_MAX_AGE_S,
        account_max_age_s: float = DEFAULT_ACCOUNT_MAX_AGE_S,
        health: Optional[Any] = None,
        kill_switch: Optional[Any] = None,
    ):
        self.event_store = event_store
        self.broker = broker
        self.position_manager = position_manager
        self.max_daily_drawdown_usd = Decimal(str(max_daily_drawdown_usd))
        self.mark_max_age_s = mark_max_age_s
        self.account_max_age_s = account_max_age_s
        self.health = health
        self.kill_switch = kill_switch

        self.mark_price: Optional[Reading] = None
        self.account_equity: Optional[Reading] = None
        self.available_balance: Optional[Decimal] = None
        self.breaker_engaged = False

    # -- data ingestion ------------------------------------------------------

    def update_mark_price(self, price: Decimal, observed_at_ms: Optional[int] = None) -> Decimal:
        """Records a fresh mark price and re-marks the open position with it."""
        price = Decimal(str(price))
        self.mark_price = Reading(price, int(observed_at_ms or time.time() * 1000))
        self.position_manager.update_unrealized_pnl(price)
        if self.health is not None:
            self.health.beat("mark_price")
        return price

    def refresh(self, now_ms: Optional[int] = None) -> Dict[str, Any]:
        """Pulls mark price and authoritative equity from whichever source this mode has."""
        now_ms = int(now_ms if now_ms is not None else time.time() * 1000)
        result: Dict[str, Any] = {"mode": getattr(self.broker, "mode", "UNKNOWN")}

        get_mark = getattr(self.broker, "get_mark_price", None)
        if get_mark is not None:
            try:
                price, ts = get_mark(self.position_manager.symbol)
                if price > Decimal("0"):
                    self.update_mark_price(price, ts)
                    result["mark_price"] = str(price)
            except Exception as exc:
                result["mark_price_error"] = str(exc)

        equity = self._read_authoritative_equity(now_ms, result)
        if equity is not None:
            self.account_equity = Reading(equity, now_ms)
            result["equity"] = str(equity)
            if self.health is not None:
                self.health.beat("account_state")
        return result

    def _read_authoritative_equity(self, now_ms: int, result: Dict[str, Any]) -> Optional[Decimal]:
        """Single equity source per mode - never a sum of overlapping figures."""
        get_state = getattr(self.broker, "get_account_state", None)
        if get_state is not None:
            try:
                state = get_state()
                self.available_balance = state.get("available_balance")
                # Margin balance already contains realized PnL, unrealized PnL, commissions
                # and funding. Adding any of them again would double count.
                return Decimal(str(state["total_margin_balance"]))
            except Exception as exc:
                result["account_error"] = str(exc)
                return None

        try:
            balances = self.broker.get_account_balance()
        except Exception as exc:
            result["account_error"] = str(exc)
            return None
        wallet = Decimal(str(balances.get("USDT", "0")))
        self.available_balance = wallet
        # A simulated wallet holds free cash only: entry posts margin off-wallet and close
        # releases it. So an open position contributes its posted margin plus its
        # mark-to-market, and each is counted exactly once.
        pm = self.position_manager
        leverage = Decimal(str(getattr(self.broker, "leverage", 1) or 1))
        posted_margin = (Decimal(str(pm.entry_price)) * Decimal(str(pm.quantity))) / leverage
        return wallet + posted_margin + pm.unrealized_pnl

    # -- freshness -----------------------------------------------------------

    def mark_is_fresh(self, now_ms: Optional[int] = None) -> bool:
        return self.mark_price is not None and self.mark_price.age_s(now_ms) <= self.mark_max_age_s

    def account_is_fresh(self, now_ms: Optional[int] = None) -> bool:
        return self.account_equity is not None and self.account_equity.age_s(now_ms) <= self.account_max_age_s

    def staleness_reason(self, now_ms: Optional[int] = None) -> Optional[str]:
        """Why new risk must be refused right now, or None when data is current."""
        if self.mark_price is None:
            return "No mark price has been observed"
        if not self.mark_is_fresh(now_ms):
            return f"Mark price is stale by {self.mark_price.age_s(now_ms):.1f}s (max {self.mark_max_age_s}s)"
        if self.account_equity is None:
            return "No authoritative account equity has been observed"
        if not self.account_is_fresh(now_ms):
            return f"Account state is stale by {self.account_equity.age_s(now_ms):.1f}s (max {self.account_max_age_s}s)"
        return None

    # -- daily snapshot ------------------------------------------------------

    def ensure_daily_snapshot(self, now_ms: Optional[int] = None) -> Dict[str, Any]:
        """Returns the start-of-day equity snapshot, creating one at each UTC rollover."""
        now_ms = int(now_ms if now_ms is not None else time.time() * 1000)
        day = utc_day_key(now_ms)
        stored = self.event_store.get_state(SNAPSHOT_KEY) if self.event_store else None

        if stored and stored.get("day") == day:
            return stored

        equity = self.account_equity.value if self.account_equity else Decimal("0")
        snapshot = {
            "day": day,
            "day_start_ms": utc_day_start_ms(now_ms),
            "equity": str(equity),
            "created_at": now_ms,
            "breaker_engaged": False,
        }
        if self.event_store is not None:
            self.event_store.set_state(SNAPSHOT_KEY, snapshot)
            self.event_store.log_event("DAILY_EQUITY_SNAPSHOT", snapshot)
        self.breaker_engaged = False
        return snapshot

    def restore(self) -> Optional[Dict[str, Any]]:
        """Restores the daily snapshot and breaker state after a restart."""
        stored = self.event_store.get_state(SNAPSHOT_KEY) if self.event_store else None
        if stored:
            self.breaker_engaged = bool(stored.get("breaker_engaged", False))
        return stored

    def daily_drawdown(self, now_ms: Optional[int] = None) -> Optional[Decimal]:
        """Loss versus start-of-day equity. Positive means down on the day."""
        snapshot = self.ensure_daily_snapshot(now_ms)
        if self.account_equity is None:
            return None
        return Decimal(str(snapshot["equity"])) - self.account_equity.value

    def check_daily_loss(self, now_ms: Optional[int] = None) -> Optional[str]:
        """Engages the breaker at the configured threshold. Returns a block reason or None."""
        drawdown = self.daily_drawdown(now_ms)
        if drawdown is None:
            return "Daily drawdown unknown: no authoritative equity reading"
        if drawdown < self.max_daily_drawdown_usd:
            return None

        reason = (
            f"Daily loss limit reached: down {drawdown} USDT against the start-of-day "
            f"snapshot (limit {self.max_daily_drawdown_usd})."
        )
        if not self.breaker_engaged:
            self.breaker_engaged = True
            snapshot = self.ensure_daily_snapshot(now_ms)
            snapshot["breaker_engaged"] = True
            if self.event_store is not None:
                self.event_store.set_state(SNAPSHOT_KEY, snapshot)
                self.event_store.record_incident("DAILY_LOSS_LIMIT", "HIGH", reason)
            if self.kill_switch is not None:
                self.kill_switch.trigger(reason=reason, triggered_by="DAILY_LOSS_GUARD")
            logger.critical(reason)
        return reason

    # -- margin --------------------------------------------------------------

    def margin_shortfall(self, notional: Decimal, leverage: Decimal, fee: Decimal) -> Optional[str]:
        """Whether authoritative available margin covers this order."""
        if self.available_balance is None:
            return "Available margin unknown: no authoritative account reading"
        required = (notional / max(leverage, Decimal("1"))) + fee
        if self.available_balance < required:
            return (
                f"Insufficient available margin: {self.available_balance} < required {required} "
                f"(notional {notional} at {leverage}x plus fee {fee})"
            )
        return None

    def snapshot(self) -> Dict[str, Any]:
        return {
            "mark_price": str(self.mark_price.value) if self.mark_price else None,
            "mark_age_s": self.mark_price.age_s() if self.mark_price else None,
            "equity": str(self.account_equity.value) if self.account_equity else None,
            "equity_age_s": self.account_equity.age_s() if self.account_equity else None,
            "available_balance": str(self.available_balance) if self.available_balance is not None else None,
            "daily_drawdown": str(self.daily_drawdown()) if self.account_equity else None,
            "breaker_engaged": self.breaker_engaged,
            "max_daily_drawdown_usd": str(self.max_daily_drawdown_usd),
        }
