"""Protective-exit state, semantics and failure safety.

**Documented benchmark semantics** (read out of the frozen strategy sources, not invented):

``BTC_ST_09_5M`` - on each closed 5m bar while in position: stop at
``entry_close - 1.5 * ATR(10)`` checked against that bar's ``low``, take profit at
``entry_close + 3.0 * ATR(10)`` checked against that bar's ``high``, then a bearish
Supertrend flip at ``close``. Same-bar collision resolves **stop first** (the strategy
tests ``low <= stop`` before ``high >= target``). Exit price is the stop/TP level itself.

``LIT_SUPERTREND_15M`` - entry fills at the next bar open; stop at
``entry_open - 1.5 * ATR(7)``, take profit at ``entry_open + 3.0 * ATR(7)``, scanned bar
by bar on ``high``/``low``; same-bar collision resolves **stop first** (explicitly
pessimistic in the strategy); a TIME exit fires at the open of bar ``entry + 128``. No
flip exit (``flip_exit = False``).

``ZEC_MOMENTUM_M03_15M`` - no stop and no take profit. The only exit is the opposing EMA
cross (strategy flip), filled at the next bar open.

**Why protection stays strategy-managed.** All three triggers are evaluated at *candle
close* against that bar's high/low, with a deterministic stop-first tie-break. A Binance
``STOP_MARKET`` triggers intrabar at first touch of the mark price, so it would fire at a
different time and would resolve a same-bar collision by whichever level was touched
first. That is not semantically equivalent, so native protective orders are not used to
drive these exits. The consequence - an interval where the exchange holds no protective
order - is surfaced as an explicit health condition instead of being hidden.
"""
from __future__ import annotations

import logging
import re
import time
from decimal import Decimal
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

STATE_KEY = "protective_state"

# Per-benchmark trigger semantics, keyed by the manifest's stop_model.
SAME_BAR_PRIORITY = {"stop_first": "STOP", "not_applicable": None}

_MULTIPLIER = re.compile(r"(\d+(?:\.\d+)?)_atr", re.IGNORECASE)


def parse_multiplier(model: Optional[str]) -> Optional[Decimal]:
    """Extracts the ATR multiplier from a manifest model string, if it declares one."""
    if not model:
        return None
    match = _MULTIPLIER.search(str(model))
    return Decimal(match.group(1)) if match else None


class ProtectiveExitManager:
    """Tracks protective levels, their lifecycle and the unprotected interval.

    The approved strategies remain the only authority on when a protective exit fires.
    This manager records the levels and their linkage to the entry order, persists every
    lifecycle transition, reconciles orphans after a restart, and reports when a position
    is held without exchange-side protection.
    """

    def __init__(self, event_store, manifest: Dict[str, Any], symbol: str,
                 health: Optional[Any] = None, broker: Optional[Any] = None):
        self.event_store = event_store
        self.manifest = manifest
        self.symbol = symbol
        self.health = health
        self.broker = broker

        execution = manifest.get("execution_model", {}) or {}
        self.stop_model = execution.get("stop_model")
        self.take_profit_model = execution.get("take_profit_model")
        self.same_bar_priority = SAME_BAR_PRIORITY.get(execution.get("intrabar_priority"), "STOP")
        self.time_exit_bars = execution.get("time_exit_bars")
        self.stop_multiplier = parse_multiplier(self.stop_model)
        self.tp_multiplier = parse_multiplier(self.take_profit_model)

    # -- semantics ----------------------------------------------------------

    @property
    def has_protective_levels(self) -> bool:
        return self.stop_multiplier is not None

    @property
    def native_equivalent(self) -> bool:
        """Whether an exchange-native protective order would preserve benchmark semantics.

        False for every approved benchmark: candle-close evaluation with a stop-first
        tie-break has no equivalent in an intrabar first-touch STOP_MARKET.
        """
        return False

    def describe(self) -> Dict[str, Any]:
        return {
            "benchmark_id": self.manifest.get("benchmark_id"),
            "stop_model": self.stop_model,
            "take_profit_model": self.take_profit_model,
            "same_bar_priority": self.same_bar_priority,
            "time_exit_bars": self.time_exit_bars,
            "trigger_resolution": "candle_close",
            "managed_by": "STRATEGY",
            "exchange_native_equivalent": self.native_equivalent,
        }

    # -- lifecycle ----------------------------------------------------------

    def on_entry_filled(self, entry_order, entry_price: Decimal,
                        atr: Optional[float], signal_id: Optional[str] = None) -> Dict[str, Any]:
        """Records protective levels for a freshly opened position.

        Levels are derived from the manifest's declared multipliers and the ATR snapshot
        the signal carried. They are recorded for failure-safety and audit; they never
        trigger an exit, because the approved strategy owns that decision.
        """
        state: Dict[str, Any] = {
            "symbol": self.symbol,
            "entry_client_order_id": entry_order.client_order_id,
            "entry_exchange_order_id": entry_order.exchange_order_id,
            "signal_id": signal_id or entry_order.signal_id,
            "entry_price": str(entry_price),
            "quantity": str(entry_order.filled_quantity),
            "stop_price": None,
            "take_profit_price": None,
            "managed_by": "STRATEGY",
            "exchange_protective_order_id": None,
            "created_at": int(time.time() * 1000),
        }
        if self.has_protective_levels and atr:
            atr_dec = Decimal(str(atr))
            state["stop_price"] = str(entry_price - self.stop_multiplier * atr_dec)
            if self.tp_multiplier is not None:
                state["take_profit_price"] = str(entry_price + self.tp_multiplier * atr_dec)
            state["atr_at_entry"] = str(atr_dec)

        self._save(state)
        self._record("PROTECTIVE_STATE_CREATED", state)
        self._update_health(open_position=True)
        return state

    def on_replaced(self, changes: Dict[str, Any]) -> Dict[str, Any]:
        """Records a change to the protective levels of the open position."""
        state = self.restore() or {}
        before = dict(state)
        state.update(changes)
        state["replaced_at"] = int(time.time() * 1000)
        self._save(state)
        self._record("PROTECTIVE_STATE_REPLACED", {"before": before, "after": state})
        return state

    def on_exit_filled(self, exit_order, reason: str) -> None:
        """Closes protective state once the position is out."""
        state = self.restore() or {}
        payload = {
            **state,
            "exit_client_order_id": exit_order.client_order_id,
            "exit_exchange_order_id": exit_order.exchange_order_id,
            "exit_reason": reason,
            "exit_filled_quantity": str(exit_order.filled_quantity),
            "resolved_at": int(time.time() * 1000),
        }
        self._record("PROTECTIVE_STATE_TRIGGERED", payload)
        self._save(None)
        self._update_health(open_position=False)

    def on_cancelled(self, reason: str) -> None:
        state = self.restore() or {}
        self._record("PROTECTIVE_STATE_CANCELLED", {**state, "reason": reason})
        self._save(None)
        self._update_health(open_position=False)

    # -- restart / reconciliation -------------------------------------------

    def restore(self) -> Optional[Dict[str, Any]]:
        if self.event_store is None:
            return None
        return self.event_store.get_state(STATE_KEY)

    def reconcile(self, open_position: bool, position_quantity: Decimal = Decimal("0")) -> Dict[str, Any]:
        """Detects protective state orphaned or missing after a restart."""
        state = self.restore()
        result: Dict[str, Any] = {
            "symbol": self.symbol,
            "open_position": open_position,
            "has_protective_state": state is not None,
            "issues": [],
        }

        if open_position and state is None:
            result["issues"].append("MISSING_PROTECTIVE_STATE")
            self._incident(
                "PROTECTIVE_STATE_MISSING", "HIGH",
                f"Open {self.symbol} position of {position_quantity} has no recorded protective "
                f"state after restart; strategy-managed exits resume but the entry linkage is lost.",
            )
        elif not open_position and state is not None:
            result["issues"].append("ORPHANED_PROTECTIVE_STATE")
            self._incident(
                "PROTECTIVE_STATE_ORPHANED", "MEDIUM",
                f"Protective state for {self.symbol} survives with no open position; clearing it.",
            )
            self.on_cancelled("orphaned after restart: position is flat")
        elif open_position and state is not None:
            result["restored"] = state

        self._update_health(open_position)
        result["unprotected"] = self.is_unprotected(open_position)
        self._record("PROTECTIVE_RECONCILIATION", result)
        return result

    # -- health -------------------------------------------------------------

    def is_unprotected(self, open_position: bool) -> bool:
        """True while a position is held with no exchange-side protective order.

        Strategy-managed protection means the exchange holds nothing: if this process
        dies, the position is unprotected until it restarts. That is an explicit,
        reportable risk condition, not an implementation detail.
        """
        return bool(open_position and not self.native_equivalent)

    def _update_health(self, open_position: bool) -> None:
        if self.health is None:
            return
        self.health.beat(
            "protective_state",
            "UNPROTECTED" if self.is_unprotected(open_position) else "OK",
            self.stop_model or "none",
        )

    # -- persistence --------------------------------------------------------

    def _save(self, state: Optional[Dict[str, Any]]) -> None:
        if self.event_store is not None:
            self.event_store.set_state(STATE_KEY, state)

    def _record(self, event_type: str, payload: Dict[str, Any]) -> None:
        if self.event_store is None:
            return
        try:
            self.event_store.log_event(event_type, payload)
        except Exception as exc:
            logger.warning("Protective state event %s failed to persist: %s", event_type, exc)

    def _incident(self, category: str, severity: str, details: str) -> None:
        logger.warning("%s: %s", category, details)
        if self.event_store is not None:
            self.event_store.record_incident(category, severity, details)
