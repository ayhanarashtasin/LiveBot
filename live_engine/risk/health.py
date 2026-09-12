"""Runtime health state, component heartbeats and incident alerting.

One object owns "is this engine allowed to take new risk right now". Heartbeats are
persisted so a supervisor (or a restarted process) can tell a crashed component from a
quiet one, and every condition that must block new entries is an explicit flag rather
than a value scattered across subsystems.

Alerts go to a local structured sink by default. A generic webhook is used only when
``ESCANOR_ALERT_WEBHOOK_URL`` is set; credentials never come from config files and no
alert body ever carries a key, signature or listen key.
"""
from __future__ import annotations

import json
import logging
import os
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class HealthState(str, Enum):
    STARTING = "STARTING"
    WARMING_UP = "WARMING_UP"
    SYNCHRONIZING = "SYNCHRONIZING"
    READY = "READY"
    DEGRADED = "DEGRADED"
    DATA_DESYNCED = "DATA_DESYNCED"
    EXCHANGE_DISCONNECTED = "EXCHANGE_DISCONNECTED"
    RECONCILIATION_REQUIRED = "RECONCILIATION_REQUIRED"
    CONFIGURATION_MISMATCH = "CONFIGURATION_MISMATCH"
    KILLED = "KILLED"


# Component -> maximum age in seconds before its heartbeat counts as stale.
DEFAULT_FRESHNESS_S: Dict[str, float] = {
    "engine": 120.0,
    "public_stream": 120.0,
    "kline_validator": 300.0,
    "private_stream": 120.0,
    "reconciliation": 900.0,
    "mark_price": 120.0,
    "account_state": 900.0,
    "dashboard_projection": 300.0,
}

# Conditions that must never coexist with new strategy entries.
BLOCKING_STATES = {
    HealthState.STARTING,
    HealthState.WARMING_UP,
    HealthState.SYNCHRONIZING,
    HealthState.DATA_DESYNCED,
    HealthState.EXCHANGE_DISCONNECTED,
    HealthState.RECONCILIATION_REQUIRED,
    HealthState.CONFIGURATION_MISMATCH,
    HealthState.KILLED,
}

# Modes whose private execution events come from the exchange. SHADOW/PAPER have none.
AUTHENTICATED_MODES = ("LIVE", "TESTNET")


@dataclass
class HealthMonitor:
    """Tracks component freshness and derives the runtime health state."""

    mode: str = "SHADOW"
    event_store: Optional[Any] = None
    freshness_s: Dict[str, float] = field(default_factory=lambda: dict(DEFAULT_FRESHNESS_S))

    warmup_ready: bool = False
    data_desynced: bool = False
    reconciliation_required: bool = False
    configuration_mismatch: bool = False
    killed: bool = False
    unknown_orders: set = field(default_factory=set)
    history_gap_open: bool = False

    _beats: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    def beat(self, component: str, status: str = "OK", details: str = "") -> None:
        """Records a liveness heartbeat for one component (in memory and durably)."""
        now_ms = int(time.time() * 1000)
        self._beats[component] = {"last_beat_ms": now_ms, "status": status, "details": details}
        if self.event_store is not None:
            try:
                self.event_store.record_heartbeat(component, status, details)
            except Exception as exc:  # persistence must never take the engine down
                logger.warning("Heartbeat persistence failed for %s: %s", component, exc)

    def age_s(self, component: str) -> Optional[float]:
        """Seconds since the last heartbeat, or None if the component never reported."""
        beat = self._beats.get(component)
        if beat is None and self.event_store is not None:
            beat = self.event_store.get_heartbeats().get(component)
        if not beat:
            return None
        return max(0.0, (time.time() * 1000 - float(beat["last_beat_ms"])) / 1000.0)

    def is_fresh(self, component: str) -> bool:
        age = self.age_s(component)
        if age is None:
            return False
        return age <= self.freshness_s.get(component, 300.0)

    def requires_private_stream(self) -> bool:
        return self.mode.upper() in AUTHENTICATED_MODES

    def stale_components(self) -> List[str]:
        """Components that are required in this mode and are not reporting fresh."""
        required = ["engine", "public_stream"]
        if self.requires_private_stream():
            required += ["private_stream", "mark_price", "account_state", "reconciliation"]
        return [c for c in required if not self.is_fresh(c)]

    def state(self) -> HealthState:
        """Current runtime state, worst condition first."""
        if self.killed:
            return HealthState.KILLED
        if self.configuration_mismatch:
            return HealthState.CONFIGURATION_MISMATCH
        if self.reconciliation_required or self.unknown_orders:
            return HealthState.RECONCILIATION_REQUIRED
        if self.data_desynced or self.history_gap_open:
            return HealthState.DATA_DESYNCED
        if not self.warmup_ready:
            return HealthState.WARMING_UP
        stale = self.stale_components()
        if stale:
            # A dead private execution feed on an authenticated account is a disconnect,
            # not mere degradation: fills would be invisible.
            if self.requires_private_stream() and "private_stream" in stale:
                return HealthState.EXCHANGE_DISCONNECTED
            return HealthState.DEGRADED
        return HealthState.READY

    def can_open_new_risk(self) -> tuple[bool, str]:
        """Whether a new strategy entry may be submitted right now."""
        st = self.state()
        if st in BLOCKING_STATES:
            detail = ""
            if st == HealthState.RECONCILIATION_REQUIRED and self.unknown_orders:
                detail = f" (unresolved UNKNOWN orders: {sorted(self.unknown_orders)})"
            elif st in (HealthState.DEGRADED, HealthState.EXCHANGE_DISCONNECTED):
                detail = f" (stale: {self.stale_components()})"
            return False, f"Health state is {st.value}; new entries blocked{detail}"
        if st == HealthState.DEGRADED:
            return False, f"Health state is DEGRADED (stale: {self.stale_components()})"
        return True, "READY"

    def snapshot(self) -> Dict[str, Any]:
        return {
            "state": self.state().value,
            "mode": self.mode,
            "warmup_ready": self.warmup_ready,
            "data_desynced": self.data_desynced,
            "history_gap_open": self.history_gap_open,
            "reconciliation_required": self.reconciliation_required,
            "configuration_mismatch": self.configuration_mismatch,
            "killed": self.killed,
            "unknown_orders": sorted(self.unknown_orders),
            "stale_components": self.stale_components(),
            "component_age_s": {c: self.age_s(c) for c in self.freshness_s},
        }


# Fields that must never leave the process inside an alert body.
_SECRET_HINTS = ("key", "secret", "signature", "token", "password", "listenkey", "listen_key")


def scrub(payload: Any) -> Any:
    """Removes anything that looks like a credential from an alert payload."""
    if isinstance(payload, dict):
        return {
            k: ("[REDACTED]" if any(h in str(k).lower() for h in _SECRET_HINTS) else scrub(v))
            for k, v in payload.items()
        }
    if isinstance(payload, list):
        return [scrub(v) for v in payload]
    return payload


class IncidentAlerter:
    """Deduplicated incident alerting with a local sink and an optional webhook."""

    def __init__(
        self,
        event_store: Optional[Any] = None,
        sink_path: Optional[Path] = None,
        dedup_window_s: float = 300.0,
        webhook_url: Optional[str] = None,
    ):
        self.event_store = event_store
        self.sink_path = Path(sink_path) if sink_path else None
        self.dedup_window_s = dedup_window_s
        # Credentials come only from the environment, never from a config file.
        self.webhook_url = webhook_url or os.environ.get("ESCANOR_ALERT_WEBHOOK_URL") or None
        self._last_sent: Dict[str, float] = {}
        self._active: Dict[str, Dict[str, Any]] = {}

    def alert(self, category: str, severity: str, message: str, context: Optional[Dict[str, Any]] = None) -> bool:
        """Raises an incident. Returns False when suppressed as a duplicate."""
        now = time.time()
        last = self._last_sent.get(category)
        if last is not None and (now - last) < self.dedup_window_s:
            return False
        self._last_sent[category] = now
        self._active[category] = {"since": now, "message": message}
        self._emit({
            "kind": "INCIDENT",
            "category": category,
            "severity": severity,
            "message": message,
            "context": scrub(context or {}),
            "timestamp_ms": int(now * 1000),
        })
        return True

    def recover(self, category: str, message: str = "") -> bool:
        """Emits a recovery notification for a previously alerted condition."""
        if category not in self._active:
            return False
        self._active.pop(category, None)
        self._last_sent.pop(category, None)
        self._emit({
            "kind": "RECOVERY",
            "category": category,
            "severity": "INFO",
            "message": message or f"{category} recovered",
            "timestamp_ms": int(time.time() * 1000),
        })
        return True

    @property
    def active_categories(self) -> List[str]:
        return sorted(self._active)

    def _emit(self, record: Dict[str, Any]) -> None:
        record = scrub(record)
        line = json.dumps(record, default=str)
        logger.warning("ALERT %s", line)

        if self.event_store is not None:
            try:
                self.event_store.record_incident(record["category"], record["severity"], record["message"])
                self.event_store.log_event("ALERT", record)
            except Exception as exc:
                logger.error("Alert persistence failed: %s", exc)

        if self.sink_path is not None:
            try:
                self.sink_path.parent.mkdir(parents=True, exist_ok=True)
                with self.sink_path.open("a", encoding="utf-8") as fh:
                    fh.write(line + "\n")
            except OSError as exc:
                logger.error("Alert sink write failed: %s", exc)

        if self.webhook_url:
            self._post_webhook(line)

    def _post_webhook(self, body: str) -> None:
        """Best-effort generic webhook delivery. Never blocks trading on failure."""
        try:
            req = urllib.request.Request(
                self.webhook_url,
                data=body.encode("utf-8"),
                headers={"Content-Type": "application/json", "User-Agent": "Escanor-LiveBot/1.0"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=5):
                pass
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            # The URL itself may embed a token, so only the failure class is logged.
            logger.error("Alert webhook delivery failed: %s", type(exc).__name__)


# Conditions the supervisor watches for. Each maps to one dedup key so a persistent
# problem alerts once and recovers once, rather than every tick.
SUPERVISED_CONDITIONS = (
    "PROCESS_CRASHED",
    "STALE_HEARTBEAT",
    "RECONNECT_STORM",
    "UNRESOLVED_GAP",
    "PRIVATE_STREAM_EXPIRED",
    "RECONCILIATION_FAILED",
    "UNKNOWN_ORDER",
    "KILL_SWITCH_ACTIVE",
    "DAILY_LOSS_ACTIVE",
)


class HealthSupervisor:
    """Turns health state into deduplicated incidents and recovery notifications.

    A two-second check after launch proves only that a process started. This runs on a
    cadence and watches the conditions that actually take an engine down quietly: a dead
    process, a heartbeat that stopped, a socket that reconnects in a loop, a gap nobody
    recovered, an expired private stream, a failed reconciliation, an UNKNOWN order, and
    the two circuit breakers.
    """

    def __init__(
        self,
        health: HealthMonitor,
        alerter: IncidentAlerter,
        kill_switch: Optional[Any] = None,
        equity: Optional[Any] = None,
        reconnect_storm_threshold: int = 5,
        reconnect_storm_window_s: float = 300.0,
    ):
        self.health = health
        self.alerter = alerter
        self.kill_switch = kill_switch
        self.equity = equity
        self.reconnect_storm_threshold = reconnect_storm_threshold
        self.reconnect_storm_window_s = reconnect_storm_window_s
        self._reconnects: List[float] = []

    def note_reconnect(self, when: Optional[float] = None) -> int:
        """Records a reconnect and returns how many happened inside the window."""
        now = when if when is not None else time.time()
        self._reconnects.append(now)
        cutoff = now - self.reconnect_storm_window_s
        self._reconnects = [t for t in self._reconnects if t >= cutoff]
        return len(self._reconnects)

    def check(self, process_alive: bool = True) -> List[str]:
        """Evaluates every supervised condition once. Returns the conditions now active."""
        active: List[str] = []

        if not process_alive:
            active.append("PROCESS_CRASHED")
            self.alerter.alert("PROCESS_CRASHED", "CRITICAL",
                               "A supervised engine process is no longer running.",
                               {"state": self.health.state().value})
        else:
            self.alerter.recover("PROCESS_CRASHED", "Engine process is running again")

        stale = self.health.stale_components()
        if stale:
            active.append("STALE_HEARTBEAT")
            self.alerter.alert("STALE_HEARTBEAT", "HIGH",
                               f"Components stopped reporting: {stale}",
                               {"stale": stale, "state": self.health.state().value})
        else:
            self.alerter.recover("STALE_HEARTBEAT", "All components are reporting again")

        if len(self._reconnects) >= self.reconnect_storm_threshold:
            active.append("RECONNECT_STORM")
            self.alerter.alert("RECONNECT_STORM", "HIGH",
                               f"{len(self._reconnects)} reconnects within "
                               f"{self.reconnect_storm_window_s:.0f}s.",
                               {"count": len(self._reconnects)})
        else:
            self.alerter.recover("RECONNECT_STORM", "Reconnect rate is back to normal")

        if self.health.data_desynced or self.health.history_gap_open:
            active.append("UNRESOLVED_GAP")
            self.alerter.alert("UNRESOLVED_GAP", "HIGH",
                               "Market data is desynced; strategy evaluation and new entries are blocked.",
                               {"state": self.health.state().value})
        else:
            self.alerter.recover("UNRESOLVED_GAP", "Market data is synchronized again")

        if self.health.requires_private_stream() and not self.health.is_fresh("private_stream"):
            active.append("PRIVATE_STREAM_EXPIRED")
            self.alerter.alert("PRIVATE_STREAM_EXPIRED", "CRITICAL",
                               "Private execution stream is stale; fills may be invisible.",
                               {"age_s": self.health.age_s("private_stream")})
        else:
            self.alerter.recover("PRIVATE_STREAM_EXPIRED", "Private execution stream is fresh again")

        if self.health.reconciliation_required:
            active.append("RECONCILIATION_FAILED")
            self.alerter.alert("RECONCILIATION_FAILED", "HIGH",
                               "Account reconciliation could not complete; new risk is blocked.",
                               {"state": self.health.state().value})
        else:
            self.alerter.recover("RECONCILIATION_FAILED", "Account reconciled")

        if self.health.unknown_orders:
            active.append("UNKNOWN_ORDER")
            self.alerter.alert("UNKNOWN_ORDER", "HIGH",
                               f"Unresolved UNKNOWN order(s): {sorted(self.health.unknown_orders)}",
                               {"orders": sorted(self.health.unknown_orders)})
        else:
            self.alerter.recover("UNKNOWN_ORDER", "No unresolved UNKNOWN orders")

        engaged = bool(self.kill_switch and self.kill_switch.is_engaged())
        self.health.killed = engaged
        if engaged:
            active.append("KILL_SWITCH_ACTIVE")
            self.alerter.alert("KILL_SWITCH_ACTIVE", "CRITICAL",
                               "Kill switch is engaged; no new risk will be opened.", {})
        else:
            self.alerter.recover("KILL_SWITCH_ACTIVE", "Kill switch disengaged")

        if self.equity is not None and getattr(self.equity, "breaker_engaged", False):
            active.append("DAILY_LOSS_ACTIVE")
            self.alerter.alert("DAILY_LOSS_ACTIVE", "CRITICAL",
                               "Daily loss limit reached; new entries are blocked for the rest of the UTC day.",
                               self.equity.snapshot())
        else:
            self.alerter.recover("DAILY_LOSS_ACTIVE", "Daily loss limit is no longer active")

        return active
