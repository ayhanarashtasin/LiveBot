"""Section 13 regressions: health supervision and incident alerting."""
import json
import time
from decimal import Decimal
from pathlib import Path

import pytest

from live_engine.persistence.event_store import EventStore
from live_engine.risk.health import (
    HealthMonitor,
    HealthState,
    HealthSupervisor,
    IncidentAlerter,
    scrub,
)
from live_engine.risk.kill_switch import KillSwitch

SCRIPTS = Path("scripts")


@pytest.fixture
def rig(tmp_path):
    store = EventStore(tmp_path / "health.db")
    health = HealthMonitor(mode="TESTNET", event_store=store)
    alerter = IncidentAlerter(event_store=store, sink_path=tmp_path / "incidents.jsonl",
                              dedup_window_s=300.0)
    ks = KillSwitch(tmp_path / ".kill_switch")
    supervisor = HealthSupervisor(health, alerter, kill_switch=ks)
    return supervisor, health, alerter, store, ks, tmp_path


def _all_fresh(health):
    for component in ("engine", "public_stream", "private_stream", "mark_price",
                      "account_state", "reconciliation"):
        health.beat(component)
    health.warmup_ready = True


# --- heartbeats --------------------------------------------------------------

def test_heartbeats_are_persisted_for_every_supervised_component(rig):
    supervisor, health, alerter, store, ks, _ = rig
    for component in ("engine", "public_stream", "kline_validator", "private_stream",
                      "reconciliation", "dashboard_projection"):
        health.beat(component)

    stored = store.get_heartbeats()
    assert set(stored) >= {"engine", "public_stream", "kline_validator", "private_stream",
                           "reconciliation", "dashboard_projection"}
    assert all(row["last_beat_ms"] > 0 for row in stored.values())


def test_a_restarted_monitor_reads_heartbeats_from_durable_state(rig):
    supervisor, health, alerter, store, ks, _ = rig
    health.beat("engine")

    restarted = HealthMonitor(mode="TESTNET", event_store=store)
    assert restarted.age_s("engine") is not None
    assert restarted.is_fresh("engine") is True


# --- supervised conditions ---------------------------------------------------

def test_crash_is_detected_and_recovers(rig):
    supervisor, health, alerter, store, ks, _ = rig
    _all_fresh(health)

    assert "PROCESS_CRASHED" in supervisor.check(process_alive=False)
    assert "PROCESS_CRASHED" in alerter.active_categories

    supervisor.check(process_alive=True)
    assert "PROCESS_CRASHED" not in alerter.active_categories


def test_stale_heartbeat_is_detected(rig):
    supervisor, health, alerter, store, ks, _ = rig
    _all_fresh(health)
    assert "STALE_HEARTBEAT" not in supervisor.check()

    health._beats["private_stream"]["last_beat_ms"] -= 10 * 60 * 1000
    assert "STALE_HEARTBEAT" in supervisor.check()


def test_reconnect_storm_is_detected(rig):
    supervisor, health, alerter, store, ks, _ = rig
    _all_fresh(health)

    for _ in range(4):
        supervisor.note_reconnect()
    assert "RECONNECT_STORM" not in supervisor.check()

    supervisor.note_reconnect()
    assert "RECONNECT_STORM" in supervisor.check()


def test_old_reconnects_fall_out_of_the_window(rig):
    supervisor, health, alerter, store, ks, _ = rig
    now = time.time()
    for _ in range(6):
        supervisor.note_reconnect(when=now - 1000)
    assert supervisor.note_reconnect(when=now) == 1


@pytest.mark.parametrize("setup,condition", [
    (lambda h: setattr(h, "data_desynced", True), "UNRESOLVED_GAP"),
    (lambda h: setattr(h, "history_gap_open", True), "UNRESOLVED_GAP"),
    (lambda h: setattr(h, "reconciliation_required", True), "RECONCILIATION_FAILED"),
    (lambda h: h.unknown_orders.add("ESC-1"), "UNKNOWN_ORDER"),
])
def test_each_supervised_condition_is_detected(rig, setup, condition):
    supervisor, health, alerter, store, ks, _ = rig
    _all_fresh(health)
    assert condition not in supervisor.check()

    setup(health)
    assert condition in supervisor.check()


def test_expired_private_stream_is_critical(rig):
    supervisor, health, alerter, store, ks, _ = rig
    _all_fresh(health)
    health._beats["private_stream"]["last_beat_ms"] -= 30 * 60 * 1000

    assert "PRIVATE_STREAM_EXPIRED" in supervisor.check()
    assert health.state() is HealthState.EXCHANGE_DISCONNECTED


def test_kill_switch_and_daily_loss_activation_are_supervised(rig):
    supervisor, health, alerter, store, ks, tmp_path = rig
    _all_fresh(health)

    ks.trigger("operator halt", "TEST")
    assert "KILL_SWITCH_ACTIVE" in supervisor.check()
    assert health.state() is HealthState.KILLED

    ks.disengage("resume", "TEST")

    class Breaker:
        breaker_engaged = True

        @staticmethod
        def snapshot():
            return {"daily_drawdown": "900", "max_daily_drawdown_usd": "500"}

    supervisor.equity = Breaker()
    assert "DAILY_LOSS_ACTIVE" in supervisor.check()


# --- alerting ----------------------------------------------------------------

def test_repeated_alerts_are_deduplicated_and_recovery_is_emitted(tmp_path):
    sink = tmp_path / "incidents.jsonl"
    alerter = IncidentAlerter(sink_path=sink, dedup_window_s=300.0)

    assert alerter.alert("UNRESOLVED_GAP", "HIGH", "gap open") is True
    assert alerter.alert("UNRESOLVED_GAP", "HIGH", "gap open") is False
    assert alerter.recover("UNRESOLVED_GAP", "gap closed") is True
    assert alerter.recover("UNRESOLVED_GAP") is False

    records = [json.loads(line) for line in sink.read_text(encoding="utf-8").splitlines()]
    assert [r["kind"] for r in records] == ["INCIDENT", "RECOVERY"]


def test_local_structured_sink_is_the_default(tmp_path):
    sink = tmp_path / "incidents.jsonl"
    alerter = IncidentAlerter(sink_path=sink)
    assert alerter.webhook_url is None

    alerter.alert("STALE_HEARTBEAT", "HIGH", "engine quiet")
    record = json.loads(sink.read_text(encoding="utf-8").splitlines()[0])
    assert record["category"] == "STALE_HEARTBEAT"
    assert record["severity"] == "HIGH"
    assert record["timestamp_ms"] > 0


def test_webhook_credentials_come_only_from_the_environment(monkeypatch, tmp_path):
    monkeypatch.setenv("ESCANOR_ALERT_WEBHOOK_URL", "https://example.invalid/hook")
    assert IncidentAlerter(sink_path=tmp_path / "i.jsonl").webhook_url == "https://example.invalid/hook"

    monkeypatch.delenv("ESCANOR_ALERT_WEBHOOK_URL")
    assert IncidentAlerter(sink_path=tmp_path / "i.jsonl").webhook_url is None


def test_alerts_never_carry_secrets(tmp_path):
    sink = tmp_path / "incidents.jsonl"
    alerter = IncidentAlerter(sink_path=sink)
    alerter.alert("PRIVATE_STREAM_EXPIRED", "CRITICAL", "stream down", {
        "api_key": "REAL-KEY", "listenKey": "REAL-LISTEN-KEY",
        "signature": "abc123", "nested": {"apiSecret": "REAL-SECRET"},
        "symbol": "BTCUSDT",
    })
    text = sink.read_text(encoding="utf-8")
    for secret in ("REAL-KEY", "REAL-LISTEN-KEY", "abc123", "REAL-SECRET"):
        assert secret not in text
    assert "BTCUSDT" in text
    assert "[REDACTED]" in text


def test_scrub_handles_nested_structures():
    scrubbed = scrub({"listen_key": "x", "orders": [{"api_secret": "y", "id": 1}]})
    assert scrubbed["listen_key"] == "[REDACTED]"
    assert scrubbed["orders"][0]["api_secret"] == "[REDACTED]"
    assert scrubbed["orders"][0]["id"] == 1


# --- operator tooling --------------------------------------------------------

def test_control_script_offers_graceful_stop_with_bounded_forced_fallback():
    script = (SCRIPTS / "escanor-control.ps1").read_text(encoding="utf-8")
    assert "-Supervise" in script and "-Stop" in script
    assert "StopTimeoutSeconds" in script
    assert "FORCED_TERMINATION" in script
    assert "CloseMainWindow" in script


def test_launchers_no_longer_present_force_kill_as_a_clean_shutdown():
    for name in ("run-btc-lit-zec-paper-dashboard.ps1", "run-lit-zec-paper-shadow.ps1"):
        text = (SCRIPTS / name).read_text(encoding="utf-8")
        assert "escanor-control.ps1 -Stop" in text
        assert "escanor-control.ps1 -Supervise" in text
        assert "last resort, not a clean shutdown" in text
        assert "escanor-processes.json" in text
