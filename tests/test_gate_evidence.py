"""Section 12 regressions: gates need evidence, not files.

Several gates used to pass by grepping a source file for a string or checking that a test
file existed. These tests pin that a gate now reads current configuration, durable
evidence or live runtime state, and fails closed when that evidence is missing or stale.
"""
import time
from decimal import Decimal
from pathlib import Path

import pytest

from live_engine.config import LiveEngineConfig
from live_engine.persistence.event_store import EventStore
from live_engine.risk.health import HealthMonitor
from live_engine.risk.safety_gates import EXPIRY, Evidence, SafetyGateVerifier, record_evidence

BASE = Path.cwd()


def _config(tmp_path, mode="SHADOW"):
    return LiveEngineConfig(
        mode=mode, symbol="BTCUSDT", timeframe="5m",
        manifest_path="benchmarks/manifests/BTC_ST_09_5M.yaml",
        event_store_path="data/btc_paper.db",
        kill_switch_path=str(tmp_path / ".kill_switch"),
    )


def _verifier(tmp_path, store=None, orchestrator=None, mode="SHADOW"):
    return SafetyGateVerifier(_config(tmp_path, mode), base_dir=BASE,
                              orchestrator=orchestrator, event_store=store)


def gate(results, name):
    return next(g for g in results if g.name == name)


# --- evidence records --------------------------------------------------------

def test_evidence_expiry_is_enforced():
    fresh = Evidence("durable_state", "ok", int(time.time() * 1000), 300.0)
    stale = Evidence("durable_state", "ok", int(time.time() * 1000) - 600_000, 300.0)
    timeless = Evidence("config", "SHADOW", None, None)

    assert fresh.is_expired is False
    assert stale.is_expired is True
    assert timeless.is_expired is False
    assert stale.to_dict()["expired"] is True


def test_every_gate_reports_source_value_timestamp_expiry_and_reason(tmp_path):
    store = EventStore(tmp_path / "gates.db")
    _, results = _verifier(tmp_path, store=store).evaluate_all_gates()

    for g in results:
        record = g.evidence.to_dict()
        assert set(record) >= {"source", "value", "observed_at_ms", "expiry_s", "expired", "reason"}
        if not g.passed:
            assert g.details  # a failure always states why


# --- file existence is not evidence -----------------------------------------

def test_a_test_file_existing_cannot_satisfy_a_gate(tmp_path):
    """An empty store means no run ever happened; SHADOW/PAPER gates must fail."""
    store = EventStore(tmp_path / "empty.db")
    _, results = _verifier(tmp_path, store=store).evaluate_all_gates()

    for name in ("SHADOW tested", "PAPER tested"):
        g = gate(results, name)
        assert g.passed is False
        assert "No recorded" in g.details or "durable" in g.details.lower()

    # These test files exist in the repository; their existence changed nothing.
    assert (BASE / "tests" / "test_shadow_broker.py").exists()
    assert (BASE / "tests" / "test_paper_broker.py").exists()


def test_mode_gate_passes_only_after_a_recorded_run(tmp_path):
    store = EventStore(tmp_path / "runs.db")
    store.log_event("SYSTEM_INITIALIZED", {"mode": "SHADOW", "symbol": "BTCUSDT"})

    _, results = _verifier(tmp_path, store=store).evaluate_all_gates()
    assert gate(results, "SHADOW tested").passed is True
    assert gate(results, "PAPER tested").passed is False


def test_unknown_order_gate_reads_state_not_source_strings(tmp_path):
    store = EventStore(tmp_path / "unknown.db")

    class Orch:
        health = HealthMonitor(event_store=store)
        event_store = store

    orch = Orch()
    _, clean = _verifier(tmp_path, store=store, orchestrator=orch).evaluate_all_gates()
    assert gate(clean, "Unknown-order reconciliation tested").passed is True

    orch.health.unknown_orders.add("ESC-BTCUSDT-5M-1-BUY")
    _, dirty = _verifier(tmp_path, store=store, orchestrator=orch).evaluate_all_gates()
    blocked = gate(dirty, "Unknown-order reconciliation tested")
    assert blocked.passed is False
    assert "UNKNOWN" in blocked.details


def test_telemetry_gate_requires_captured_records(tmp_path):
    store = EventStore(tmp_path / "tel.db")
    _, before = _verifier(tmp_path, store=store).evaluate_all_gates()
    assert gate(before, "Execution telemetry available").passed is False

    store.record_telemetry({"client_order_id": "ESC-1", "signal_id": "SIG-1"})
    _, after = _verifier(tmp_path, store=store).evaluate_all_gates()
    assert gate(after, "Execution telemetry available").passed is True


def test_reconciliation_gate_requires_a_clean_recorded_pass(tmp_path):
    store = EventStore(tmp_path / "recon.db")
    store.log_event("RECONCILIATION_COMPLETED", {"reconciled": False, "unresolved_orders": ["ESC-1"]})
    _, dirty = _verifier(tmp_path, store=store).evaluate_all_gates()
    assert gate(dirty, "Account reconciled with no UNKNOWN orders").passed is False

    store.log_event("RECONCILIATION_COMPLETED", {"reconciled": True, "unresolved_orders": []})
    _, clean = _verifier(tmp_path, store=store).evaluate_all_gates()
    assert gate(clean, "Account reconciled with no UNKNOWN orders").passed is True


def test_stale_evidence_fails_closed(tmp_path, monkeypatch):
    store = EventStore(tmp_path / "stale.db")
    record_evidence(store, "testnet_soak_evidence", True, {"hours": 24})
    record_evidence(store, "chaos_evidence", True, {"scenarios": 12})

    _, fresh = _verifier(tmp_path, store=store).evaluate_all_gates()
    assert gate(fresh, "Testnet and chaos evidence recorded").passed is True

    # Age the testnet evidence past its expiry policy.
    aged = store.get_state("testnet_soak_evidence")
    aged["observed_at_ms"] -= int((EXPIRY["testnet_soak"] + 60) * 1000)
    store.set_state("testnet_soak_evidence", aged)

    _, stale = _verifier(tmp_path, store=store).evaluate_all_gates()
    blocked = gate(stale, "Testnet and chaos evidence recorded")
    assert blocked.passed is False
    assert "stale" in blocked.details.lower()


def test_unrecorded_testnet_or_chaos_evidence_fails_closed(tmp_path):
    store = EventStore(tmp_path / "none.db")
    _, results = _verifier(tmp_path, store=store).evaluate_all_gates()
    blocked = gate(results, "Testnet and chaos evidence recorded")
    assert blocked.passed is False
    assert "No testnet soak evidence" in blocked.details


def test_raw_coverage_gate_reads_the_manifest_and_disk(tmp_path):
    store = EventStore(tmp_path / "cov.db")
    cfg = _config(tmp_path)
    cfg.manifest_path = "benchmarks/manifests/HYPE_LUXALGO_RANK12_5M.yaml"
    cfg.symbol, cfg.timeframe = "HYPEUSDT", "5m"
    verifier = SafetyGateVerifier(cfg, base_dir=BASE, event_store=store)

    _, results = verifier.evaluate_all_gates()
    assert gate(results, "Raw dataset coverage complete").passed is True


def test_credentials_in_a_config_file_fail_the_secrets_gate(tmp_path):
    store = EventStore(tmp_path / "sec.db")
    _, clean = _verifier(tmp_path, store=store).evaluate_all_gates()
    assert gate(clean, "Secrets excluded from Git").passed is True

    leaky = BASE / "config" / "__gate_probe.json"
    leaky.write_text('{"mode": "SHADOW", "binance_api_key": "AKIA-NOT-REAL"}', encoding="utf-8")
    try:
        _, dirty = _verifier(tmp_path, store=store).evaluate_all_gates()
        blocked = gate(dirty, "Secrets excluded from Git")
        assert blocked.passed is False
        assert "__gate_probe.json" in str(blocked.evidence.value)
    finally:
        leaky.unlink()


def test_operator_env_flag_is_additional_not_a_substitute(tmp_path, monkeypatch):
    store = EventStore(tmp_path / "live.db")
    monkeypatch.setenv("ESCANOR_LIVE_TRADING_ENABLED", "true")
    cfg = _config(tmp_path, mode="LIVE")
    cfg.binance_api_key, cfg.binance_api_secret = "k", "s"

    verifier = SafetyGateVerifier(cfg, base_dir=BASE, event_store=store)
    all_passed, results = verifier.evaluate_all_gates()

    assert gate(results, "LIVE disabled by default").passed is True   # operator ack present
    assert all_passed is False                                        # evidence still missing
    failed = [g.name for g in results if not g.passed]
    assert "Execution telemetry available" in failed
    assert "Testnet and chaos evidence recorded" in failed


def test_kill_switch_gate_reads_live_state(tmp_path):
    from live_engine.risk.kill_switch import KillSwitch

    store = EventStore(tmp_path / "ks.db")
    _, clean = _verifier(tmp_path, store=store).evaluate_all_gates()
    assert gate(clean, "Kill switch tested").passed is True

    KillSwitch(tmp_path / ".kill_switch").trigger("test halt", "TEST")
    _, engaged = _verifier(tmp_path, store=store).evaluate_all_gates()
    assert gate(engaged, "Kill switch tested").passed is False
