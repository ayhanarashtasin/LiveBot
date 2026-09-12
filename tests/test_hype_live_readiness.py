"""HYPEUSDT production LIVE readiness: configuration, isolation and gate evidence.

Phase 48 promotes HYPE from SHADOW/PAPER to TESTNET and funded LIVE. What has to hold is
that the two new configurations are frozen to the approved manifest, carry no secrets, own
their own databases, and that the activation gates still fail closed without operator
acknowledgement and pass only against real recorded evidence.
"""
import asyncio
import json
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pytest

from live_engine.config import (
    LiveEngineConfig,
    load_config,
    reject_config_credentials,
    validate_config_against_manifest,
    validate_database_path,
    validate_unique_databases,
)
from live_engine.main import (
    _await_stream_evidence,
    preflight_live_authorization,
    validate_live_safety_gates,
)
from live_engine.persistence.event_store import EventStore
from live_engine.risk.kill_switch import KillSwitch
from live_engine.risk.safety_gates import SafetyGateVerifier, record_evidence
from live_engine.strategy.loader import StrategyLoader

BASE = Path(__file__).resolve().parents[1]
MANIFEST_PATH = BASE / "benchmarks/manifests/HYPE_LUXALGO_RANK12_5M.yaml"
PRODUCTION_MODES = ("testnet", "live")

# Gates that resolve from configuration, hashes on disk or durable state. The remaining
# gates read live runtime evidence (stream freshness, exchange filters, account state) and
# can only be satisfied by a connected engine, which is exactly why they are not asserted
# here: a test that made them pass offline would be testing a weakened gate.
STATIC_GATE_IDS = (1, 2, 3, 4, 5, 6, 7, 8, 9, 16, 17, 18, 22, 23, 24, 25, 34)


def _manifest():
    return StrategyLoader.load_manifest(MANIFEST_PATH)


def _config(mode: str) -> LiveEngineConfig:
    return load_config(str(BASE / f"config/hype-{mode}.json"))


def gate(results, gate_id):
    return next(g for g in results if g.gate_id == gate_id)


def test_hype_live_config_matches_manifest():
    """Both production configs are frozen to the approved manifest."""
    manifest = _manifest()
    assert manifest["allowed_modes"] == ["SHADOW", "PAPER", "TESTNET", "LIVE"]

    for mode in PRODUCTION_MODES:
        config = _config(mode)
        validate_config_against_manifest(config, manifest)  # must not raise
        assert config.symbol == "HYPEUSDT"
        assert config.timeframe == "5m"
        assert config.warmup_candles == manifest["signal"]["warmup_candles"] == 1000
        assert config.max_open_trades == manifest["risk"]["max_open_trades"] == 12
        assert config.stake_rule == manifest["risk"]["stake_rule"] == "full_compounding"
        assert config.stake_amount == Decimal("10000.0")

    # The frozen execution and risk inputs are unchanged by the promotion.
    assert manifest["fees"] == {"maker": 0.0002, "taker": 0.0005}
    assert manifest["risk"]["leverage"] == 3 and manifest["risk"]["slots"] == 12
    assert manifest["execution_model"]["stop_atr_multiplier"] == 6.0
    assert manifest["execution_model"]["take_profit_atr_multiplier"] == 4.5
    assert manifest["execution_model"]["time_exit_bars"] == 384

    # TESTNET routes to the testnet host; LIVE opens funded risk behind a canary ceiling.
    testnet, live = _config("testnet"), _config("live")
    assert testnet.binance_testnet is True and testnet.canary_mode is False
    assert live.binance_testnet is False
    assert live.canary_mode is True and live.max_canary_allocation_usd == Decimal("100.0")

    # A mode the manifest does not allow is still refused.
    with pytest.raises(ValueError, match="MODE NOT ALLOWED"):
        validate_config_against_manifest(
            replace(live, mode="BACKTEST"), {**manifest, "allowed_modes": ["SHADOW"]})


def test_hype_live_config_rejects_credentials(tmp_path):
    """Shipped configs carry no secrets, and an embedded credential fails closed."""
    for mode in PRODUCTION_MODES:
        raw = json.loads((BASE / f"config/hype-{mode}.json").read_text(encoding="utf-8"))
        reject_config_credentials(raw, f"config/hype-{mode}.json")  # must not raise
        flat = json.dumps(raw).lower()
        assert "api_key" not in flat and "api_secret" not in flat and "secret" not in flat

        for leaked in ("binance_api_key", "binance_api_secret", "api_secret"):
            poisoned = tmp_path / f"hype-{mode}-{leaked}.json"
            poisoned.write_text(json.dumps({**raw, leaked: "AKIA-not-a-real-key"}), encoding="utf-8")
            with pytest.raises(ValueError, match="CREDENTIALS IN CONFIG"):
                load_config(str(poisoned))


def test_hype_live_database_isolation():
    """Each HYPE mode owns a distinct database contained under data/."""
    configs = [_config(mode) for mode in ("shadow", "paper", "testnet", "live")]
    validate_unique_databases(configs, base_dir=BASE)

    resolved = {c.mode: validate_database_path(c.event_store_path, base_dir=BASE) for c in configs}
    assert len(set(resolved.values())) == len(configs)
    assert resolved["TESTNET"].name == "hype_testnet.db"
    assert resolved["LIVE"].name == "hype_live.db"
    for path in resolved.values():
        assert path.parent == (BASE / "data").resolve()

    # Neither production instance may reach another instance's database or escape data/.
    for bad in ("data/../hype_live.db", "database/hype_live.db", "data"):
        with pytest.raises(ValueError, match="DATABASE ISOLATION VIOLATION"):
            validate_database_path(bad, base_dir=BASE)


def test_hype_safety_gates_evaluation(tmp_path, monkeypatch):
    """LIVE is blocked without acknowledgement and passes the static gates with evidence."""
    kill_switch_path = tmp_path / ".kill_switch"
    config = replace(_config("live"), kill_switch_path=str(kill_switch_path))
    store = EventStore(tmp_path / "hype_live.db")

    # --- unacknowledged and unauthenticated: fails closed -------------------
    monkeypatch.delenv("ESCANOR_LIVE_TRADING_ENABLED", raising=False)
    bare = replace(config, binance_api_key=None, binance_api_secret=None)
    ks = KillSwitch(kill_switch_path)

    with pytest.raises(PermissionError, match="LIVE EXECUTION BLOCKED"):
        preflight_live_authorization(bare, ks)
    with pytest.raises(PermissionError, match="LIVE EXECUTION BLOCKED"):
        validate_live_safety_gates(bare, ks)

    _, results = SafetyGateVerifier(bare, base_dir=BASE, event_store=store).evaluate_all_gates()
    assert gate(results, 24).passed is False
    assert "ESCANOR_LIVE_TRADING_ENABLED" in gate(results, 24).details
    # No soak or chaos evidence has been recorded yet either.
    assert gate(results, 34).passed is False

    # Acknowledgement alone is not enough: credentials are still required.
    monkeypatch.setenv("ESCANOR_LIVE_TRADING_ENABLED", "true")
    with pytest.raises(PermissionError, match="credentials"):
        preflight_live_authorization(bare, ks)

    # An engaged kill switch refuses LIVE before anything else is considered.
    ks.trigger(reason="readiness test", triggered_by="TEST")
    authorized = replace(config, binance_api_key="mock-key", binance_api_secret="mock-secret")
    with pytest.raises(PermissionError, match="Persistent Kill Switch is active"):
        preflight_live_authorization(authorized, ks)
    _, engaged = SafetyGateVerifier(authorized, base_dir=BASE, event_store=store).evaluate_all_gates()
    assert gate(engaged, 22).passed is False
    ks.disengage(reason="readiness test complete", disengaged_by="TEST")

    # --- acknowledged, authenticated, evidence recorded ---------------------
    record_evidence(store, "testnet_soak_evidence", True, {"hours": 72, "orders": 40})
    record_evidence(store, "chaos_evidence", True, {"scenarios": 16})
    preflight_live_authorization(authorized, ks)  # must not raise

    verifier = SafetyGateVerifier(authorized, base_dir=BASE, event_store=store)
    _, passing = verifier.evaluate_all_gates()
    assert len(passing) == len(verifier.GATE_NAMES) == 34

    failed = [f"Gate {g.gate_id} ({g.name}): {g.details}"
              for g in passing if g.gate_id in STATIC_GATE_IDS and not g.passed]
    assert not failed, failed
    assert gate(passing, 34).passed is True
    assert gate(passing, 3).passed is True  # config still matches the frozen manifest

    # Stale evidence is not evidence: Gate 34 fails again once the soak record expires.
    stale = store.get_state("testnet_soak_evidence")
    stale["observed_at_ms"] -= int(8 * 86400 * 1000)
    store.set_state("testnet_soak_evidence", stale)
    _, expired = SafetyGateVerifier(authorized, base_dir=BASE, event_store=store).evaluate_all_gates()
    assert gate(expired, 34).passed is False
    assert "stale" in gate(expired, 34).details


def test_gate_33_reads_the_instance_that_captured_the_telemetry(tmp_path):
    """A funded database has no telemetry before activation; the soak instance has it."""
    data = tmp_path / "data"
    data.mkdir()
    live = EventStore(data / "hype_live.db")
    config = replace(_config("live"), kill_switch_path=str(tmp_path / ".kill_switch"))

    def telemetry_gate(cfg=config):
        return SafetyGateVerifier(cfg, base_dir=tmp_path, event_store=live)._gate_telemetry()

    # Nothing anywhere: fails closed and says where it looked.
    blocked = telemetry_gate()
    assert blocked.passed is False
    assert "hype_live.db" in blocked.details

    # An empty sibling is not evidence either, and it is named in the failure.
    EventStore(data / "hype_testnet.db")
    still_blocked = telemetry_gate()
    assert still_blocked.passed is False
    assert "hype_testnet.db" in still_blocked.details

    # The soak instance captured telemetry: that is this symbol's execution evidence.
    EventStore(data / "hype_testnet.db").record_telemetry(
        {"client_order_id": "ESC-HYPEUSDT-5M-1-BUY", "signal_id": "SIG-1"})
    passing = telemetry_gate()
    assert passing.passed is True
    assert "hype_testnet.db" in passing.details

    # The funded database's own telemetry takes precedence once it exists.
    live.record_telemetry({"client_order_id": "ESC-HYPEUSDT-5M-2-BUY", "signal_id": "SIG-2"})
    primary = telemetry_gate()
    assert primary.passed is True and "hype_live.db" in primary.details

    # Siblings are only for LIVE: a TESTNET instance must produce its own telemetry.
    testnet = replace(_config("testnet"), kill_switch_path=str(tmp_path / ".kill_switch"))
    verifier = SafetyGateVerifier(testnet, base_dir=tmp_path, event_store=EventStore(data / "empty.db"))
    assert verifier._sibling_instance_dbs() == []
    assert verifier._gate_telemetry().passed is False


def test_hype_live_gates_all_pass_with_complete_evidence(tmp_path, monkeypatch):
    """Every one of the 34 gates has a producer: with all evidence present, LIVE is authorised.

    This is the funded pre-flight in the state the operator reaches after a real TESTNET
    soak, a chaos exercise and the seasoning dry runs. The runtime halves (filters, stream
    freshness) come from an attached engine exactly as they do in `run_live_loop`.
    """
    from live_engine.execution.order_filters import SymbolFilters
    from live_engine.risk.health import HealthMonitor

    store = EventStore(tmp_path / "hype_live.db")
    monkeypatch.setenv("ESCANOR_LIVE_TRADING_ENABLED", "true")
    config = replace(_config("live"), kill_switch_path=str(tmp_path / ".kill_switch"),
                     binance_api_key="mock-key", binance_api_secret="mock-secret")

    # Durable evidence a real run records.
    for mode in ("SHADOW", "PAPER"):
        store.log_event("SYSTEM_INITIALIZED", {"mode": mode, "symbol": "HYPEUSDT"})
    store.log_event("KLINE_VALIDATION", {"status": "MATCH"})
    store.log_event("RECONCILIATION_COMPLETED", {"reconciled": True, "unresolved_orders": []})
    store.log_event("POSITION_MODE_VALIDATED", {"position_mode": "ONE_WAY"})
    store.log_event("ACCOUNT_CONFIG_RECONCILIATION",
                    {"matched": True, "leverage": "3", "margin_mode": "CROSSED", "issues": []})
    store.log_event("PROTECTIVE_RECONCILIATION",
                    {"symbol": "HYPEUSDT", "source": "slot_book", "open_slots": 0, "issues": []})
    store.record_telemetry({"client_order_id": "ESC-HYPEUSDT-5M-1-BUY", "signal_id": "SIG-1"})
    record_evidence(store, "testnet_soak_evidence", True, {"hours": 72, "orders": 40})
    record_evidence(store, "chaos_evidence", True, {"scenarios": 16})

    # Runtime evidence an attached, connected engine reports.
    health = HealthMonitor(mode="LIVE", event_store=store)
    for component in ("engine", "public_stream", "private_stream", "mark_price"):
        health.beat(component)

    class AttachedEngine:
        pass

    engine = AttachedEngine()
    engine.health = health
    engine.event_store = store
    engine.filters = SymbolFilters(
        symbol="HYPEUSDT", tick_size=Decimal("0.001"), step_size=Decimal("0.01"),
        min_qty=Decimal("0.01"), max_qty=Decimal("200000"), min_notional=Decimal("5"),
        price_precision=5, qty_precision=2,
    )

    verifier = SafetyGateVerifier(config, base_dir=BASE, orchestrator=engine, event_store=store)
    all_passed, results = verifier.evaluate_all_gates()

    failed = [f"Gate {g.gate_id} ({g.name}): {g.details}" for g in results if not g.passed]
    assert not failed, failed
    assert all_passed is True and len(results) == 34
    # Authorisation is evidence plus acknowledgement, never acknowledgement alone.
    monkeypatch.delenv("ESCANOR_LIVE_TRADING_ENABLED")
    _, without_ack = SafetyGateVerifier(config, base_dir=BASE, orchestrator=engine,
                                        event_store=store).evaluate_all_gates()
    assert gate(without_ack, 24).passed is False


def test_live_stream_evidence_wait_is_bounded_and_fails_open_to_the_gates():
    """The pre-flight wait returns on real events, and a silent stream is left to the gates."""

    class FakeHealth:
        def __init__(self, ages): self.ages = ages
        def age_s(self, component): return self.ages.get(component)

    class FakeOrchestrator:
        def __init__(self, private_age, user_stream=True):
            self.health = FakeHealth({"private_stream": private_age})
            self.user_data_stream = object() if user_stream else None

    # A trade received and the private stream reporting: the wait completes at once.
    asyncio.run(_await_stream_evidence(
        FakeOrchestrator(private_age=1.0), {"buffer": ["trade"]}, timeout=5.0))

    # A silent public stream does not hang and does not raise: the gates report the gap.
    asyncio.run(_await_stream_evidence(
        FakeOrchestrator(private_age=1.0), {"buffer": []}, timeout=0.5))
    # A private stream that never connected is treated the same way.
    asyncio.run(_await_stream_evidence(
        FakeOrchestrator(private_age=None), {"buffer": ["trade"]}, timeout=0.5))
    # Modes without a private stream only need the public one.
    asyncio.run(_await_stream_evidence(
        FakeOrchestrator(private_age=None, user_stream=False), {"buffer": ["trade"]}, timeout=5.0))
