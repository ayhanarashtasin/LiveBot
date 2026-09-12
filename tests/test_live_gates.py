"""Tests for SafetyGateVerifier and funded activation gate enforcement."""
import os
import pytest
from pathlib import Path

from live_engine.config import LiveEngineConfig
from live_engine.risk.safety_gates import SafetyGateVerifier
from live_engine.risk.kill_switch import KillSwitch
from live_engine.main import validate_live_safety_gates


def test_safety_gate_verifier_covers_goal_gates_plus_runtime_evidence():
    """The 24 goal.md gates remain, joined by the runtime-evidence gates."""
    config = LiveEngineConfig(mode="SHADOW")
    verifier = SafetyGateVerifier(config)
    all_passed, results = verifier.evaluate_all_gates()

    assert len(results) == len(verifier.GATE_NAMES)
    assert len(results) >= 24
    assert results[0].name == "Repository audit completed"
    assert verifier.GATE_NAMES[23] == "LIVE disabled by default"

    names = [g.name for g in results]
    for required in (
        "Raw dataset coverage complete",
        "Market data synchronized",
        "Private execution stream fresh",
        "Account reconciled with no UNKNOWN orders",
        "Position/margin/leverage mode correct",
        "Exchange filters current",
        "Protective state consistent",
        "Mark price and account data fresh",
        "Execution telemetry available",
        "Testnet and chaos evidence recorded",
    ):
        assert required in names

    # Every gate carries an evidence record, not just a verdict.
    assert all(g.evidence is not None for g in results)


def test_validate_live_safety_gates_rejects_unauthorized_live():
    """Verify that validate_live_safety_gates blocks live mode when unauthenticated."""
    # Ensure LIVE_TRADING_ENABLED is unset
    if "ESCANOR_LIVE_TRADING_ENABLED" in os.environ:
        del os.environ["ESCANOR_LIVE_TRADING_ENABLED"]

    config = LiveEngineConfig(
        mode="LIVE",
        binance_api_key=None,
        binance_api_secret=None,
    )
    ks = KillSwitch(Path(".kill_switch"))

    with pytest.raises(PermissionError) as exc_info:
        validate_live_safety_gates(config, ks)

    assert "LIVE EXECUTION BLOCKED" in str(exc_info.value)
    assert "Gate 24" in str(exc_info.value) or "Gate" in str(exc_info.value)


def test_validate_live_safety_gates_rejects_when_kill_switch_engaged():
    """Verify that engaged kill switch rejects live mode immediately."""
    ks_path = Path(".test_kill_switch_gate")
    if ks_path.exists():
        ks_path.unlink()

    ks = KillSwitch(ks_path)
    ks.trigger(reason="Test active kill switch", triggered_by="TEST")

    config = LiveEngineConfig(
        mode="LIVE",
        binance_api_key="mock_key",
        binance_api_secret="mock_secret",
        kill_switch_path=str(ks_path),
    )

    with pytest.raises(PermissionError) as exc_info:
        validate_live_safety_gates(config, ks)

    assert "Persistent Kill Switch is active" in str(exc_info.value)

    if ks_path.exists():
        ks_path.unlink()
