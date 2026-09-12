"""Section 6 regressions: benchmark execution-timing parity.

BTC fills at the signal close; LIT and ZEC fill at the next candle open. Before this
release the replay used next-open for LIT/ZEC while the runtime sent a market order on
the signal candle - a whole bar of timing divergence that parity could not see.
"""
from decimal import Decimal

import pytest
import yaml

from live_engine.config import (
    LiveEngineConfig,
    validate_config_against_manifest,
    validate_execution_model,
)
from live_engine.execution.models import SignalAction

MANIFESTS = {
    "BTC_ST_09_5M": "benchmarks/manifests/BTC_ST_09_5M.yaml",
    "LIT_SUPERTREND_15M": "benchmarks/manifests/LIT_SUPERTREND_15M.yaml",
    "ZEC_MOMENTUM_M03_15M": "benchmarks/manifests/ZEC_MOMENTUM_M03_15M.yaml",
}


def load(name):
    return yaml.safe_load(open(MANIFESTS[name], encoding="utf-8"))


@pytest.mark.parametrize("name", list(MANIFESTS))
def test_every_manifest_declares_explicit_fill_models(name):
    execution = validate_execution_model(load(name))
    assert execution["entry_fill_model"] in ("signal_close", "next_open")
    assert execution["exit_fill_model"] in ("signal_close", "signal_reference", "next_open")


def test_frozen_fill_models_match_the_approved_benchmarks():
    """These values are read out of the frozen strategies; they must not drift."""
    btc = load("BTC_ST_09_5M")["execution_model"]
    assert (btc["entry_fill_model"], btc["exit_fill_model"]) == ("signal_close", "signal_reference")

    lit = load("LIT_SUPERTREND_15M")["execution_model"]
    assert (lit["entry_fill_model"], lit["exit_fill_model"]) == ("next_open", "signal_reference")

    zec = load("ZEC_MOMENTUM_M03_15M")["execution_model"]
    assert (zec["entry_fill_model"], zec["exit_fill_model"]) == ("next_open", "next_open")


def test_missing_execution_semantics_fails_validation():
    with pytest.raises(ValueError, match="MANIFEST INCOMPLETE"):
        validate_execution_model({"benchmark_id": "X", "execution_model": {}})

    partial = {
        "benchmark_id": "X",
        "execution_model": {"entry_order_type": "market", "exit_order_type": "market",
                            "entry_fill_model": "next_open", "stop_model": "s",
                            "take_profit_model": "t"},
    }
    with pytest.raises(ValueError, match="exit_fill_model"):
        validate_execution_model(partial)


def test_unknown_fill_model_is_rejected():
    bad = {
        "benchmark_id": "X",
        "execution_model": {"entry_order_type": "market", "exit_order_type": "market",
                            "entry_fill_model": "psychic", "exit_fill_model": "next_open",
                            "stop_model": "s", "take_profit_model": "t"},
    }
    with pytest.raises(ValueError, match="not one of"):
        validate_execution_model(bad)


def test_runtime_contract_is_validated_against_the_manifest():
    cfg = LiveEngineConfig(symbol="ZECUSDT", timeframe="15m", mode="SHADOW",
                           warmup_candles=250, max_open_trades=1,
                           stake_rule="full_compounding")
    validate_config_against_manifest(cfg, load("ZEC_MOMENTUM_M03_15M"))

    wrong_stake = LiveEngineConfig(symbol="ZECUSDT", timeframe="15m", mode="SHADOW",
                                   stake_rule="fixed")
    with pytest.raises(ValueError, match="stake rule"):
        validate_config_against_manifest(wrong_stake, load("ZEC_MOMENTUM_M03_15M"))


def test_incomplete_fees_or_leverage_fail_closed():
    manifest = load("ZEC_MOMENTUM_M03_15M")
    cfg = LiveEngineConfig(symbol="ZECUSDT", timeframe="15m", mode="SHADOW")

    no_fees = {**manifest, "fees": {"maker": 0.0002}}
    with pytest.raises(ValueError, match="fees.maker and fees.taker"):
        validate_config_against_manifest(cfg, no_fees)

    no_leverage = {**manifest, "risk": {**manifest["risk"], "leverage": None}}
    with pytest.raises(ValueError, match="risk.leverage"):
        validate_config_against_manifest(cfg, no_leverage)


# --- pending next_open actions ----------------------------------------------

class PendingRig:
    """Minimal orchestrator surface for exercising pending-action handling."""

    def __init__(self, store, symbol="ZECUSDT", timeframe="15m"):
        from live_engine.risk.health import HealthMonitor
        from live_engine.market_data.gap_detector import AggTradeGapDetector

        class Cfg:
            pass

        self.config = Cfg()
        self.config.symbol = symbol
        self.config.timeframe = timeframe
        self.event_store = store
        self.health = HealthMonitor(event_store=store)
        self.gap_detector = AggTradeGapDetector(symbol)
        self.manifest = load("ZEC_MOMENTUM_M03_15M")
        self._latest_strategy_candle = None
        self.submitted = []

    def _execute_pending(self, row, price):
        from live_engine.orchestrator import LiveEngineOrchestrator

        LiveEngineOrchestrator._execute_pending(self, row, price)

    def _submit_entry(self, signal, price, cid_key, latest_candle):
        self.submitted.append(("ENTRY", signal.signal_id, price, cid_key))

    def _submit_exit(self, signal, price, cid_key):
        self.submitted.append(("EXIT", signal.signal_id, price, cid_key))


@pytest.fixture
def rig(tmp_path):
    from live_engine.persistence.event_store import EventStore

    return PendingRig(EventStore(tmp_path / "pending.db"))


def _pending(store, signal_id="SIG-1", signal_open=0, tf_ms=900_000, action="ENTER_LONG"):
    store.save_pending_action({
        "signal_id": signal_id, "symbol": "ZECUSDT", "timeframe": "15m", "action": action,
        "signal_candle_open_time": signal_open,
        "expected_execution_open_time": signal_open + tf_ms,
        "benchmark_reference_price": "100", "quantity": None,
        "state": "PENDING", "created_at": 1,
    })


def test_next_open_signal_at_candle_n_submits_at_candle_n_plus_one(rig):
    from live_engine.orchestrator import LiveEngineOrchestrator

    _pending(rig.event_store, signal_open=0)
    process = LiveEngineOrchestrator.process_pending_actions

    # Still inside the signal candle: nothing fires.
    assert process(rig, 0, Decimal("100")) == []
    assert rig.submitted == []

    # Open of candle N+1: it fires exactly once.
    assert process(rig, 900_000, Decimal("101")) == ["EXECUTED"]
    assert rig.submitted == [("ENTRY", "SIG-1", Decimal("101"), 0)]

    # And never again.
    assert process(rig, 900_000, Decimal("102")) == []
    assert len(rig.submitted) == 1


def test_restored_pending_action_executes_at_most_once_after_restart(rig):
    from live_engine.orchestrator import LiveEngineOrchestrator

    _pending(rig.event_store, signal_open=0)
    # Simulate a restart: a fresh rig reading the same durable store.
    restarted = PendingRig(rig.event_store)
    LiveEngineOrchestrator.process_pending_actions(restarted, 900_000, Decimal("101"))
    assert len(restarted.submitted) == 1

    restarted_again = PendingRig(rig.event_store)
    LiveEngineOrchestrator.process_pending_actions(restarted_again, 900_000, Decimal("101"))
    assert restarted_again.submitted == []


def test_missing_next_open_blocks_rather_than_fabricating_a_fill(rig):
    from live_engine.orchestrator import LiveEngineOrchestrator

    _pending(rig.event_store, signal_open=0)
    rig.health.data_desynced = True
    assert LiveEngineOrchestrator.process_pending_actions(rig, 900_000, Decimal("101")) == ["BLOCKED"]
    assert rig.submitted == []
    # Still pending, still unexecuted: no price was invented.
    assert len(rig.event_store.get_pending_actions()) == 1


def test_unobserved_execution_candle_expires_deterministically(rig):
    from live_engine.orchestrator import LiveEngineOrchestrator

    _pending(rig.event_store, signal_open=0)
    # The first bucket the engine observes is two candles later.
    assert LiveEngineOrchestrator.process_pending_actions(rig, 2_700_000, Decimal("120")) == ["EXPIRED"]
    assert rig.submitted == []
    assert rig.event_store.get_pending_actions() == []
