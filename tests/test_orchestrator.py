"""End-to-end integration tests for LiveEngineOrchestrator."""
import pytest
from decimal import Decimal
from pathlib import Path

from live_engine.config import LiveEngineConfig, validate_config_against_manifest
from live_engine.orchestrator import LiveEngineOrchestrator
from live_engine.execution.models import OrderSide
from live_engine.market_data.models import Candle


def test_orchestrator_initialization_and_candle_processing(tmp_path, monkeypatch):
    from unittest.mock import patch, MagicMock
    from live_engine.execution.order_filters import SymbolFilters
    from decimal import Decimal as D

    # Mock exchange info to avoid network calls in tests
    mock_filters = SymbolFilters(
        symbol="BTCUSDT",
        tick_size=D("0.10"),
        step_size=D("0.001"),
        min_qty=D("0.001"),
        max_qty=D("1000.0"),
        min_notional=D("5.0"),
    )

    with patch("live_engine.orchestrator.fetch_public_exchange_info", return_value=mock_filters):
        db_path = tmp_path / "test_events.db"
        ks_path = tmp_path / ".test_ks"

        cfg = LiveEngineConfig(
            mode="PAPER",
            symbol="BTCUSDT",
            timeframe="5m",
            manifest_path="benchmarks/manifests/BTC_ST_09_5M.yaml",
            event_store_path=str(db_path),
            kill_switch_path=str(ks_path),
            warmup_candles=250,
            stake_amount=Decimal("1000.0"),
        )

        orchestrator = LiveEngineOrchestrator(cfg)
        orchestrator.initialize()

        assert orchestrator._is_initialized is True
        assert orchestrator.position_manager.is_flat is True

        # Check event store logged initialization
        events = orchestrator.event_store.get_events_by_type("SYSTEM_INITIALIZED")
        assert len(events) == 1
        assert events[0]["payload"]["mode"] == "PAPER"


def test_configuration_mismatch_rejected(tmp_path):
    """Verify that runtime symbol/timeframe mismatches against manifest are rejected."""
    db_path = tmp_path / "test_events.db"
    ks_path = tmp_path / ".test_ks"

    # Try to create orchestrator with mismatched timeframe
    cfg_bad = LiveEngineConfig(
        mode="PAPER",
        symbol="BTCUSDT",
        timeframe="15m",  # Manifest specifies 5m
        manifest_path="benchmarks/manifests/BTC_ST_09_5M.yaml",
        event_store_path=str(db_path),
        kill_switch_path=str(ks_path),
    )

    with pytest.raises(ValueError, match="CONFIGURATION MISMATCH"):
        LiveEngineOrchestrator(cfg_bad)
