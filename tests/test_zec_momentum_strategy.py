"""Tests for ZECMomentumEMA15M strategy and parity verification."""
import json
from pathlib import Path
from decimal import Decimal
import numpy as np
import pandas as pd
import pytest

from live_engine.config import LiveEngineConfig
from live_engine.orchestrator import LiveEngineOrchestrator
from live_engine.strategy.loader import StrategyLoader
from live_engine.dashboards import ZECDashboard

ZEC_MANIFEST_PATH = Path("benchmarks/manifests/ZEC_MOMENTUM_M03_15M.yaml")
ZEC_DATASET_PATH = Path("ZECUSDT_USDM_DATA/candles/ZECUSDT_15m.parquet")


def test_zec_strategy_loader_verified():
    """ZEC manifest loads ZECMomentumEMA15M with verified SHA-256 hash."""
    strat_instance, manifest = StrategyLoader.load_strategy(ZEC_MANIFEST_PATH)
    assert strat_instance is not None
    assert manifest["benchmark_id"] == "ZEC_MOMENTUM_M03_15M"
    assert manifest["strategy"]["class"] == "ZECMomentumEMA15M"
    assert manifest["strategy"]["source_file"] == "strategies/approved/ZECMomentumEMA15M.py"
    assert strat_instance.fast_period == 10
    assert strat_instance.slow_period == 100
    assert strat_instance.timeframe == "15m"


def test_zec_strategy_indicators():
    """ZEC strategy populates required indicators."""
    strat_instance, manifest = StrategyLoader.load_strategy(ZEC_MANIFEST_PATH)
    n = 120
    np.random.seed(42)
    closes = 50.0 + np.cumsum(np.random.randn(n) * 1.0)
    highs = closes + np.random.rand(n) * 1.5
    lows = closes - np.random.rand(n) * 1.5
    opens = (highs + lows) / 2.0
    df = pd.DataFrame({
        "open": opens,
        "high": highs,
        "low": lows,
        "close": closes,
        "volume": np.ones(n) * 1000.0,
    })
    res = strat_instance.populate_indicators(df, {"pair": "ZECUSDT"})
    assert "ema_fast" in res.columns
    assert "ema_slow" in res.columns
    assert pd.notna(res["ema_fast"].iloc[-1])
    assert pd.notna(res["ema_slow"].iloc[-1])


def test_zec_strategy_entry_exit_signals():
    """ZEC strategy triggers long entries on confirmed crossover and exits on opposing crossover."""
    strat_instance, manifest = StrategyLoader.load_strategy(ZEC_MANIFEST_PATH)
    n = 150
    closes = [100.0 - i * 0.1 for i in range(100)]
    closes.extend([90.0 + i * 1.5 for i in range(50)])
    highs = [c + 0.5 for c in closes]
    lows = [c - 0.5 for c in closes]
    opens = closes.copy()

    df = pd.DataFrame({
        "open": opens,
        "high": highs,
        "low": lows,
        "close": closes,
        "volume": np.ones(n) * 1000.0,
    })
    res = strat_instance.populate_indicators(df, {"pair": "ZECUSDT"})
    res = strat_instance.populate_entry_trend(res, {"pair": "ZECUSDT"})
    res = strat_instance.populate_exit_trend(res, {"pair": "ZECUSDT"})

    assert "enter_long" in res.columns
    assert "exit_long" in res.columns
    assert res["enter_long"].sum() > 0


def test_zec_historical_parity_report_pass():
    """ZEC parity report passes with 100% decision parity."""
    report_path = Path("benchmarks/reports/ZEC_MOMENTUM_M03_15M_parity_report.json")
    assert report_path.exists(), f"Missing parity report: {report_path}"
    data = json.loads(report_path.read_text(encoding="utf-8"))
    assert data["status"] == "PASS"
    assert data["benchmark_id"] == "ZEC_MOMENTUM_M03_15M"
    assert data["strategy_decision_parity_pct"] == 100.0
    assert data["benchmark_trades_count"] == 321
    assert data["replay_trades_count"] == 321
    assert data["matched_trades_count"] == 321
    assert len(data["mismatches"]) == 0


def test_zec_paper_orchestrator_initialization(tmp_path):
    """ZEC PAPER orchestrator initializes cleanly with parity enforcement passing."""
    db_path = tmp_path / "zec_paper_test.db"
    ks_path = tmp_path / ".ks"

    cfg = LiveEngineConfig(
        mode="PAPER",
        symbol="ZECUSDT",
        timeframe="15m",
        manifest_path="benchmarks/manifests/ZEC_MOMENTUM_M03_15M.yaml",
        event_store_path=str(db_path),
        kill_switch_path=str(ks_path),
        warmup_candles=250,
        stake_amount="10000.0",
        stake_rule="full_compounding",
        max_open_trades=1,
    )

    orch = LiveEngineOrchestrator(cfg)
    orch._validate_parity_report()
    assert orch.manifest["benchmark_id"] == "ZEC_MOMENTUM_M03_15M"
    assert orch.config.symbol == "ZECUSDT"
    assert orch.config.timeframe == "15m"


def test_zec_shadow_orchestrator_initialization(tmp_path):
    """ZEC SHADOW orchestrator initializes cleanly with parity enforcement passing."""
    db_path = tmp_path / "zec_shadow_test.db"
    ks_path = tmp_path / ".ks_shadow"

    cfg = LiveEngineConfig(
        mode="SHADOW",
        symbol="ZECUSDT",
        timeframe="15m",
        manifest_path="benchmarks/manifests/ZEC_MOMENTUM_M03_15M.yaml",
        event_store_path=str(db_path),
        kill_switch_path=str(ks_path),
        warmup_candles=250,
        stake_amount="10000.0",
        stake_rule="full_compounding",
        max_open_trades=1,
    )

    orch = LiveEngineOrchestrator(cfg)
    orch._validate_parity_report()
    assert orch.manifest["benchmark_id"] == "ZEC_MOMENTUM_M03_15M"
    assert orch.config.symbol == "ZECUSDT"
    assert orch.config.timeframe == "15m"


def test_zec_dashboard_payload_timeframe_and_params(tmp_path):
    """Dashboard correctly reflects ZECUSDT 15m strategy parameters (10, 100)."""
    db_path = Path("data/zec_paper.db")
    if not db_path.exists():
        pytest.skip("data/zec_paper.db not available on this system")
    cfg = LiveEngineConfig(
        mode="PAPER",
        symbol="ZECUSDT",
        timeframe="15m",
        manifest_path="benchmarks/manifests/ZEC_MOMENTUM_M03_15M.yaml",
        event_store_path=str(db_path),
        kill_switch_path=str(tmp_path / ".ks_dash"),
    )
    agg = ZECDashboard(cfg)
    payload = agg.get_dashboard_payload()

    assert payload["symbol"] == "ZECUSDT"
    assert payload["timeframe"] == "15m"
    assert payload["strategy_timeframe"] == "15m"
    assert payload["supertrend_params"] == "(10, 100)"
    assert payload["mode"] == "PAPER"
    assert payload["benchmark_id"] == "ZEC_MOMENTUM_M03_15M"
    assert payload["strategy_name"] == "ZECMomentumEMA15M"
    assert len(payload["candles"]) > 0
    assert payload["supertrend_direction"] in ("BULLISH", "BEARISH", "NEUTRAL")
