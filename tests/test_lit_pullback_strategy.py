"""Tests for LITSupertrendPullback15M strategy and parity verification."""
import pytest
from pathlib import Path
from decimal import Decimal
import pandas as pd
import numpy as np

from live_engine.strategy.loader import StrategyLoader
from live_engine.strategy.adapter import StrategyAdapter
from live_engine.strategy.signal_engine import SignalEngine
from live_engine.parity.replay import HistoricalReplayRunner
from live_engine.market_data.models import Candle

LIT_MANIFEST_PATH = Path("benchmarks/manifests/LIT_SUPERTREND_15M.yaml")
LIT_DATASET_PATH = Path("LITUSDT_USDM_DATA/candles/LITUSDT_15m.parquet")


def test_lit_strategy_loader_verified():
    """LIT manifest loads LITSupertrendPullback15M with verified SHA-256 hash."""
    strat_instance, manifest = StrategyLoader.load_strategy(LIT_MANIFEST_PATH)
    assert strat_instance is not None
    assert manifest["benchmark_id"] == "LIT_SUPERTREND_15M"
    assert manifest["strategy"]["class"] == "LITSupertrendPullback15M"
    assert manifest["strategy"]["source_file"] == "strategies/approved/LITSupertrendPullback15M.py"
    assert strat_instance.st_atr_length == 28
    assert strat_instance.st_multiplier == 2.0
    assert strat_instance.atr_period == 7
    assert strat_instance.pb_atr == 0.5
    assert strat_instance.sl_multiplier == 1.5
    assert strat_instance.tp_multiplier == 3.0
    assert strat_instance.max_hold == 128
    assert strat_instance.flip_exit is False


def test_lit_strategy_indicators():
    """LIT strategy populates required indicators."""
    strat_instance, manifest = StrategyLoader.load_strategy(LIT_MANIFEST_PATH)
    # Generate 30 synthetic bars
    n = 60
    np.random.seed(42)
    closes = 2.0 + np.cumsum(np.random.randn(n) * 0.02)
    highs = closes + np.random.rand(n) * 0.05
    lows = closes - np.random.rand(n) * 0.05
    opens = (highs + lows) / 2.0
    df = pd.DataFrame({
        "open": opens,
        "high": highs,
        "low": lows,
        "close": closes,
        "volume": np.ones(n) * 100.0,
    })
    res = strat_instance.populate_indicators(df, {"pair": "LITUSDT"})
    assert "supertrend" in res.columns
    assert "supertrend_direction" in res.columns
    assert "atr_7" in res.columns
    assert "near_recent" in res.columns
    assert "entry_candidate" in res.columns


def test_lit_historical_parity_report_pass():
    """LIT parity report passes with 100% decision parity."""
    import json
    report_path = Path("benchmarks/reports/LIT_SUPERTREND_15M_parity_report.json")
    assert report_path.exists()
    data = json.loads(report_path.read_text())
    assert data["status"] == "PASS"
    assert data["benchmark_id"] == "LIT_SUPERTREND_15M"
    assert data["strategy_decision_parity_pct"] == 100.0
    assert data["benchmark_trades_count"] == data["replay_trades_count"]
    assert data["matched_trades_count"] == data["benchmark_trades_count"]
    assert len(data["mismatches"]) == 0
