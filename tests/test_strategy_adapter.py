"""Unit tests for StrategyLoader, StrategyAdapter, and SignalEngine."""
import pytest
from pathlib import Path
from decimal import Decimal

from live_engine.strategy.loader import StrategyLoader, ConfigurationMismatchError
from live_engine.strategy.adapter import StrategyAdapter
from live_engine.strategy.signal_engine import SignalEngine
from live_engine.execution.models import SignalAction
from live_engine.market_data.models import Candle

MANIFEST_PATH = Path("benchmarks/manifests/BTC_ST_09_5M.yaml")


def test_strategy_loader_valid():
    strat_instance, manifest = StrategyLoader.load_strategy(MANIFEST_PATH)
    assert strat_instance is not None
    assert manifest["benchmark_id"] == "BTC_ST_09_5M"
    assert manifest["strategy"]["class"] == "BTCSupertrendPullback5M"


def test_strategy_loader_hash_mismatch(tmp_path):
    # Create fake manifest with incorrect hash
    fake_manifest = tmp_path / "fake_manifest.yaml"
    fake_manifest.write_text("""
benchmark_id: TEST_MISMATCH
exchange: binance
market_type: usdm_futures
symbol: BTCUSDT
strategy:
  name: BTCSupertrendPullback5M
  class: BTCSupertrendPullback5M
  source_file: strategies/approved/BTCSupertrendPullback5M.py
  source_hash: "0000000000000000000000000000000000000000000000000000000000000000"
data:
  strategy_timeframe: 5m
signal:
  warmup_candles: 250
""")
    with pytest.raises(ConfigurationMismatchError):
        StrategyLoader.load_strategy(fake_manifest)


def test_strategy_adapter_and_signal_engine():
    strat_instance, manifest = StrategyLoader.load_strategy(MANIFEST_PATH)
    adapter = StrategyAdapter(strat_instance, manifest)
    engine = SignalEngine(adapter)

    # If candles < 250, engine emits nothing
    c1 = Candle(
        symbol="BTCUSDT",
        timeframe="5m",
        open_time=1786752000000,
        close_time=1786752299999,
        open=Decimal("63000.00"),
        high=Decimal("63100.00"),
        low=Decimal("62900.00"),
        close=Decimal("63050.00"),
        volume=Decimal("10.0"),
        is_closed=True,
    )
    sig = engine.on_candle_close(c1)
    assert sig is None
