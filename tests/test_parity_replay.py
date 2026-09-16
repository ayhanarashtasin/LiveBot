"""Unit and regression tests for HistoricalReplayRunner and Parity verification."""
import copy
import pytest
from pathlib import Path

from live_engine.parity.replay import HistoricalReplayRunner

MANIFEST_PATH = Path("benchmarks/manifests/BTC_ST_09_5M.yaml")
DATASET_PATH = Path("BTCUSDT_USDM_DATA/candles/BTCUSDT_5m.parquet")


@pytest.fixture(scope="module")
def replay_context():
    if not DATASET_PATH.exists():
        pytest.skip(f"Historical dataset {DATASET_PATH} not present")
    runner = HistoricalReplayRunner(MANIFEST_PATH)
    try:
        warmup, evals = runner.load_dataset(DATASET_PATH)
    except (FileNotFoundError, ValueError) as exc:
        pytest.skip(f"Historical evaluation dataset not present in {DATASET_PATH}: {exc}")
    trades = runner.run_replay(warmup, evals)
    return runner, trades, warmup, evals


def test_historical_parity_100_percent(replay_context):
    runner, trades, warmup, evals = replay_context

    assert len(warmup) == 250
    assert len(evals) == 4896  # 17 days * 24h * 12 bars/h

    report = runner.verify_parity(trades)

    assert report.status == "PASS"
    assert report.benchmark_trades_count == 12
    assert report.replay_trades_count == 12
    assert report.matched_trades_count == 12
    assert report.candle_timestamp_parity_pct == 100.0
    assert report.entry_signal_parity_pct == 100.0
    assert report.exit_signal_parity_pct == 100.0
    assert report.trade_direction_parity_pct == 100.0
    assert report.strategy_decision_parity_pct == 100.0
    assert abs(report.replay_net_return_pct - report.benchmark_net_return_pct) < 0.01
    assert len(report.mismatches) == 0


def test_parity_fails_on_corrupted_signal_timing(replay_context):
    runner, trades, _, _ = replay_context

    # Corrupt trade 1 signal timing
    corrupted_trades = copy.deepcopy(trades)
    corrupted_trades[0].entry_signal_candle_utc = "2026-08-16 00:00:00+00:00"

    report = runner.verify_parity(corrupted_trades)
    assert report.status == "FAIL"
    assert any("signal timing mismatch" in m for m in report.mismatches)


def test_parity_fails_on_wrong_exit_reason(replay_context):
    runner, trades, _, _ = replay_context

    # Corrupt exit reason
    corrupted_trades = copy.deepcopy(trades)
    corrupted_trades[0].exit_reason = "stoploss"

    report = runner.verify_parity(corrupted_trades)
    assert report.status == "FAIL"
    assert any("exit reason mismatch" in m for m in report.mismatches)


def test_parity_fails_on_return_discrepancy(replay_context):
    runner, trades, _, _ = replay_context

    # Corrupt profit
    corrupted_trades = copy.deepcopy(trades)
    corrupted_trades[0].profit_pct = 50.0

    report = runner.verify_parity(corrupted_trades)
    assert report.status == "FAIL"
    assert any("profit mismatch" in m or "return mismatch" in m for m in report.mismatches)
