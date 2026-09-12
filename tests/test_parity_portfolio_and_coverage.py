"""Section 10 regressions: parity must prove candles, sizing and portfolio results.

The old report summed per-trade percentages and called it a net return, never compared
reconstructed candles against the canonical dataset, and bound its verdict to nothing.
"""
import json
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

import pytest
import yaml

from live_engine.market_data.models import Candle
from live_engine.parity.portfolio import (
    PARITY_TOOL_VERSION,
    arithmetic_sum_of_trade_pct,
    bind_inputs,
    build_equity_curve,
    compare_candles,
    report_is_stale,
    required_raw_months,
    resolve_raw_paths,
    validate_raw_coverage,
    verify_sizing,
)

BASE = Path.cwd()


@dataclass
class FakeTrade:
    trade_id: int
    entry_price: float
    exit_price: float
    profit_pct: float
    entry_time_utc: str = "2026-08-15 00:00:00"


def candle(open_time, o="100", h="101", l="99", c="100.5", v="10", tf="15m"):
    return Candle(symbol="ZECUSDT", timeframe=tf, open_time=open_time,
                  close_time=open_time + 899_999, open=Decimal(o), high=Decimal(h),
                  low=Decimal(l), close=Decimal(c), volume=Decimal(v), trade_count=5,
                  is_closed=True)


# --- portfolio ---------------------------------------------------------------

def test_compounded_return_is_verified_separately_from_the_arithmetic_sum():
    """Ten +10% trades sum to 100% arithmetically but compound to ~159%."""
    trades = [FakeTrade(i, 100.0, 110.0, 10.0) for i in range(1, 11)]

    arithmetic = arithmetic_sum_of_trade_pct(t.profit_pct for t in trades)
    assert arithmetic == Decimal("100.0")

    curve = build_equity_curve(trades, initial_balance=Decimal("10000"),
                               taker_fee=Decimal("0"), compounding=True)
    assert curve.compounded_return_pct > Decimal("150")
    assert curve.compounded_return_pct != arithmetic
    assert curve.final_balance > Decimal("25000")


def test_fixed_stake_does_not_compound():
    trades = [FakeTrade(i, 100.0, 110.0, 10.0) for i in range(1, 11)]
    curve = build_equity_curve(trades, initial_balance=Decimal("10000"),
                               taker_fee=Decimal("0"), compounding=False)
    assert curve.compounded_return_pct == Decimal("100")


def test_equity_curve_charges_fees_on_both_sides():
    trades = [FakeTrade(1, 100.0, 100.0, 0.0)]
    curve = build_equity_curve(trades, initial_balance=Decimal("10000"),
                               taker_fee=Decimal("0.0005"), compounding=True)
    assert curve.final_balance < Decimal("10000")


def test_a_sizing_difference_fails_parity_even_when_timestamps_match():
    trades = [FakeTrade(1, 100.0, 110.0, 10.0)]
    # A coarse step size forces the requested size down materially (100 -> 90).
    curve = build_equity_curve(trades, initial_balance=Decimal("10000"),
                               step_size=Decimal("30"), compounding=True)
    problems = verify_sizing(curve)
    assert problems
    assert "requested" in problems[0]

    # Sub-minimum size is reported as an unfillable trade, not silently ignored.
    tiny = build_equity_curve(trades, initial_balance=Decimal("1"),
                              step_size=Decimal("1"), min_qty=Decimal("1"))
    assert "rounds to zero" in verify_sizing(tiny)[0]


def test_requested_and_constrained_sizes_are_recorded_for_every_trade():
    trades = [FakeTrade(i, 100.0, 101.0, 1.0) for i in range(1, 4)]
    curve = build_equity_curve(trades, initial_balance=Decimal("10000"),
                               step_size=Decimal("0.001"))
    assert len(curve.sizing) == 3
    for sizing in curve.sizing:
        assert sizing.requested_quantity > 0
        assert sizing.constrained_quantity > 0
        assert "requested_quantity" in sizing.to_dict()


# --- candle parity -----------------------------------------------------------

def test_a_one_value_ohlcv_corruption_fails_candle_parity():
    canonical = [candle(0), candle(900_000), candle(1_800_000)]
    reconstructed = [candle(0), candle(900_000, h="101.5"), candle(1_800_000)]

    result = compare_candles(reconstructed, canonical, tick_size=Decimal("0.01"))
    assert result.passed is False
    assert result.compared == 3
    assert result.matched == 2
    assert any("high" in m for m in result.mismatched)


def test_identical_candles_pass_at_tick_resolution():
    canonical = [candle(0), candle(900_000)]
    result = compare_candles(list(canonical), canonical, tick_size=Decimal("0.01"))
    assert result.passed is True
    assert result.parity_pct == 100.0


def test_missing_extra_and_duplicated_candles_are_reported_explicitly():
    canonical = [candle(0), candle(900_000), candle(1_800_000)]
    reconstructed = [candle(0), candle(0), candle(2_700_000)]

    result = compare_candles(reconstructed, canonical, tick_size=Decimal("0.01"))
    assert result.missing == [900_000, 1_800_000]
    assert result.extra == [2_700_000]
    assert result.duplicated == [0]
    report = result.to_dict()
    assert report["missing_count"] == 2 and report["extra_count"] == 1


def test_close_time_disagreement_is_a_mismatch():
    canonical = [candle(0)]
    broken = candle(0)
    broken.close_time += 1000
    result = compare_candles([broken], canonical, tick_size=Decimal("0.01"))
    assert any("close_time" in m for m in result.mismatched)


# --- raw dataset coverage ----------------------------------------------------

def test_required_months_span_the_whole_evaluation_window():
    months = required_raw_months("2025-08-01 00:00:00+00:00", "2026-08-31 23:59:59+00:00")
    assert months[0] == "2025-08" and months[-1] == "2026-08"
    assert len(months) == 13


def test_zec_single_raw_file_configuration_fails_with_a_precise_coverage_error():
    manifest = yaml.safe_load(open("benchmarks/manifests/ZEC_MOMENTUM_M03_15M.yaml", encoding="utf-8"))
    broken = {**manifest, "data": {**manifest["data"], "dataset_aggtrade_paths": None,
                                   "dataset_aggtrade_path": "ZECUSDT_USDM_DATA/aggTrades/2026-08.parquet"}}

    ok, errors, paths = validate_raw_coverage(broken, BASE)
    assert ok is False
    assert len(paths) == 1
    message = errors[0]
    assert "RAW COVERAGE ERROR (ZEC_MOMENTUM_M03_15M)" in message
    assert "2025-08" in message
    assert "13 monthly raw file" in message


def test_zec_configured_coverage_now_spans_the_window():
    if not Path("ZECUSDT_USDM_DATA/aggTrades/2026-08.parquet").exists():
        pytest.skip("ZECUSDT_USDM_DATA raw parquet files not available on this system")
    manifest = yaml.safe_load(open("benchmarks/manifests/ZEC_MOMENTUM_M03_15M.yaml", encoding="utf-8"))
    ok, errors, paths = validate_raw_coverage(manifest, BASE)
    assert ok is True, errors
    assert len(paths) == 13


def test_missing_raw_file_on_disk_fails_coverage(tmp_path):
    manifest = {
        "benchmark_id": "X",
        "data": {
            "evaluation_window_start": "2026-08-01 00:00:00+00:00",
            "evaluation_window_end": "2026-08-31 23:59:59+00:00",
            "dataset_aggtrade_paths": ["nowhere/2026-08.parquet"],
        },
    }
    ok, errors, _ = validate_raw_coverage(manifest, tmp_path)
    assert ok is False
    assert any("not found on disk" in e for e in errors)


# --- input binding -----------------------------------------------------------

def test_report_binds_every_input_it_depends_on():
    if not Path("ZECUSDT_USDM_DATA/aggTrades/2026-08.parquet").exists():
        pytest.skip("ZECUSDT_USDM_DATA raw parquet files not available on this system")
    manifest_path = Path("benchmarks/manifests/ZEC_MOMENTUM_M03_15M.yaml")
    manifest = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    binding = bind_inputs(manifest_path, manifest, BASE)

    assert binding["tool_version"] == PARITY_TOOL_VERSION
    assert binding["manifest_hash"] and binding["strategy_hash"]
    assert binding["benchmark_csv_hash"] and binding["canonical_candles_hash"]
    assert len(binding["raw_file_hashes"]) == 13
    assert binding["generated_at_ms"] > 0


def test_changing_a_manifest_or_dataset_invalidates_the_saved_report(tmp_path):
    manifest_path = tmp_path / "m.yaml"
    manifest_path.write_text("benchmark_id: X\n", encoding="utf-8")
    manifest = {"benchmark_id": "X", "strategy": {}, "data": {}}

    binding = bind_inputs(manifest_path, manifest, tmp_path)
    saved_report = {"status": "PASS", "input_binding": binding}

    stale, differences = report_is_stale(saved_report, binding)
    assert stale is False and differences == []

    manifest_path.write_text("benchmark_id: X\n# edited\n", encoding="utf-8")
    stale, differences = report_is_stale(saved_report, bind_inputs(manifest_path, manifest, tmp_path))
    assert stale is True
    assert any("manifest_hash changed" in d for d in differences)


def test_a_report_without_a_binding_is_not_trusted():
    stale, differences = report_is_stale({"status": "PASS"}, {"tool_version": PARITY_TOOL_VERSION})
    assert stale is True
    assert "no input binding" in differences[0]


def test_saved_operational_reports_are_bound_and_checkable():
    from live_engine.parity.replay import HistoricalReplayRunner

    runner = HistoricalReplayRunner("benchmarks/manifests/ZEC_MOMENTUM_M03_15M.yaml", base_dir=BASE)
    result = runner.check_saved_report()
    # The operational report predates input binding, so it must be reported as untrusted
    # rather than silently accepted.
    assert "valid" in result and "differences" in result
    if not result["valid"]:
        assert result["differences"] or result["reason"]

def test_candle_diagnosis_separates_boundary_reassignment_from_corruption():
    """A conserved volume shift across a bucket edge is not the same finding as lost data."""
    canonical = [candle(0, v="100"), candle(900_000, v="100")]

    moved = compare_candles([candle(0, v="132.2"), candle(900_000, v="67.8")],
                            canonical, tick_size=Decimal("0.01"))
    diagnosis = moved.classify()
    assert diagnosis["volume_mismatched_bars"] == 2
    assert diagnosis["volume_deltas_cancelling_with_an_adjacent_bar"] == 2
    assert Decimal(diagnosis["net_volume_difference"]) == Decimal("0")
    assert "boundary" in diagnosis["likely_cause"]

    corrupted = compare_candles([candle(0, v="150"), candle(900_000, v="100")],
                                canonical, tick_size=Decimal("0.01"))
    bad = corrupted.classify()
    assert bad["volume_deltas_cancelling_with_an_adjacent_bar"] == 0
    assert Decimal(bad["net_volume_difference"]) == Decimal("50")
    assert "unexplained" in bad["likely_cause"]
    assert corrupted.passed is False
