"""Unit tests for RiskGuardEngine and KillSwitch."""
import pytest
import tempfile
from decimal import Decimal
from pathlib import Path
from datetime import datetime, timezone

from live_engine.risk.kill_switch import KillSwitch
from live_engine.risk.guards import RiskGuardEngine
from live_engine.execution.position_manager import PositionManager
from live_engine.execution.models import OrderSide
from live_engine.market_data.models import Candle


@pytest.fixture
def temp_ks_path(tmp_path):
    return tmp_path / ".kill_switch"


def test_kill_switch_lifecycle(temp_ks_path):
    ks = KillSwitch(temp_ks_path)
    assert ks.is_engaged() is False

    # Trigger
    payload = ks.trigger(reason="Test trigger", triggered_by="TEST_RUNNER")
    assert ks.is_engaged() is True
    assert payload["engaged"] is True

    # Check status
    status = ks.get_status()
    assert status["engaged"] is True
    assert status["reason"] == "Test trigger"

    # Disengage
    ks.disengage(reason="Test release")
    assert ks.is_engaged() is False


def test_kill_switch_corrupted_file_fails_closed(temp_ks_path):
    temp_ks_path.write_text("INVALID JSON", encoding="utf-8")
    ks = KillSwitch(temp_ks_path)
    # Fail closed behavior
    assert ks.is_engaged() is True


def test_risk_guard_engine_checks(temp_ks_path):
    ks = KillSwitch(temp_ks_path)
    risk = RiskGuardEngine(
        kill_switch=ks,
        max_open_positions=1,
        max_order_notional_usd=Decimal("10000.0"),
        max_candle_staleness_sec=1800,
        max_price_deviation_pct=Decimal("2.0"),
    )
    pm = PositionManager("BTCUSDT")

    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    valid_candle = Candle(
        symbol="BTCUSDT",
        timeframe="15m",
        open_time=now_ms - 900000,
        close_time=now_ms,
        open=Decimal("60000.0"),
        high=Decimal("60500.0"),
        low=Decimal("59900.0"),
        close=Decimal("60100.0"),
        volume=Decimal("100.0"),
        trade_count=1000,
    )

    # 1. Healthy Entry
    res = risk.evaluate_entry(
        symbol="BTCUSDT",
        side=OrderSide.BUY,
        quantity=Decimal("0.1"),
        price=Decimal("60100.0"),
        position_manager=pm,
        latest_candle=valid_candle,
        has_active_gaps=False,
    )
    assert res.passed is True

    # 2. Block on Kill Switch
    ks.trigger("Emergency halt")
    res_ks = risk.evaluate_entry(
        symbol="BTCUSDT",
        side=OrderSide.BUY,
        quantity=Decimal("0.1"),
        price=Decimal("60100.0"),
        position_manager=pm,
        latest_candle=valid_candle,
        has_active_gaps=False,
    )
    assert res_ks.passed is False
    assert "Kill switch" in res_ks.reason
    ks.disengage("Cleared")

    # 3. Block on Active Gaps
    res_gap = risk.evaluate_entry(
        symbol="BTCUSDT",
        side=OrderSide.BUY,
        quantity=Decimal("0.1"),
        price=Decimal("60100.0"),
        position_manager=pm,
        latest_candle=valid_candle,
        has_active_gaps=True,
    )
    assert res_gap.passed is False
    assert "gap" in res_gap.reason

    # 4. Block on Max Notional ($60,100 > $10,000 max)
    res_notional = risk.evaluate_entry(
        symbol="BTCUSDT",
        side=OrderSide.BUY,
        quantity=Decimal("1.0"),
        price=Decimal("60100.0"),
        position_manager=pm,
        latest_candle=valid_candle,
        has_active_gaps=False,
    )
    assert res_notional.passed is False
    assert "notional" in res_notional.reason

    # 5. Block on Price Deviation (> 2%)
    res_dev = risk.evaluate_entry(
        symbol="BTCUSDT",
        side=OrderSide.BUY,
        quantity=Decimal("0.1"),
        price=Decimal("62000.0"),  # > 3% deviation from 60100
        position_manager=pm,
        latest_candle=valid_candle,
        has_active_gaps=False,
    )
    assert res_dev.passed is False
    assert "deviation" in res_dev.reason

    # 6. Block on Position Limit (already open)
    pm.on_fill(OrderSide.BUY, Decimal("0.1"), Decimal("60100.0"))
    res_pos = risk.evaluate_entry(
        symbol="BTCUSDT",
        side=OrderSide.BUY,
        quantity=Decimal("0.1"),
        price=Decimal("60100.0"),
        position_manager=pm,
        latest_candle=valid_candle,
        has_active_gaps=False,
    )
    assert res_pos.passed is False
    assert "already open" in res_pos.reason
