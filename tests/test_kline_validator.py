"""Tests for BinanceKlineValidator volume, boundary, and price validation."""
from decimal import Decimal
import tempfile
from pathlib import Path
import pytest

from live_engine.market_data.models import Candle
from live_engine.market_data.binance_kline import BinanceKlineValidator
from live_engine.persistence.event_store import EventStore


def test_kline_validator_volume_discrepancy_mismatch():
    """Verify that a large volume discrepancy (e.g. 1 vs 999) triggers MISMATCH."""
    validator = BinanceKlineValidator(symbol="BTCUSDT", max_volume_tolerance_pct=1.0)

    # Reconstructed candle with volume 1.0
    recon = Candle(
        symbol="BTCUSDT",
        timeframe="1m",
        open_time=1723818600000,
        close_time=1723818659999,
        open=Decimal("60000.0"),
        high=Decimal("60100.0"),
        low=Decimal("59900.0"),
        close=Decimal("60050.0"),
        volume=Decimal("1.0"),
        trade_count=10,
        is_closed=True,
    )
    validator.register_reconstructed_candle(recon)

    # Binance official kline with volume 999.0
    binance_kline = {
        "t": 1723818600000,
        "T": 1723818659999,
        "o": "60000.0",
        "h": "60100.0",
        "l": "59900.0",
        "c": "60050.0",
        "v": "999.0",
        "x": True,
    }

    result = validator.validate_closed_kline(binance_kline)
    assert result is not None
    assert result.status == "MISMATCH"
    assert "Vol diff" in result.discrepancy_details
    assert validator.has_active_mismatch is True


def test_kline_validator_close_boundary_mismatch():
    """Verify that incorrect close boundary triggers MISMATCH."""
    validator = BinanceKlineValidator(symbol="BTCUSDT")

    recon = Candle(
        symbol="BTCUSDT",
        timeframe="1m",
        open_time=1723818600000,
        close_time=1723818659999,
        open=Decimal("60000.0"),
        high=Decimal("60100.0"),
        low=Decimal("59900.0"),
        close=Decimal("60050.0"),
        volume=Decimal("10.0"),
        trade_count=10,
        is_closed=True,
    )
    validator.register_reconstructed_candle(recon)

    # Binance official kline with wrong close timestamp (e.g. 5 minutes later)
    binance_kline = {
        "t": 1723818600000,
        "T": 1723818900000,
        "o": "60000.0",
        "h": "60100.0",
        "l": "59900.0",
        "c": "60050.0",
        "v": "10.0",
        "x": True,
    }

    result = validator.validate_closed_kline(binance_kline)
    assert result is not None
    assert result.status == "MISMATCH"
    assert "Close boundary diff" in result.discrepancy_details


def test_kline_validator_perfect_match():
    """Verify that matching OHLCV and boundary yields MATCH status."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        store = EventStore(Path(tmp_dir) / "test.db")
        validator = BinanceKlineValidator(symbol="BTCUSDT", event_store=store)

        recon = Candle(
            symbol="BTCUSDT",
            timeframe="1m",
            open_time=1723818600000,
            close_time=1723818659999,
            open=Decimal("60000.0"),
            high=Decimal("60100.0"),
            low=Decimal("59900.0"),
            close=Decimal("60050.0"),
            volume=Decimal("10.0"),
            trade_count=10,
            is_closed=True,
        )
        validator.register_reconstructed_candle(recon)

        binance_kline = {
            "t": 1723818600000,
            "T": 1723818659999,
            "o": "60000.0",
            "h": "60100.0",
            "l": "59900.0",
            "c": "60050.0",
            "v": "10.0",
            "x": True,
        }

        result = validator.validate_closed_kline(binance_kline)
        assert result is not None
        assert result.status == "MATCH"
        assert validator.has_active_mismatch is False
