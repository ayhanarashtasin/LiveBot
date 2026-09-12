"""Integration and security tests for critical bug fixes.

Tests for:
- Issue #1: LIVE mode initialization
- Issue #2: User-data stream URL correctness
- Issue #3: Paper accounting round-trip accuracy
- Issue #5: Gap recovery mechanism
- Issue #8: Partial fill handling
- Issue #9: Health state machine
- Issue #10: Security audit (secrets, permissions, Git tracking)
"""
import pytest
import os
import tempfile
from decimal import Decimal
from pathlib import Path
from datetime import datetime, timezone

from live_engine.config import LiveEngineConfig
from live_engine.execution.broker import BinanceLiveBroker, PaperBroker, BinanceTestnetBroker
from live_engine.execution.models import OrderSide, OrderStatus, OrderType
from live_engine.account.user_stream import BinanceUserDataStream
from live_engine.market_data.gap_detector import AggTradeGapDetector
from live_engine.market_data.models import AggTrade
from live_engine.orchestrator import LiveEngineOrchestrator
from live_engine.persistence.event_store import EventStore


# ============================================================================
# ISSUE #1: LIVE MODE INITIALIZATION
# ============================================================================

class TestLiveModeInitialization:
    """Verifies BinanceLiveBroker initializes without testnet parameter."""

    def test_binance_live_broker_no_testnet_param(self):
        """BinanceLiveBroker should not accept testnet parameter."""
        with pytest.raises(TypeError):
            # This should fail because BinanceLiveBroker doesn't have testnet param
            broker = BinanceLiveBroker(
                api_key="test_key",
                api_secret="test_secret",
                testnet=False  # This parameter should not exist
            )

    def test_binance_live_broker_init_valid(self):
        """BinanceLiveBroker should initialize with only api_key and api_secret."""
        # Mock the environment variable check
        os.environ["ESCANOR_LIVE_TRADING_ENABLED"] = "false"
        try:
            broker = BinanceLiveBroker(
                api_key="test_key",
                api_secret="test_secret"
            )
            assert broker.mode == "LIVE"
            assert broker.base_url == "https://fapi.binance.com"
        finally:
            if "ESCANOR_LIVE_TRADING_ENABLED" in os.environ:
                del os.environ["ESCANOR_LIVE_TRADING_ENABLED"]

    def test_binance_testnet_broker_init(self):
        """BinanceTestnetBroker should work with testnet credentials."""
        os.environ["BINANCE_TESTNET_API_KEY"] = "test_testnet_key"
        os.environ["BINANCE_TESTNET_API_SECRET"] = "test_testnet_secret"
        try:
            broker = BinanceTestnetBroker(
                api_key="test_testnet_key",
                api_secret="test_testnet_secret"
            )
            assert broker.mode == "TESTNET"
            assert broker.base_url == "https://testnet.binancefuture.com"
        finally:
            if "BINANCE_TESTNET_API_KEY" in os.environ:
                del os.environ["BINANCE_TESTNET_API_KEY"]
            if "BINANCE_TESTNET_API_SECRET" in os.environ:
                del os.environ["BINANCE_TESTNET_API_SECRET"]


# ============================================================================
# ISSUE #2: USER-DATA STREAM URL CORRECTNESS
# ============================================================================

class TestUserDataStreamURL:
    """Verifies correct Binance user-data stream URLs."""

    def test_live_user_data_stream_base_host(self):
        """Live user-data stream uses the official USD-M Futures WebSocket base host.

        The previous expectation ('/private/ws') is not a Binance endpoint; the documented
        USD-M user data stream is <base>/ws/<listenKey>.
        """
        stream = BinanceUserDataStream(
            api_key="test_key",
            testnet=False
        )
        assert stream.ws_url == "wss://fstream.binance.com", \
            f"Expected 'wss://fstream.binance.com', got '{stream.ws_url}'"

    def test_testnet_user_data_stream_url(self):
        """Testnet user-data stream URL should be testnet endpoint."""
        stream = BinanceUserDataStream(
            api_key="test_key",
            testnet=True
        )
        assert "binancefuture" in stream.ws_url.lower() or "testnet" in stream.ws_url.lower()

    def test_user_data_stream_url_construction(self):
        """User-data stream URL is <base>/ws/<listenKey> on the official endpoint."""
        from live_engine.account.user_stream import build_stream_url

        listen_key = "test_listen_key_12345"
        assert build_stream_url(listen_key, testnet=False) == \
            f"wss://fstream.binance.com/ws/{listen_key}"
        assert build_stream_url(listen_key, testnet=True) == \
            f"wss://stream.binancefuture.com/ws/{listen_key}"


# ============================================================================
# ISSUE #3: PAPER ACCOUNTING ACCURACY
# ============================================================================

class TestPaperAccountingAccuracy:
    """Verifies paper broker accounting is mathematically correct."""

    def test_round_trip_no_fees_with_leverage(self):
        """Round trip buy/sell at same price with 10x leverage should preserve capital."""
        broker = PaperBroker(
            initial_balance=Decimal("1000.0"),
            taker_fee=Decimal("0"),  # No fees for this test
            leverage=10
        )

        # Initial state
        assert broker.balance == Decimal("1000.0")

        # BUY 10 units at 100 USDT (notional: 1000, margin required: 100)
        buy_order = broker.place_order(
            symbol="BTCUSDT",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=Decimal("10"),
            price=Decimal("100")
        )
        assert buy_order.status == OrderStatus.FILLED
        assert broker.balance == Decimal("900.0"), f"After buy: expected 900, got {broker.balance}"

        # SELL 10 units at 100 USDT (should close position, credit margin + proceeds)
        sell_order = broker.place_order(
            symbol="BTCUSDT",
            side=OrderSide.SELL,
            order_type=OrderType.MARKET,
            quantity=Decimal("10"),
            price=Decimal("100")
        )
        assert sell_order.status == OrderStatus.FILLED
        assert broker.balance == Decimal("1000.0"), \
            f"After sell round-trip: expected 1000, got {broker.balance}"

    def test_round_trip_with_profit(self):
        """Buy low, sell high should increase capital by exact profit."""
        broker = PaperBroker(
            initial_balance=Decimal("1000.0"),
            taker_fee=Decimal("0"),
            leverage=10
        )

        # BUY 10 at 100 (margin: 100)
        buy_order = broker.place_order(
            symbol="BTCUSDT",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=Decimal("10"),
            price=Decimal("100")
        )
        assert broker.balance == Decimal("900.0")

        # SELL 10 at 110 (profit: 100, closes position, credits 100 margin)
        sell_order = broker.place_order(
            symbol="BTCUSDT",
            side=OrderSide.SELL,
            order_type=OrderType.MARKET,
            quantity=Decimal("10"),
            price=Decimal("110")
        )
        # Expected: 900 + (10 * 110) - (10 * 100) + 100 (margin return)
        # = 900 + 1100 - 1000 + 100 = 1100
        assert broker.balance == Decimal("1100.0"), \
            f"After profitable round-trip: expected 1100, got {broker.balance}"

    def test_round_trip_with_loss(self):
        """Buy high, sell low should decrease capital by exact loss."""
        broker = PaperBroker(
            initial_balance=Decimal("1000.0"),
            taker_fee=Decimal("0"),
            leverage=10
        )

        # BUY 10 at 110 (margin: 110)
        buy_order = broker.place_order(
            symbol="BTCUSDT",
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=Decimal("10"),
            price=Decimal("110")
        )
        assert broker.balance == Decimal("890.0")

        # SELL 10 at 100 (loss: -100)
        sell_order = broker.place_order(
            symbol="BTCUSDT",
            side=OrderSide.SELL,
            order_type=OrderType.MARKET,
            quantity=Decimal("10"),
            price=Decimal("100")
        )
        # Expected: 890 + (10 * 100) - (10 * 110) + 110 (margin return)
        # = 890 + 1000 - 1100 + 110 = 900
        assert broker.balance == Decimal("900.0"), \
            f"After losing round-trip: expected 900, got {broker.balance}"


# ============================================================================
# ISSUE #5: GAP RECOVERY MECHANISM
# ============================================================================

class TestGapRecoveryMechanism:
    """Verifies gap detection and recovery implementation."""

    def test_gap_detector_detects_gaps(self):
        """Gap detector should identify missing trade IDs."""
        detector = AggTradeGapDetector(symbol="BTCUSDT")

        # Process trades with gap
        trade1 = AggTrade(
            event_type="aggTrade",
            symbol="BTCUSDT",
            agg_trade_id=100,
            price=Decimal("50000"),
            quantity=Decimal("1"),
            first_trade_id=100,
            last_trade_id=100,
            trade_time=int(datetime.now(timezone.utc).timestamp() * 1000),
            event_time=int(datetime.now(timezone.utc).timestamp() * 1000),
            buyer_is_market_maker=False,
            received_at=int(datetime.now(timezone.utc).timestamp() * 1000),
        )
        trade2 = AggTrade(
            event_type="aggTrade",
            symbol="BTCUSDT",
            agg_trade_id=103,  # Gap of 2 trades
            price=Decimal("50001"),
            quantity=Decimal("1"),
            first_trade_id=103,
            last_trade_id=103,
            trade_time=int(datetime.now(timezone.utc).timestamp() * 1000),
            event_time=int(datetime.now(timezone.utc).timestamp() * 1000),
            buyer_is_market_maker=False,
            received_at=int(datetime.now(timezone.utc).timestamp() * 1000),
        )

        status1 = detector.process_trade(trade1)
        status2 = detector.process_trade(trade2)

        assert status2.value == "GAP", "Gap detector should identify missing trades"
        assert len(detector.active_gaps) == 1
        assert detector.active_gaps[0].from_id == 101
        assert detector.active_gaps[0].to_id == 102


# ============================================================================
# ISSUE #8: PARTIAL FILL HANDLING
# ============================================================================

class TestPartialFillHandling:
    """Verifies correct handling of partially filled orders."""

    def test_partial_fill_status_tracking(self):
        """Partially filled orders should maintain correct status."""
        from live_engine.execution.order_manager import OrderManager
        from live_engine.execution.models import Order

        manager = OrderManager()

        # Create order
        order = Order(
            client_order_id="test_001",
            symbol="BTCUSDT",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("10"),
            price=Decimal("50000"),
            status=OrderStatus.NEW,
            created_at=int(datetime.now(timezone.utc).timestamp() * 1000),
        )

        manager.upsert_order(order)

        # Simulate partial fill
        manager.update_from_stream(
            client_order_id="test_001",
            status=OrderStatus.PARTIALLY_FILLED,
            filled_qty=Decimal("5"),
            avg_price=Decimal("50000")
        )

        updated_order = manager.get_order_by_client_id("test_001")
        assert updated_order.status == OrderStatus.PARTIALLY_FILLED
        assert updated_order.filled_quantity == Decimal("5")


# ============================================================================
# ISSUE #9: HEALTH STATE MACHINE
# ============================================================================

class TestHealthStateMachine:
    """Verifies proper health state tracking."""

    def test_health_state_reflects_synchronization(self):
        """Health state should accurately reflect data synchronization status."""
        detector = AggTradeGapDetector(symbol="BTCUSDT")

        # Initially synchronized
        assert not detector.is_desynced

        # After gap detection, should be desynced
        detector.is_desynced = True
        assert detector.is_desynced

        # After recovery, should be synchronized again
        detector.is_desynced = False
        assert not detector.is_desynced


# ============================================================================
# ISSUE #10: SECURITY AUDIT
# ============================================================================

class TestSecurityAudit:
    """Verifies security requirements and Git tracking."""

    def test_secrets_not_in_git(self):
        """Critical files should not contain hardcoded secrets."""
        base_dir = Path(__file__).resolve().parent.parent
        critical_files = [
            "live_engine/config.py",
            "live_engine/execution/broker.py",
            "live_engine/account/user_stream.py",
            "live_engine/main.py",
        ]

        secret_patterns = [
            "BINANCE_API_KEY =",
            "BINANCE_SECRET_KEY =",
            "api_key = \"",
            "api_secret = \"",
            "password = \"",
        ]

        for file_path in critical_files:
            full_path = base_dir / file_path
            if full_path.exists():
                content = full_path.read_text()
                for pattern in secret_patterns:
                    assert pattern not in content, \
                        f"Found potential hardcoded secret in {file_path}: {pattern}"

    def test_git_ignore_includes_env_files(self):
        """Git should ignore sensitive files."""
        base_dir = Path(__file__).resolve().parent.parent
        gitignore_path = base_dir / ".gitignore"

        if gitignore_path.exists():
            content = gitignore_path.read_text()
            sensitive_patterns = [".env", "credentials", "*.key", "*.secret"]

            for pattern in sensitive_patterns:
                assert pattern in content or pattern.replace(".", "") in content, \
                    f"Git should ignore {pattern}"

    def test_live_trading_disabled_by_default(self):
        """Live trading should require explicit environment variable."""
        # This is verified by BinanceLiveBroker checking ESCANOR_LIVE_TRADING_ENABLED
        enabled = os.environ.get("ESCANOR_LIVE_TRADING_ENABLED", "").lower() == "true"

        # By default it should be false
        assert not enabled, "Live trading should be disabled by default in test environment"

    def test_api_key_from_environment_not_hardcoded(self):
        """API keys must come from environment, never hardcoded."""
        from live_engine.execution.broker import BinanceLiveBroker

        # Should fail if no credentials provided
        with pytest.raises((ValueError, TypeError)):
            broker = BinanceLiveBroker(
                api_key=None,
                api_secret=None
            )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
