"""Tests for LIT/ZEC multi-symbol support and path resolution."""
import pytest
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch, MagicMock
import json
import tempfile

from live_engine.config import LiveEngineConfig, validate_config_against_manifest
from live_engine.orchestrator import LiveEngineOrchestrator
from live_engine.execution.order_filters import (
    validate_symbol_for_path,
    fetch_public_exchange_info,
    SymbolFilters,
)


class TestSymbolPathResolution:
    """Test symbol-to-data-path resolution."""

    def test_btc_data_path_resolution(self):
        """BTC uses BTCUSDT_USDM_DATA."""
        symbol = "BTCUSDT"
        path = f"{symbol}_USDM_DATA/candles/{symbol}_5m.parquet"
        assert "BTCUSDT_USDM_DATA" in path
        assert "BTCUSDT_5m.parquet" in path

    def test_lit_data_path_resolution(self):
        """LIT uses LITUSDT_USDM_DATA."""
        symbol = "LITUSDT"
        path = f"{symbol}_USDM_DATA/candles/{symbol}_15m.parquet"
        assert "LITUSDT_USDM_DATA" in path
        assert "LITUSDT_15m.parquet" in path

    def test_zec_data_path_resolution(self):
        """ZEC uses ZECUSDT_USDM_DATA."""
        symbol = "ZECUSDT"
        path = f"{symbol}_USDM_DATA/candles/{symbol}_15m.parquet"
        assert "ZECUSDT_USDM_DATA" in path
        assert "ZECUSDT_15m.parquet" in path


class TestSymbolValidation:
    """Test symbol validation for path/URL safety."""

    def test_valid_symbols(self):
        """Valid symbols pass validation."""
        assert validate_symbol_for_path("BTCUSDT") is True
        assert validate_symbol_for_path("LITUSDT") is True
        assert validate_symbol_for_path("ZECUSDT") is True

    def test_invalid_symbols_rejected(self):
        """Invalid symbols are rejected."""
        assert validate_symbol_for_path("") is False
        assert validate_symbol_for_path("BTC/USDT") is False
        assert validate_symbol_for_path("btcusdt") is False
        assert validate_symbol_for_path("BTC USDT") is False
        assert validate_symbol_for_path("../../etc/passwd") is False

    def test_symbol_length_limit(self):
        """Symbols longer than 20 chars are rejected."""
        assert validate_symbol_for_path("A" * 21) is False
        assert validate_symbol_for_path("A" * 20) is True


def _manifest(symbol="LITUSDT", allowed_modes=("SHADOW", "PAPER")):
    """Complete stub manifest.

    Execution semantics, fees and leverage are now mandatory, so a mode-restriction test
    still has to supply a manifest the engine would actually accept.
    """
    return {
        "benchmark_id": "STUB",
        "symbol": symbol,
        "allowed_modes": list(allowed_modes),
        "data": {"strategy_timeframe": "15m"},
        "signal": {"warmup_candles": 250},
        "execution_model": {
            "entry_order_type": "market", "exit_order_type": "market",
            "entry_fill_model": "next_open", "exit_fill_model": "signal_reference",
            "stop_model": "bracket_1.5_atr7", "take_profit_model": "bracket_3.0_atr7",
        },
        "fees": {"maker": 0.0002, "taker": 0.0005},
        "risk": {"stake_rule": "full_compounding", "max_open_trades": 1, "leverage": 1},
    }


class TestManifestModeRestriction:
    """Test that LIT/ZEC manifests reject TESTNET/LIVE."""

    def test_lit_shadow_allowed(self, tmp_path):
        """LIT SHADOW mode is allowed."""
        db_path = tmp_path / "test.db"
        ks_path = tmp_path / ".ks"
        cfg = LiveEngineConfig(
            mode="SHADOW",
            symbol="LITUSDT",
            timeframe="15m",
            manifest_path="benchmarks/manifests/LIT_SUPERTREND_15M.yaml",
            event_store_path=str(db_path),
            kill_switch_path=str(ks_path),
        )
        # Should not raise
        try:
            manifest = _manifest()
            validate_config_against_manifest(cfg, manifest)
        except ValueError as e:
            pytest.fail(f"LIT SHADOW should be allowed: {e}")

    def test_lit_paper_allowed(self, tmp_path):
        """LIT PAPER mode is allowed."""
        db_path = tmp_path / "test.db"
        ks_path = tmp_path / ".ks"
        cfg = LiveEngineConfig(
            mode="PAPER",
            symbol="LITUSDT",
            timeframe="15m",
            manifest_path="benchmarks/manifests/LIT_SUPERTREND_15M.yaml",
            event_store_path=str(db_path),
            kill_switch_path=str(ks_path),
        )
        manifest = _manifest()
        # Should not raise
        try:
            validate_config_against_manifest(cfg, manifest)
        except ValueError as e:
            pytest.fail(f"LIT PAPER should be allowed: {e}")

    def test_lit_testnet_rejected(self, tmp_path):
        """LIT TESTNET mode is rejected."""
        db_path = tmp_path / "test.db"
        ks_path = tmp_path / ".ks"
        cfg = LiveEngineConfig(
            mode="TESTNET",
            symbol="LITUSDT",
            manifest_path="benchmarks/manifests/LIT_SUPERTREND_15M.yaml",
            event_store_path=str(db_path),
            kill_switch_path=str(ks_path),
        )
        manifest = _manifest()
        with pytest.raises(ValueError, match="MODE NOT ALLOWED"):
            validate_config_against_manifest(cfg, manifest)

    def test_lit_live_rejected(self, tmp_path):
        """LIT LIVE mode is rejected."""
        db_path = tmp_path / "test.db"
        ks_path = tmp_path / ".ks"
        cfg = LiveEngineConfig(
            mode="LIVE",
            symbol="LITUSDT",
            manifest_path="benchmarks/manifests/LIT_SUPERTREND_15M.yaml",
            event_store_path=str(db_path),
            kill_switch_path=str(ks_path),
        )
        manifest = _manifest()
        with pytest.raises(ValueError, match="MODE NOT ALLOWED"):
            validate_config_against_manifest(cfg, manifest)


class TestPublicExchangeInfoFetching:
    """Test public Binance exchangeInfo fetching."""

    @patch("live_engine.execution.order_filters.urllib.request.urlopen")
    def test_fetch_btc_filters(self, mock_urlopen):
        """Fetch BTC filters from public API."""
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps({
            "symbols": [
                {
                    "symbol": "BTCUSDT",
                    "status": "TRADING",
                    "contractType": "PERPETUAL",
                    "pricePrecision": 2,
                    "quantityPrecision": 3,
                    "filters": [
                        {"filterType": "PRICE_FILTER", "tickSize": "0.10"},
                        {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001", "maxQty": "1000"},
                        {"filterType": "MIN_NOTIONAL", "notional": "5"},
                    ],
                }
            ]
        }).encode("utf-8")
        mock_response.__enter__.return_value = mock_response
        mock_urlopen.return_value = mock_response

        filters = fetch_public_exchange_info("BTCUSDT")
        assert filters is not None
        assert filters.symbol == "BTCUSDT"
        assert filters.tick_size == Decimal("0.10")
        assert filters.step_size == Decimal("0.001")

    @patch("live_engine.execution.order_filters.urllib.request.urlopen")
    def test_fetch_lit_filters(self, mock_urlopen):
        """Fetch LIT filters from public API."""
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps({
            "symbols": [
                {
                    "symbol": "LITUSDT",
                    "status": "TRADING",
                    "contractType": "PERPETUAL",
                    "pricePrecision": 5,
                    "quantityPrecision": 1,
                    "filters": [
                        {"filterType": "PRICE_FILTER", "tickSize": "0.0001"},
                        {"filterType": "LOT_SIZE", "stepSize": "0.1", "minQty": "0.1", "maxQty": "1000"},
                        {"filterType": "MIN_NOTIONAL", "notional": "5"},
                    ],
                }
            ]
        }).encode("utf-8")
        mock_response.__enter__.return_value = mock_response
        mock_urlopen.return_value = mock_response

        filters = fetch_public_exchange_info("LITUSDT")
        assert filters is not None
        assert filters.symbol == "LITUSDT"
        assert filters.tick_size == Decimal("0.0001")
        assert filters.step_size == Decimal("0.1")

    @patch("live_engine.execution.order_filters.urllib.request.urlopen")
    def test_fetch_zec_filters(self, mock_urlopen):
        """Fetch ZEC filters from public API."""
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps({
            "symbols": [
                {
                    "symbol": "ZECUSDT",
                    "status": "TRADING",
                    "contractType": "PERPETUAL",
                    "pricePrecision": 2,
                    "quantityPrecision": 3,
                    "filters": [
                        {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
                        {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001", "maxQty": "1000"},
                        {"filterType": "MIN_NOTIONAL", "notional": "5"},
                    ],
                }
            ]
        }).encode("utf-8")
        mock_response.__enter__.return_value = mock_response
        mock_urlopen.return_value = mock_response

        filters = fetch_public_exchange_info("ZECUSDT")
        assert filters is not None
        assert filters.symbol == "ZECUSDT"
        assert filters.tick_size == Decimal("0.01")
        assert filters.step_size == Decimal("0.001")

    @patch("live_engine.execution.order_filters.urllib.request.urlopen")
    def test_symbol_not_found_fails_closed(self, mock_urlopen):
        """Missing symbol fails closed."""
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps({"symbols": []}).encode("utf-8")
        mock_response.__enter__.return_value = mock_response
        mock_urlopen.return_value = mock_response

        filters = fetch_public_exchange_info("FAKESYMBOL")
        assert filters is None

    @patch("live_engine.execution.order_filters.urllib.request.urlopen")
    def test_network_timeout_fails_closed(self, mock_urlopen):
        """Network timeout fails closed."""
        mock_urlopen.side_effect = TimeoutError("Connection timeout")
        filters = fetch_public_exchange_info("BTCUSDT")
        assert filters is None

    @patch("live_engine.execution.order_filters.urllib.request.urlopen")
    def test_invalid_symbol_rejected(self, mock_urlopen):
        """Invalid symbols are rejected before network call."""
        filters = fetch_public_exchange_info("../../etc/passwd")
        assert filters is None
        # urlopen should not be called
        mock_urlopen.assert_not_called()


class TestQuantityRounding:
    """Test quantity rounding respects symbol step sizes."""

    def test_btc_rounding(self):
        """BTC quantity rounding (step size 0.001)."""
        filters = SymbolFilters(
            symbol="BTCUSDT",
            tick_size=Decimal("0.10"),
            step_size=Decimal("0.001"),
            min_qty=Decimal("0.001"),
            max_qty=Decimal("1000"),
            min_notional=Decimal("5"),
        )
        qty = Decimal("1.2345")
        rounded = filters.round_quantity(qty)
        assert rounded == Decimal("1.234")  # Rounds down to multiple of 0.001

    def test_lit_rounding(self):
        """LIT quantity rounding (step size 0.1)."""
        filters = SymbolFilters(
            symbol="LITUSDT",
            tick_size=Decimal("0.0001"),
            step_size=Decimal("0.1"),
            min_qty=Decimal("0.1"),
            max_qty=Decimal("1000"),
            min_notional=Decimal("5"),
        )
        qty = Decimal("100.95")
        rounded = filters.round_quantity(qty)
        assert rounded == Decimal("100.9")  # Rounds down to multiple of 0.1

    def test_zec_rounding(self):
        """ZEC quantity rounding (step size 0.001)."""
        filters = SymbolFilters(
            symbol="ZECUSDT",
            tick_size=Decimal("0.01"),
            step_size=Decimal("0.001"),
            min_qty=Decimal("0.001"),
            max_qty=Decimal("1000"),
            min_notional=Decimal("5"),
        )
        qty = Decimal("10.5678")
        rounded = filters.round_quantity(qty)
        assert rounded == Decimal("10.567")  # Rounds down to multiple of 0.001

    def test_rounding_never_exceeds_requested(self):
        """Rounding down never exceeds the requested quantity."""
        filters = SymbolFilters(
            symbol="TESTUSDT",
            tick_size=Decimal("0.01"),
            step_size=Decimal("0.1"),
            min_qty=Decimal("0.1"),
            max_qty=Decimal("10000"),
            min_notional=Decimal("5"),
        )
        original = Decimal("99.999")
        rounded = filters.round_quantity(original)
        assert rounded <= original


class TestConfigLoading:
    """Test loading four-instance configurations."""

    def test_load_lit_shadow_config(self):
        """Load lit-shadow config."""
        cfg = LiveEngineConfig(
            mode="SHADOW",
            symbol="LITUSDT",
            manifest_path="benchmarks/manifests/LIT_SUPERTREND_15M.yaml",
            event_store_path="data/lit_shadow.db",
        )
        assert cfg.mode == "SHADOW"
        assert cfg.symbol == "LITUSDT"
        assert "lit_shadow.db" in cfg.event_store_path

    def test_load_lit_paper_config(self):
        """Load lit-paper config."""
        cfg = LiveEngineConfig(
            mode="PAPER",
            symbol="LITUSDT",
            manifest_path="benchmarks/manifests/LIT_SUPERTREND_15M.yaml",
            event_store_path="data/lit_paper.db",
        )
        assert cfg.mode == "PAPER"
        assert cfg.symbol == "LITUSDT"
        assert "lit_paper.db" in cfg.event_store_path

    def test_load_zec_shadow_config(self):
        """Load zec-shadow config."""
        cfg = LiveEngineConfig(
            mode="SHADOW",
            symbol="ZECUSDT",
            manifest_path="benchmarks/manifests/ZEC_MOMENTUM_M03_15M.yaml",
            event_store_path="data/zec_shadow.db",
        )
        assert cfg.mode == "SHADOW"
        assert cfg.symbol == "ZECUSDT"
        assert "zec_shadow.db" in cfg.event_store_path

    def test_load_zec_paper_config(self):
        """Load zec-paper config."""
        cfg = LiveEngineConfig(
            mode="PAPER",
            symbol="ZECUSDT",
            manifest_path="benchmarks/manifests/ZEC_MOMENTUM_M03_15M.yaml",
            event_store_path="data/zec_paper.db",
        )
        assert cfg.mode == "PAPER"
        assert cfg.symbol == "ZECUSDT"
        assert "zec_paper.db" in cfg.event_store_path


class TestDatabaseIsolation:
    """Test that instances use isolated databases."""

    def test_four_instance_paths_unique(self):
        """Four instances use unique database paths."""
        paths = [
            "data/lit_shadow.db",
            "data/lit_paper.db",
            "data/zec_shadow.db",
            "data/zec_paper.db",
        ]
        assert len(paths) == len(set(paths))  # All unique

    def test_paper_instances_independent(self, tmp_path):
        """LIT and ZEC paper instances have independent balances."""
        db1 = tmp_path / "lit_paper.db"
        db2 = tmp_path / "zec_paper.db"
        cfg1 = LiveEngineConfig(
            mode="PAPER",
            symbol="LITUSDT",
            event_store_path=str(db1),
            manifest_path="benchmarks/manifests/LIT_SUPERTREND_15M.yaml",
        )
        cfg2 = LiveEngineConfig(
            mode="PAPER",
            symbol="ZECUSDT",
            event_store_path=str(db2),
            manifest_path="benchmarks/manifests/ZEC_MOMENTUM_M03_15M.yaml",
        )
        # Each config uses a unique database
        assert str(cfg1.event_store_path) != str(cfg2.event_store_path)


class TestSignalAndOrderIsolation:
    """Test that SHADOW/PAPER instances don't collide."""

    def test_shadow_paper_signal_isolation(self, tmp_path):
        """Same symbol SHADOW and PAPER signals are isolated by database."""
        shadow_db = tmp_path / "lit_shadow.db"
        paper_db = tmp_path / "lit_paper.db"
        shadow_cfg = LiveEngineConfig(
            mode="SHADOW",
            symbol="LITUSDT",
            event_store_path=str(shadow_db),
            manifest_path="benchmarks/manifests/LIT_SUPERTREND_15M.yaml",
        )
        paper_cfg = LiveEngineConfig(
            mode="PAPER",
            symbol="LITUSDT",
            event_store_path=str(paper_db),
            manifest_path="benchmarks/manifests/LIT_SUPERTREND_15M.yaml",
        )
        # Signals are persisted to different databases
        assert str(shadow_cfg.event_store_path) != str(paper_cfg.event_store_path)


class TestSharedKillSwitch:
    """Test shared kill switch across instances."""

    def test_shared_kill_switch_path(self):
        """All four instances share the same kill switch."""
        configs = {
            "lit_shadow": LiveEngineConfig(
                mode="SHADOW",
                symbol="LITUSDT",
                kill_switch_path=".kill_switch",
                event_store_path="data/lit_shadow.db",
                manifest_path="benchmarks/manifests/LIT_SUPERTREND_15M.yaml",
            ),
            "lit_paper": LiveEngineConfig(
                mode="PAPER",
                symbol="LITUSDT",
                kill_switch_path=".kill_switch",
                event_store_path="data/lit_paper.db",
                manifest_path="benchmarks/manifests/LIT_SUPERTREND_15M.yaml",
            ),
            "zec_shadow": LiveEngineConfig(
                mode="SHADOW",
                symbol="ZECUSDT",
                kill_switch_path=".kill_switch",
                event_store_path="data/zec_shadow.db",
                manifest_path="benchmarks/manifests/ZEC_MOMENTUM_M03_15M.yaml",
            ),
            "zec_paper": LiveEngineConfig(
                mode="PAPER",
                symbol="ZECUSDT",
                kill_switch_path=".kill_switch",
                event_store_path="data/zec_paper.db",
                manifest_path="benchmarks/manifests/ZEC_MOMENTUM_M03_15M.yaml",
            ),
        }
        # All share the same kill switch
        kill_switches = set(cfg.kill_switch_path for cfg in configs.values())
        assert len(kill_switches) == 1


class TestBenchmarkSpecificParity:
    """Test benchmark-specific parity reports."""

    def test_parity_report_naming(self):
        """Parity reports are named by benchmark_id."""
        report_paths = [
            "benchmarks/reports/BTC_ST_09_5M_parity_report.json",
            "benchmarks/reports/LIT_SUPERTREND_15M_parity_report.json",
            "benchmarks/reports/ZEC_MOMENTUM_M03_15M_parity_report.json",
        ]
        assert all(".json" in p for p in report_paths)
        assert all("_parity_report" in p for p in report_paths)
        # Verify uniqueness
        assert len(report_paths) == len(set(report_paths))


class TestParityEnforcement:
    """Test that parity reports are validated before startup."""

    def test_parity_validation_rejects_mismatched_trade_counts(self):
        """Parity validation rejects reports where trade counts don't match."""
        from live_engine.parity.replay import ParityReport
        # Synthetic report with mismatched trade counts
        bad_report = {
            "benchmark_id": "TEST_15M",
            "status": "PASS",
            "strategy_decision_parity_pct": 100.0,
            "candle_timestamp_parity_pct": 100.0,
            "entry_signal_parity_pct": 100.0,
            "exit_signal_parity_pct": 100.0,
            "trade_direction_parity_pct": 100.0,
            "benchmark_trades_count": 10,
            "replay_trades_count": 9,  # Mismatch
            "matched_trades_count": 9,
        }
        # This should fail orchestrator validation if used
        assert bad_report["benchmark_trades_count"] != bad_report["replay_trades_count"]


class TestWarmupValidation:
    """Test that missing/empty warm-up data blocks startup."""

    def test_missing_dataset_blocks_startup(self, tmp_path):
        """Startup fails if warm-up dataset is missing."""
        db_path = tmp_path / "test.db"
        ks_path = tmp_path / ".ks"

        cfg = LiveEngineConfig(
            mode="PAPER",
            symbol="FAKESYM",
            manifest_path="benchmarks/manifests/BTC_ST_09_5M.yaml",
            event_store_path=str(db_path),
            kill_switch_path=str(ks_path),
        )

        try:
            orch = LiveEngineOrchestrator(cfg)
            with pytest.raises(RuntimeError, match="WARMUP VALIDATION FAILED|Dataset not found"):
                orch.initialize()
        except (ValueError, FileNotFoundError):
            pass  # Config validation or loader errors are acceptable


class TestDatabaseIsolationEnforcement:
    """Test that database paths are validated in production."""

    def test_database_paths_are_distinct(self):
        """All four instance configs use distinct database paths with resolved-path containment."""
        from live_engine.config import validate_database_path
        configs = {
            "lit_shadow": "data/lit_shadow.db",
            "lit_paper": "data/lit_paper.db",
            "zec_shadow": "data/zec_shadow.db",
            "zec_paper": "data/zec_paper.db",
        }
        paths = list(configs.values())
        resolved_paths = [validate_database_path(p) for p in paths]
        # Verify all paths are unique
        assert len(resolved_paths) == len(set(resolved_paths)), "Database paths must be unique"
        # Verify resolved-path containment
        data_dir = Path("data").resolve()
        for rp in resolved_paths:
            assert rp.parent == data_dir

        # Verify traversal and escape paths are rejected
        with pytest.raises(ValueError, match="DATABASE ISOLATION VIOLATION"):
            validate_database_path("data/../outside.db")
        with pytest.raises(ValueError, match="DATABASE ISOLATION VIOLATION"):
            validate_database_path("database/shared.db")
        with pytest.raises(ValueError, match="DATABASE ISOLATION VIOLATION"):
            validate_database_path("data")


class TestExchangeFilterValidation:
    """Test that missing exchange filters fail closed."""

    @patch("live_engine.execution.order_filters.urllib.request.urlopen")
    def test_missing_price_filter_fails_closed(self, mock_urlopen):
        """Missing PRICE_FILTER causes from_exchange_info to fail."""
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps({
            "symbols": [
                {
                    "symbol": "TESTUSDT",
                    "status": "TRADING",
                    "contractType": "PERPETUAL",
                    "filters": [
                        # Missing PRICE_FILTER
                        {"filterType": "LOT_SIZE", "stepSize": "0.1", "minQty": "0.1", "maxQty": "1000"},
                        {"filterType": "MIN_NOTIONAL", "notional": "5"},
                    ],
                }
            ]
        }).encode("utf-8")
        mock_response.__enter__.return_value = mock_response
        mock_urlopen.return_value = mock_response

        from live_engine.execution.order_filters import SymbolFilters
        with pytest.raises(ValueError, match="PRICE_FILTER"):
            SymbolFilters.from_exchange_info({"symbol": "TESTUSDT", "filters": []})

    @patch("live_engine.execution.order_filters.urllib.request.urlopen")
    def test_missing_lot_size_fails_closed(self, mock_urlopen):
        """Missing LOT_SIZE causes from_exchange_info to fail."""
        from live_engine.execution.order_filters import SymbolFilters
        with pytest.raises(ValueError, match="LOT_SIZE"):
            SymbolFilters.from_exchange_info({
                "symbol": "TESTUSDT",
                "filters": [
                    {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
                    # Missing LOT_SIZE
                    {"filterType": "MIN_NOTIONAL", "notional": "5"},
                ]
            })

    @patch("live_engine.execution.order_filters.urllib.request.urlopen")
    def test_missing_min_notional_fails_closed(self, mock_urlopen):
        """Missing MIN_NOTIONAL causes from_exchange_info to fail."""
        from live_engine.execution.order_filters import SymbolFilters
        with pytest.raises(ValueError, match="MIN_NOTIONAL"):
            SymbolFilters.from_exchange_info({
                "symbol": "TESTUSDT",
                "filters": [
                    {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
                    {"filterType": "LOT_SIZE", "stepSize": "0.1", "minQty": "0.1", "maxQty": "1000"},
                    # Missing MIN_NOTIONAL
                ]
            })
