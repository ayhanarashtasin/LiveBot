"""Global pytest fixtures and mocking for Escanor test suite.

Ensures:
- Normal tests do not call live Binance endpoints.
- All network requests are mocked in normal tests.
- Operational parity reports are never overwritten during test runs.
"""
from decimal import Decimal
import pytest

from live_engine.execution.order_filters import SymbolFilters

TEST_FILTERS = {
    "BTCUSDT": SymbolFilters(
        symbol="BTCUSDT",
        tick_size=Decimal("0.10"),
        step_size=Decimal("0.001"),
        min_qty=Decimal("0.001"),
        max_qty=Decimal("1000.0"),
        min_notional=Decimal("5.0"),
        price_precision=2,
        qty_precision=3,
    ),
    "LITUSDT": SymbolFilters(
        symbol="LITUSDT",
        tick_size=Decimal("0.0001"),
        step_size=Decimal("0.1"),
        min_qty=Decimal("0.1"),
        max_qty=Decimal("1000.0"),
        min_notional=Decimal("5.0"),
        price_precision=4,
        qty_precision=1,
    ),
    "ZECUSDT": SymbolFilters(
        symbol="ZECUSDT",
        tick_size=Decimal("0.01"),
        step_size=Decimal("0.001"),
        min_qty=Decimal("0.001"),
        max_qty=Decimal("1000.0"),
        min_notional=Decimal("5.0"),
        price_precision=2,
        qty_precision=3,
    ),
}


def mock_fetch_public_exchange_info(symbol: str, timeout: int = 10):
    sym_upper = symbol.upper()
    return TEST_FILTERS.get(sym_upper, TEST_FILTERS["BTCUSDT"])


def mock_fetch_closed_klines(symbol, timeframe, start_ms, end_ms=None, base_url=""):
    """No warm-up backfill in tests; the Parquet fixture is the whole history."""
    return []


@pytest.fixture(autouse=True)
def mock_binance_network_calls(monkeypatch, request):
    """Automatically mock public exchange info to prevent live network calls.

    Preserves tests in TestPublicExchangeInfoFetching that explicitly test the order_filters module.
    """
    if "TestPublicExchangeInfoFetching" not in request.node.nodeid:
        monkeypatch.setattr(
            "live_engine.execution.order_filters.fetch_public_exchange_info",
            mock_fetch_public_exchange_info,
        )
        try:
            monkeypatch.setattr(
                "live_engine.orchestrator.fetch_public_exchange_info",
                mock_fetch_public_exchange_info,
            )
        except AttributeError:
            pass
        for target in (
            "live_engine.market_data.warmup.fetch_closed_klines",
            "live_engine.orchestrator.fetch_closed_klines",
        ):
            try:
                monkeypatch.setattr(target, mock_fetch_closed_klines)
            except AttributeError:
                pass
