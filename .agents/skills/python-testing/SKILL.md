---
name: python-testing
description: Pytest configuration, test suite organization, fixtures, mocking, and TDD practices for the Escanor trading engine. Use when writing, refactoring, or running unit and integration tests for market data, brokers, risk guards, or dashboards.
---

# Python Testing Skill

Engineering guidelines for unit, integration, and asynchronous testing across the Escanor codebase.

## 1. When to Use This Skill

Activate this skill when:
- Writing new unit or integration tests for `live_engine`.
- Mocking external Binance REST endpoints, WebSockets, or file I/O.
- Debugging failing test suites or asserting deterministic behavior.
- Setting up pytest fixtures, temporary SQLite test databases, or mock candle streams.

## 2. Testing Principles & Rules

1. **Deterministic & Fast Execution**:
   - Tests must not rely on live internet connections or Binance exchange endpoints.
   - Use `unittest.mock` or `pytest-mock` to intercept network calls (`aiohttp`, `websockets`, `requests`).
   - The entire test suite must complete in under 3 minutes.
2. **Database Isolation in Tests**:
   - Never write test data to production database paths (`data/btc_paper.db`).
   - Use pytest's `tmp_path` fixture to create isolated temporary SQLite databases.
3. **Asyncio Testing**:
   - Escanor runs asynchronous pipelines. Use `@pytest.mark.asyncio` or standard `asyncio.run` inside test helpers.
   - Properly cancel background tasks and close client sessions to prevent unclosed event loop warnings.

## 3. Standard Test Pattern for Escanor

```python
import pytest
from decimal import Decimal
from live_engine.config import LiveEngineConfig
from live_engine.execution.broker import PaperBroker
from live_engine.execution.models import OrderSide, OrderType

def test_paper_broker_margin_and_fee(tmp_path):
    db_path = tmp_path / "test_paper.db"
    config = LiveEngineConfig(
        mode="PAPER",
        symbol="BTCUSDT",
        event_store_path=str(db_path),
        stake_amount=Decimal("1000.0"),
    )
    broker = PaperBroker(config)
    order = broker.place_order(
        symbol="BTCUSDT",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=Decimal("0.05"),
        price=Decimal("60000.0"),
        client_order_id="TEST-001",
    )
    assert order.status.name == "FILLED"
    assert order.fee > Decimal("0")
```

## 4. Verification Command

Always run the full suite to verify zero regressions:
```powershell
py -m pytest tests/
```
