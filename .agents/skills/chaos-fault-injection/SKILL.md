---
name: chaos-fault-injection
description: Network fault injection, chaos engineering, dirty socket simulation, and failure recovery testing for the Escanor live trading engine. Trigger automatically whenever the user asks about chaos testing, network partitions, simulated websocket drops, reconnect recovery, exchange rate limit (429/418) handling, HTTP 502/504 retry loops, out-of-order aggTrades, or stress-testing system resilience.
---

# Chaos Engineering & Network Fault Injection Skill

Hardening the Escanor live trading engine against dirty network failures, Binance API degradation, WebSocket flapping, and unexpected edge-case crashes.

## 1. When to Use This Skill

Activate this skill automatically when:
- Designing or running chaos / stress tests against the market data or order execution pipelines.
- Simulating network partitions, dirty socket termination without TCP `FIN` packet, or half-open WebSocket streams.
- Testing Binance HTTP REST rate limit backoff (HTTP 429 / 418 IP bans) and transient 502/503/504 gateway drops.
- Injecting out-of-order, delayed, or corrupt `aggTrade` sequence packets into `AggTradeGapDetector`.
- Simulating order submission timeouts where exchange fill status is `UNKNOWN` (query-before-retry validation).
- Verifying that background engine instances recover automatically without human intervention or data corruption.

## 2. Core Chaos Principles

1. **Parity Invariant Under Chaos**:
   - Network failure or recovery must NEVER cause the engine to generate false or repainted signals.
   - If a gap is detected or the stream is desynced (`is_desynced = True`), order entry MUST be blocked until backfill completes with 100% data integrity.
2. **Fail-Closed on Unresolvable States**:
   - If a network partition exceeds timeout limits or a position reconciliation fails, the engine must disengage cleanly or trip the `.kill_switch` file.
3. **Deterministic Fault Injection**:
   - Use mock transports or monkeypatching in `tests/` rather than depending on live network disruptions.
   - Tests must run deterministically in pytest under 5 seconds per test.

## 3. Top Chaos Test Scenarios

### Scenario A: Flapping WebSocket Stream
- **Condition**: WebSocket disconnects every 500ms for 10 iterations.
- **Expected Invariant**: Exponential backoff prevents rate-limiting; deduplication prevents repeated candles; gap detector backfills any missed aggregate trade IDs seamlessly.

### Scenario B: Dirty Order Submission Timeout (UNKNOWN State)
- **Condition**: Order POST request reaches Binance, but the HTTP response drops due to local socket reset.
- **Expected Invariant**: Order enters `OrderStatus.UNKNOWN`; query-before-retry logic queries `/fapi/v1/order` using deterministic `client_order_id`; order is properly reconciled as `FILLED` without double-submitting.

### Scenario C: Corrupted / Out-of-Order aggTrades
- **Condition**: Ingest trades with timestamps $T_{10}, T_{12}, T_{11}$.
- **Expected Invariant**: Candle builder correctly tracks true minute boundary OHLCV without index errors.

## 4. Reference Chaos Test Implementation

```python
import pytest
import asyncio
from decimal import Decimal
from unittest.mock import AsyncMock, patch
from live_engine.market_data.gap_detector import AggTradeGapDetector
from live_engine.market_data.models import AggTrade

@pytest.mark.asyncio
async def test_chaos_gap_detector_burst_losses():
    detector = AggTradeGapDetector(symbol="BTCUSDT")
    
    # Process initial trade
    t1 = AggTrade(aggregate_trade_id=100, price=Decimal("60000"), quantity=Decimal("1"), first_trade_id=100, last_trade_id=100, timestamp=1000, is_buyer_maker=False)
    detector.process_trade(t1)
    assert not detector.is_desynced
    
    # Chaos injection: jump from 100 to 110 (10 lost trades)
    t2 = AggTrade(aggregate_trade_id=110, price=Decimal("60100"), quantity=Decimal("1"), first_trade_id=110, last_trade_id=110, timestamp=2000, is_buyer_maker=False)
    gap = detector.process_trade(t2)
    
    assert detector.is_desynced is True
    assert gap is not None
    assert gap == (101, 109)
```

## 5. Verification Command

Run chaos and recovery tests:
```powershell
py -m pytest tests/test_gap_detector.py tests/test_gap_recovery.py tests/test_recovery.py
```
