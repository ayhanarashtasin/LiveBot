---
name: property-based-testing
description: Invariants and generated test cases for parsers, candle aggregation, timestamp boundaries, normalization, deduplication, deterministic hashes, replay, and idempotency in Escanor.
---

# Property-Based Testing Skill

Testing invariants and boundary conditions across market data transformations and state transitions.

## 1. When to Use This Skill

Activate this skill when:
- Verifying mathematical and structural invariants across `MinuteCandleBuilder` and `TimeframeResampler`.
- Testing millisecond boundary conditions ($T$, $T+60,000\text{ ms}$) on high-frequency aggTrades.
- Verifying deduplication logic when duplicate or out-of-order trade IDs are ingested.
- Testing SHA-256 code hashing and deterministic client order ID generator constraints ($\le 36$ chars).

## 2. Invariants to Validate

1. **OHLC Boundary Invariant**:
   For any valid aggregated candle:
   $$\text{Low} \le \min(\text{Open}, \text{Close}, \text{High})$$
   $$\text{High} \ge \max(\text{Open}, \text{Close}, \text{Low})$$
   $$\text{Volume} \ge 0$$
2. **Deterministic Partition Invariant**:
   Given a collection of aggTrades within $[T, T+60,000)$, partitioning them into arbitrary chunks and processing them sequentially must yield the identical candle as processing all at once.
3. **Absorbing Terminal State Invariant**:
   Once an `Order` transitions to `FILLED`, `CANCELED`, or `REJECTED`, no subsequent event can revert it to `NEW` or `SUBMITTING`.
4. **Idempotent Order ID Invariant**:
   Calling `generate_client_order_id(symbol, tf, ts, action)` with identical arguments must always produce the identical string, strictly $\le 36$ ASCII characters.

## 3. Implementation Pattern

```python
import pytest
from decimal import Decimal
from live_engine.market_data.models import AggTrade
from live_engine.market_data.candle_builder import MinuteCandleBuilder

def test_candle_aggregation_invariants():
    builder = MinuteCandleBuilder(symbol="BTCUSDT")
    minute_start_ms = 1700000000000
    
    trades = [
        AggTrade(aggregate_trade_id=1, price=Decimal("60000.0"), quantity=Decimal("1.0"), first_trade_id=1, last_trade_id=1, timestamp=minute_start_ms, is_buyer_maker=False),
        AggTrade(aggregate_trade_id=2, price=Decimal("60500.0"), quantity=Decimal("0.5"), first_trade_id=2, last_trade_id=2, timestamp=minute_start_ms + 10000, is_buyer_maker=False),
        AggTrade(aggregate_trade_id=3, price=Decimal("59800.0"), quantity=Decimal("2.0"), first_trade_id=3, last_trade_id=3, timestamp=minute_start_ms + 25000, is_buyer_maker=True),
        AggTrade(aggregate_trade_id=4, price=Decimal("60100.0"), quantity=Decimal("1.2"), first_trade_id=4, last_trade_id=4, timestamp=minute_start_ms + 59999, is_buyer_maker=False),
    ]
    
    for t in trades:
        builder.process_trade(t)
    
    candle = builder.close_minute(minute_start_ms)
    assert candle is not None
    assert candle.open == Decimal("60000.0")
    assert candle.high == Decimal("60500.0")
    assert candle.low == Decimal("59800.0")
    assert candle.close == Decimal("60100.0")
    assert candle.volume == Decimal("4.7")
    assert candle.low <= candle.open <= candle.high
    assert candle.low <= candle.close <= candle.high
```
