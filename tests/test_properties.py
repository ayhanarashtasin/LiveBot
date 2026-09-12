"""Property-based invariants over generated sequences.

Generated with the standard library under fixed seeds rather than a new dependency: the
cases are reproducible, and a failure prints the seed and the sequence that broke it.

Properties covered: aggregate-trade sequencing, timestamp bucket boundaries, fill-delta
invariants, idempotency, deterministic hashes and replay.
"""
import random
from decimal import Decimal

import pytest

from live_engine.execution.fill_applier import FillApplier
from live_engine.execution.idempotency import generate_client_order_id
from live_engine.execution.models import Order, OrderSide, OrderStatus, OrderType
from live_engine.execution.order_manager import OrderManager
from live_engine.execution.position_manager import PositionManager
from live_engine.market_data.candle_builder import CANDLE_1M_MS, MinuteCandleBuilder
from live_engine.market_data.gap_detector import AggTradeGapDetector
from live_engine.market_data.gap_recovery import AggTradeGapRecovery
from live_engine.market_data.models import AggTrade
from live_engine.market_data.resampler import TIMEFRAME_MAP_MS, TimeframeResampler
from live_engine.persistence.event_store import EventStore
from live_engine.strategy.loader import StrategyLoader

SYMBOL = "BTCUSDT"
SEEDS = list(range(1, 26))
BASE_MS = 1_700_000_000_000


def make_trade(agg_id, trade_time, price="100", qty="1"):
    return AggTrade(event_type="aggTrade", event_time=trade_time, symbol=SYMBOL,
                    agg_trade_id=agg_id, price=Decimal(price), quantity=Decimal(qty),
                    first_trade_id=agg_id, last_trade_id=agg_id, trade_time=trade_time,
                    buyer_is_market_maker=False, received_at=trade_time)


# --- aggregate-trade sequencing ----------------------------------------------

@pytest.mark.parametrize("seed", SEEDS)
def test_gap_detector_accounts_for_every_missing_id(seed):
    """Total missing IDs always equals the sum of the gaps the detector reported."""
    rng = random.Random(seed)
    ids, cursor = [], rng.randint(1, 10_000)
    for _ in range(rng.randint(5, 60)):
        ids.append(cursor)
        cursor += rng.choice([1, 1, 1, 2, 5, 50])

    detector = AggTradeGapDetector(SYMBOL)
    for i, agg_id in enumerate(ids):
        detector.process_trade(make_trade(agg_id, BASE_MS + i))

    expected_missing = (ids[-1] - ids[0] + 1) - len(set(ids))
    assert detector.missing_ids_total == expected_missing, f"seed={seed} ids={ids[:10]}"
    assert detector.is_desynced == (expected_missing > 0)


@pytest.mark.parametrize("seed", SEEDS)
def test_duplicates_and_reversals_never_advance_the_sequence_head(seed):
    rng = random.Random(seed)
    detector = AggTradeGapDetector(SYMBOL)
    head = 1000
    detector.process_trade(make_trade(head, BASE_MS))

    for i in range(40):
        noise = rng.choice(["dup", "reverse", "forward"])
        if noise == "dup":
            detector.process_trade(make_trade(head, BASE_MS + i))
            assert detector.last_agg_trade_id == head
        elif noise == "reverse":
            detector.process_trade(make_trade(head - rng.randint(1, 20), BASE_MS + i))
            assert detector.last_agg_trade_id == head
        else:
            head += 1
            detector.process_trade(make_trade(head, BASE_MS + i))
            assert detector.last_agg_trade_id == head


# --- timestamp bucket boundaries ---------------------------------------------

@pytest.mark.parametrize("seed", SEEDS)
def test_minute_bucket_boundaries_are_half_open(seed):
    """A trade at T+59_999 belongs to the bucket; one at T+60_000 starts the next."""
    rng = random.Random(seed)
    bucket = (BASE_MS + rng.randint(0, 10**9)) // CANDLE_1M_MS * CANDLE_1M_MS

    builder = MinuteCandleBuilder(SYMBOL)
    assert builder.add_trade(make_trade(1, bucket)) is None
    assert builder.add_trade(make_trade(2, bucket + 59_999)) is None
    closed = builder.add_trade(make_trade(3, bucket + CANDLE_1M_MS))

    assert closed is not None
    assert closed.open_time == bucket
    assert closed.close_time == bucket + CANDLE_1M_MS - 1
    assert closed.trade_count == 2


@pytest.mark.parametrize("timeframe", ["5m", "15m", "1h"])
@pytest.mark.parametrize("seed", SEEDS[:10])
def test_resampled_buckets_are_utc_aligned(timeframe, seed):
    rng = random.Random(seed)
    tf_ms = TIMEFRAME_MAP_MS[timeframe]
    resampler = TimeframeResampler(SYMBOL, timeframe)
    start = (BASE_MS + rng.randint(0, 10**8)) // tf_ms * tf_ms

    emitted = []
    minutes = tf_ms // CANDLE_1M_MS
    for i in range(minutes * 3):
        open_time = start + i * CANDLE_1M_MS
        from live_engine.market_data.models import Candle

        candle = Candle(symbol=SYMBOL, timeframe="1m", open_time=open_time,
                        close_time=open_time + CANDLE_1M_MS - 1, open=Decimal("100"),
                        high=Decimal("101"), low=Decimal("99"), close=Decimal("100"),
                        volume=Decimal("1"), trade_count=1, is_closed=True)
        out = resampler.add_1m_candle(candle)
        if out is not None:
            emitted.append(out)

    assert emitted, f"no bucket closed for {timeframe} seed={seed}"
    for bar in emitted:
        assert bar.open_time % tf_ms == 0
        assert bar.close_time == bar.open_time + tf_ms - 1


@pytest.mark.parametrize("seed", SEEDS)
def test_candle_ohlc_bounds_hold_for_any_trade_sequence(seed):
    rng = random.Random(seed)
    builder = MinuteCandleBuilder(SYMBOL)
    bucket = BASE_MS // CANDLE_1M_MS * CANDLE_1M_MS
    prices = [Decimal(str(rng.randint(9000, 11000))) / Decimal("100") for _ in range(rng.randint(2, 40))]

    for i, price in enumerate(prices):
        builder.add_trade(make_trade(i + 1, bucket + i * 1000, price=str(price)))
    candle = builder.finalize_current_candle()

    assert candle.open == prices[0]
    assert candle.close == prices[-1]
    assert candle.high == max(prices)
    assert candle.low == min(prices)
    assert candle.low <= candle.open <= candle.high
    assert candle.low <= candle.close <= candle.high


# --- fill-delta invariants and idempotency -----------------------------------

@pytest.fixture
def applier(tmp_path):
    store = EventStore(tmp_path / "props.db")
    om, pm = OrderManager(), PositionManager(SYMBOL)
    return FillApplier(om, pm, store, SYMBOL), om, pm, store


def _seed_order(om, store, cid, qty):
    order = Order(client_order_id=cid, symbol=SYMBOL, side=OrderSide.BUY,
                  order_type=OrderType.MARKET, quantity=qty, price=Decimal("100"),
                  status=OrderStatus.SUBMITTING, created_at=1)
    om.upsert_order(order)
    store.save_order(order)


@pytest.mark.parametrize("seed", SEEDS)
def test_applied_deltas_always_sum_to_the_final_cumulative_quantity(seed, applier):
    """However the partials arrive, position == final cumulative filled quantity."""
    rng = random.Random(seed)
    app, om, pm, store = applier
    total = Decimal(str(rng.randint(1, 50)))

    steps, cursor = [], Decimal("0")
    while cursor < total:
        cursor = min(total, cursor + Decimal(str(rng.randint(1, 10))))
        steps.append(cursor)

    _seed_order(om, store, "ESC-P", total)
    applied = Decimal("0")
    for i, cumulative in enumerate(steps):
        applied += app.apply(
            client_order_id="ESC-P", symbol=SYMBOL, side=OrderSide.BUY,
            status=OrderStatus.FILLED if cumulative == total else OrderStatus.PARTIALLY_FILLED,
            cumulative_qty=cumulative, last_price=Decimal("100"), trade_id=str(i),
            source="TEST",
        )

    assert applied == total, f"seed={seed} steps={steps}"
    assert pm.quantity == total


@pytest.mark.parametrize("seed", SEEDS)
def test_replaying_any_event_order_is_idempotent(seed, applier):
    """Re-delivering the same executions in any order never moves the position again."""
    rng = random.Random(seed)
    app, om, pm, store = applier
    total = Decimal("10")
    events = [(Decimal(str(c)), str(c)) for c in range(1, 11)]

    _seed_order(om, store, "ESC-R", total)
    for cumulative, trade_id in events:
        app.apply(client_order_id="ESC-R", symbol=SYMBOL, side=OrderSide.BUY,
                  status=OrderStatus.PARTIALLY_FILLED, cumulative_qty=cumulative,
                  last_price=Decimal("100"), trade_id=trade_id, source="TEST")

    settled = pm.to_dict()
    shuffled = events[:]
    rng.shuffle(shuffled)
    for cumulative, trade_id in shuffled:
        assert app.apply(client_order_id="ESC-R", symbol=SYMBOL, side=OrderSide.BUY,
                         status=OrderStatus.PARTIALLY_FILLED, cumulative_qty=cumulative,
                         last_price=Decimal("100"), trade_id=trade_id,
                         source="TEST") == Decimal("0")
    assert pm.to_dict() == settled


@pytest.mark.parametrize("seed", SEEDS)
def test_reduce_only_fills_can_never_drive_the_position_negative(seed, applier):
    rng = random.Random(seed)
    app, om, pm, store = applier
    opened = Decimal(str(rng.randint(1, 20)))

    _seed_order(om, store, "ESC-IN", opened)
    app.apply(client_order_id="ESC-IN", symbol=SYMBOL, side=OrderSide.BUY,
              status=OrderStatus.FILLED, cumulative_qty=opened,
              last_price=Decimal("100"), trade_id="in", source="TEST")

    oversize = opened + Decimal(str(rng.randint(1, 50)))
    _seed_order(om, store, "ESC-OUT", oversize)
    app.apply(client_order_id="ESC-OUT", symbol=SYMBOL, side=OrderSide.SELL,
              status=OrderStatus.FILLED, cumulative_qty=oversize,
              last_price=Decimal("101"), trade_id="out", reduce_only=True, source="TEST")

    assert pm.quantity >= Decimal("0")
    assert pm.is_flat, f"seed={seed} opened={opened} exit={oversize}"


@pytest.mark.parametrize("seed", SEEDS)
def test_replay_from_the_ledger_reproduces_state_exactly(seed, applier, tmp_path):
    rng = random.Random(seed)
    app, om, pm, store = applier
    total = Decimal(str(rng.randint(2, 20)))

    _seed_order(om, store, "ESC-L", total)
    cursor = Decimal("0")
    idx = 0
    while cursor < total:
        cursor = min(total, cursor + Decimal(str(rng.randint(1, 5))))
        app.apply(client_order_id="ESC-L", symbol=SYMBOL, side=OrderSide.BUY,
                  status=OrderStatus.FILLED if cursor == total else OrderStatus.PARTIALLY_FILLED,
                  cumulative_qty=cursor, last_price=Decimal(str(100 + idx)),
                  commission=Decimal("0.01"), trade_id=str(idx), source="TEST")
        idx += 1

    original = pm.to_dict()
    fresh_om, fresh_pm = OrderManager(), PositionManager(SYMBOL)
    FillApplier(fresh_om, fresh_pm, store, SYMBOL).replay_from_store()
    assert fresh_pm.to_dict() == original, f"seed={seed}"


# --- deterministic identifiers and hashes ------------------------------------

@pytest.mark.parametrize("seed", SEEDS)
def test_client_order_ids_are_deterministic_and_binance_legal(seed):
    rng = random.Random(seed)
    symbol = rng.choice(["BTCUSDT", "LITUSDT", "ZECUSDT", "VERYLONGSYMBOLUSDT"])
    timeframe = rng.choice(["1m", "5m", "15m", "1h"])
    ts = rng.randrange(1_600_000_000_000, 1_900_000_000_000, 1000)
    action = rng.choice(["BUY", "SELL"])

    first = generate_client_order_id(symbol, timeframe, ts, action)
    second = generate_client_order_id(symbol, timeframe, ts, action)

    assert first == second
    assert len(first) <= 36
    assert all(c.isalnum() or c in "-_" for c in first)
    assert generate_client_order_id(symbol, timeframe, ts + 1000, action) != first


@pytest.mark.parametrize("seed", SEEDS[:10])
def test_source_hashes_are_stable_across_repeated_reads(seed, tmp_path):
    payload = random.Random(seed).randbytes(4096)
    path = tmp_path / "sample.bin"
    path.write_bytes(payload)

    digests = {StrategyLoader.compute_sha256(path) for _ in range(3)}
    assert len(digests) == 1

    path.write_bytes(payload + b"\x00")
    assert StrategyLoader.compute_sha256(path) not in digests


# --- gap recovery exactness ---------------------------------------------------

@pytest.mark.parametrize("seed", SEEDS)
def test_recovery_is_complete_only_when_no_id_is_missing(seed):
    rng = random.Random(seed)
    lo = rng.randint(1, 5000)
    hi = lo + rng.randint(1, 60)
    dropped = set(rng.sample(range(lo, hi + 1), k=rng.randint(0, min(5, hi - lo))))

    def pages(from_id, limit):
        return [{"a": i, "s": SYMBOL, "p": "100", "q": "1", "T": BASE_MS + i, "m": False}
                for i in range(from_id, min(from_id + limit, hi + 1)) if i not in dropped]

    result = AggTradeGapRecovery(SYMBOL, fetch_page=pages, sleep=lambda s: None).recover(lo, hi)

    assert result.complete == (not dropped), f"seed={seed} dropped={sorted(dropped)}"
    if dropped:
        assert result.remaining is not None
        assert result.remaining[0] == min(dropped)
    else:
        assert [t.agg_trade_id for t in result.trades] == list(range(lo, hi + 1))
