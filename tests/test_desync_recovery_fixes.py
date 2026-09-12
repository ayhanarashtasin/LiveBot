"""A trade-stream gap must not freeze the engine forever.

Three defects took the LIT and ZEC paper runs off-strategy on 2026-09-11/12:

1. One failed aggTrade recovery latched DATA_DESYNCED permanently - nothing ever re-attempted
   an already-active gap, so ZEC stopped evaluating for 14 hours and held a position its
   strategy had told it to close.
2. History healing refetched the missing bars from REST and replayed them, but the desync gate
   discarded every replayed bar, so the hole never closed and the refetch repeated forever,
   one bar larger each time.
3. The filler-provenance check compared a 15m REST kline against the single 1m reconstruction
   sharing its open time, which disagrees by construction.
"""
import sqlite3
import tempfile
from decimal import Decimal

import pytest

from live_engine.config import LiveEngineConfig
from live_engine.market_data.models import AggTrade, Candle
from live_engine.orchestrator import MAX_GAP_RECOVERY_ATTEMPTS, LiveEngineOrchestrator

SYMBOL = "BTCUSDT"
TF_MS = 300_000


@pytest.fixture
def orch():
    with tempfile.TemporaryDirectory() as tmpdir:
        cfg = LiveEngineConfig(
            mode="PAPER", symbol=SYMBOL, timeframe="5m",
            event_store_path=f"{tmpdir}/events.db",
            kill_switch_path=f"{tmpdir}/kill_switch.trigger",
        )
        engine = LiveEngineOrchestrator(cfg)
        engine.initialize()
        yield engine


def incidents(orch, category):
    con = sqlite3.connect(orch.config.event_store_path)
    try:
        return [r[0] for r in con.execute(
            "SELECT details FROM incidents WHERE category = ? ORDER BY incident_id", (category,))]
    finally:
        con.close()


def open_a_gap(orch, last_id=100, next_id=110):
    """Drives a real sequence gap through the detector."""
    orch.gap_detector.last_agg_trade_id = last_id
    orch.process_aggtrade(AggTrade(
        event_type="aggTrade", event_time=1, symbol=SYMBOL, agg_trade_id=next_id,
        price=Decimal("100"), quantity=Decimal("1"), first_trade_id=next_id, last_trade_id=next_id,
        trade_time=1700000000000, buyer_is_market_maker=False, received_at=1,
    ))


def next_local_bar(orch, provenance="aggtrade"):
    """The next strategy bar after current history, as the local resampler would build it."""
    tail = orch.signal_engine.candle_history[-1]
    px = tail.close
    return Candle(
        symbol=SYMBOL, timeframe="5m", open_time=tail.open_time + TF_MS,
        close_time=tail.open_time + 2 * TF_MS - 1, open=px, high=px, low=px, close=px,
        volume=Decimal("1"), trade_count=1, is_closed=True, provenance=provenance,
    )


# --- 1. an unrecoverable gap is abandoned instead of latching forever ---------

def test_unrecoverable_gap_is_abandoned_after_bounded_attempts(orch):
    # The page Binance can no longer serve: every attempt comes back short.
    orch.gap_recovery._fetch_page = lambda f, l: []
    open_a_gap(orch)  # detection itself spends attempt 1
    assert orch.gap_detector.is_desynced is True
    assert orch.gap_detector.active_gaps[0].attempts == 1

    for attempt in range(2, MAX_GAP_RECOVERY_ATTEMPTS + 1):
        assert orch.gap_detector.is_desynced is True, f"gave up before attempt {attempt}"
        assert orch.health.data_desynced is True
        orch._retry_stalled_gaps()

    assert orch.gap_detector.active_gaps == []
    assert orch.gap_detector.is_desynced is False
    assert orch.health.data_desynced is False
    assert incidents(orch, "GAP_ABANDONED_TO_KLINES"), "abandoning a gap must be recorded"
    assert orch.event_store.get_events_by_type("GAP_ABANDONED")


def test_retry_sweep_does_not_stack_attempts_on_one_gap(orch):
    open_a_gap(orch)
    orch._recovering.add((101, 109))
    calls = []

    def record(from_id, limit):
        calls.append((from_id, limit))
        return []

    orch.gap_recovery._fetch_page = record
    orch._retry_stalled_gaps()
    assert calls == [], "a gap already being recovered must not be re-scheduled"


# --- 2. a desynced bar is evaluated from the exchange kline, not dropped ------

def test_desynced_bar_is_substituted_with_the_exchange_kline(orch, monkeypatch):
    bar = next_local_bar(orch)
    authoritative = Candle(
        symbol=SYMBOL, timeframe="5m", open_time=bar.open_time, close_time=bar.close_time,
        open=Decimal("100"), high=Decimal("140"), low=Decimal("90"), close=Decimal("130"),
        volume=Decimal("7"), trade_count=9, is_closed=True,
    )
    monkeypatch.setattr("live_engine.orchestrator.fetch_closed_klines",
                        lambda *a, **k: [authoritative])
    orch.health.data_desynced = True
    orch.gap_detector.is_desynced = True

    depth = len(orch.signal_engine.candle_history)
    orch.process_candle(bar)

    tail = orch.signal_engine.candle_history[-1]
    assert len(orch.signal_engine.candle_history) == depth + 1, "the bar must still be evaluated"
    assert tail.open_time == bar.open_time
    assert (tail.high, tail.close) == (authoritative.high, authoritative.close)
    assert tail.provenance == "rest_kline"
    assert incidents(orch, "DESYNCED_BAR_SUBSTITUTED")


def test_desynced_bar_still_fails_closed_when_the_exchange_bar_is_unavailable(orch, monkeypatch):
    monkeypatch.setattr("live_engine.orchestrator.fetch_closed_klines", lambda *a, **k: [])
    orch.health.data_desynced = True
    orch.gap_detector.is_desynced = True

    depth = len(orch.signal_engine.candle_history)
    assert orch.process_candle(next_local_bar(orch)) == []
    assert len(orch.signal_engine.candle_history) == depth
    assert incidents(orch, "EVALUATION_BLOCKED_DESYNCED")


def test_rest_sourced_bar_bypasses_the_desync_gate(orch, monkeypatch):
    """A healed bar is an exchange kline; our own feed's hole cannot have corrupted it.

    This is what makes history healing actually heal: before the fix the replayed bars were
    discarded by the gate, so the hole survived and grew by one bar every evaluation.
    """
    def explode(*a, **k):
        raise AssertionError("a rest_kline bar must not trigger a refetch")

    monkeypatch.setattr("live_engine.orchestrator.fetch_closed_klines", explode)
    orch.health.data_desynced = True
    orch.gap_detector.is_desynced = True

    depth = len(orch.signal_engine.candle_history)
    orch.process_candle(next_local_bar(orch, provenance="rest_kline"))
    assert len(orch.signal_engine.candle_history) == depth + 1


# --- 3. provenance compares a strategy bar against the minutes it spans -------

def minutes_for(bar, highs):
    """The 1m reconstructions spanning `bar`, with per-minute highs supplied."""
    out = []
    for i, high in enumerate(highs):
        t = bar.open_time + i * 60_000
        out.append(Candle(
            symbol=SYMBOL, timeframe="1m", open_time=t, close_time=t + 59_999,
            open=bar.open if i == 0 else Decimal("100"), high=high, low=bar.low,
            close=bar.close if i == len(highs) - 1 else Decimal("100"),
            volume=Decimal("1"), trade_count=1, is_closed=True,
        ))
    return out


def rest_bar():
    return Candle(
        symbol=SYMBOL, timeframe="5m", open_time=1700000100000, close_time=1700000399999,
        open=Decimal("100"), high=Decimal("110"), low=Decimal("95"), close=Decimal("105"),
        volume=Decimal("5"), trade_count=5, is_closed=True, provenance="rest_kline",
    )


def test_agreeing_reconstruction_is_not_reported_as_a_mismatch(orch):
    bar = rest_bar()
    # Only the aggregate matches the 5m bar; no single minute does, which is exactly what the
    # old check compared and why it fired on every heal.
    for m in minutes_for(bar, [Decimal("101"), Decimal("110"), Decimal("104"),
                               Decimal("108"), Decimal("102")]):
        orch.kline_validator.register_reconstructed_candle(m)

    orch._verify_filler_equivalence([bar])
    assert incidents(orch, "CANDLE_PROVENANCE_MISMATCH") == []


def test_genuinely_divergent_reconstruction_is_reported(orch):
    bar = rest_bar()
    for m in minutes_for(bar, [Decimal("101"), Decimal("180"), Decimal("104"),
                               Decimal("108"), Decimal("102")]):
        orch.kline_validator.register_reconstructed_candle(m)

    orch._verify_filler_equivalence([bar])
    assert len(incidents(orch, "CANDLE_PROVENANCE_MISMATCH")) == 1


def test_partial_minute_coverage_proves_nothing_and_is_skipped(orch):
    bar = rest_bar()
    for m in minutes_for(bar, [Decimal("101"), Decimal("110")]):  # 2 of 5 minutes
        orch.kline_validator.register_reconstructed_candle(m)

    orch._verify_filler_equivalence([bar])
    assert incidents(orch, "CANDLE_PROVENANCE_MISMATCH") == []
