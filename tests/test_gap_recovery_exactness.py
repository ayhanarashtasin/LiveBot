"""Section 5 regressions: exact, fail-closed aggTrade gap recovery.

The old routine marked a gap recovered after an empty or short REST page, did blocking
HTTP inside the websocket callback, and committed one SQLite transaction per trade.
"""
import asyncio
import tempfile
import time
import urllib.error
from decimal import Decimal

import pytest

from live_engine.config import LiveEngineConfig
from live_engine.market_data.gap_recovery import (
    AggTradeGapRecovery,
    RateLimitedError,
    retry_delay,
)
from live_engine.market_data.models import AggTrade
from live_engine.orchestrator import LiveEngineOrchestrator

SYMBOL = "BTCUSDT"


def row(agg_id, price="100.0", qty="1.0", t=None, symbol=SYMBOL):
    return {"a": agg_id, "s": symbol, "p": price, "q": qty, "f": agg_id, "l": agg_id,
            "T": t if t is not None else 1700000000000 + agg_id, "m": False}


def recovery(pages, sleep=lambda s: None):
    """Recovery whose page fetcher is a callable over a canned dataset."""
    return AggTradeGapRecovery(SYMBOL, fetch_page=pages, sleep=sleep)


def full_dataset(lo, hi):
    return lambda from_id, limit: [row(i) for i in range(max(lo, from_id), hi + 1)][:limit]


# --- page-level validation ---------------------------------------------------

def test_empty_response_leaves_gap_active():
    res = recovery(lambda f, l: []).recover(10, 14)
    assert res.complete is False
    assert res.remaining == (10, 14)
    assert "empty" in res.reason


def test_partial_page_leaves_gap_active_with_exact_remainder():
    res = recovery(lambda f, l: [row(i) for i in range(f, min(f + 2, 13))]).recover(10, 14)
    assert res.complete is False
    assert res.remaining == (13, 14)


def test_complete_multi_page_recovery_clears_gap_once():
    calls = []

    def pages(from_id, limit):
        calls.append(from_id)
        return [row(i) for i in range(from_id, min(from_id + 3, 21))]

    res = recovery(pages).recover(10, 20)
    assert res.complete is True
    assert [t.agg_trade_id for t in res.trades] == list(range(10, 21))
    assert len(calls) > 1


def test_out_of_order_and_duplicate_rows_are_normalised():
    payload = [row(12), row(10), row(11), row(11), row(12)]
    res = recovery(lambda f, l: [r for r in payload if r["a"] >= f]).recover(10, 12)
    assert res.complete is True
    assert [t.agg_trade_id for t in res.trades] == [10, 11, 12]
    assert res.rejected["duplicate"] >= 1


def test_malformed_wrong_symbol_and_out_of_range_rows_are_rejected():
    payload = [row(10), {"a": "not-an-int"}, row(11, symbol="ETHUSDT"), row(999), row(11)]
    res = recovery(lambda f, l: [r for r in payload if not isinstance(r.get("a"), int) or r["a"] >= f]).recover(10, 11)
    assert res.rejected["malformed"] >= 1
    assert res.rejected["wrong_symbol"] >= 1
    assert res.rejected["out_of_range"] >= 1
    assert res.complete is True


def test_page_with_an_internal_hole_is_requested_again_from_the_contiguous_head():
    seen = []

    def pages(from_id, limit):
        seen.append(from_id)
        if from_id == 10:
            return [row(10), row(12), row(13)]  # 11 missing
        return [row(i) for i in range(from_id, 14)]

    res = recovery(pages).recover(10, 13)
    # Pagination continues from the highest contiguous ID (10), never from the largest (13).
    assert seen[1] == 11
    assert res.complete is True


def test_non_advancing_response_terminates_instead_of_looping():
    res = recovery(lambda f, l: [row(10)]).recover(10, 12)
    assert res.complete is False
    assert res.remaining == (11, 12)


# --- rate limiting -----------------------------------------------------------

@pytest.mark.parametrize("code", [429, 418, 502, 504])
def test_retryable_http_codes_back_off_then_succeed(code):
    state = {"n": 0}
    slept = []

    def pages(from_id, limit):
        state["n"] += 1
        if state["n"] == 1:
            raise urllib.error.HTTPError("u", code, "rate limited", {"Retry-After": "2"}, None)
        return [row(i) for i in range(from_id, 12)]

    res = recovery(pages, sleep=slept.append).recover(10, 11)
    assert res.complete is True
    assert slept == [2.0]  # honoured Retry-After, not the exponential default


def test_persistent_rate_limit_stays_fail_closed():
    def pages(from_id, limit):
        raise urllib.error.HTTPError("u", 429, "rate limited", {}, None)

    res = recovery(pages, sleep=lambda s: None).recover(10, 11)
    assert res.complete is False
    assert res.remaining == (10, 11)
    assert "429" in res.reason or "unavailable" in res.reason


def test_retry_delay_prefers_retry_after_then_bounded_exponential():
    err = urllib.error.HTTPError("u", 429, "x", {"Retry-After": "5"}, None)
    assert retry_delay(err, 0) == 5.0
    assert retry_delay(None, 0) == 1.0
    assert retry_delay(None, 20) <= 60.0


def test_non_retryable_http_error_is_not_retried():
    calls = []

    def pages(from_id, limit):
        calls.append(from_id)
        raise urllib.error.HTTPError("u", 400, "bad request", {}, None)

    res = recovery(pages).recover(10, 11)
    assert len(calls) == 1
    assert res.complete is False


# --- orchestrator integration ------------------------------------------------

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


def test_partial_recovery_keeps_runtime_desynced(orch):
    orch.gap_detector.last_agg_trade_id = 100
    orch.process_aggtrade(AggTrade(
        event_type="aggTrade", event_time=1, symbol=SYMBOL, agg_trade_id=110,
        price=Decimal("100"), quantity=Decimal("1"), first_trade_id=110, last_trade_id=110,
        trade_time=1700000000000, buyer_is_market_maker=False, received_at=1,
    ))
    assert orch.gap_detector.is_desynced is True

    orch.gap_recovery._fetch_page = lambda f, l: [row(i) for i in range(f, min(f + 2, 105))]
    res = orch.recover_gap(101, 109)

    assert res.complete is False
    assert orch.gap_detector.is_desynced is True
    assert orch.health.data_desynced is True
    assert orch.gap_detector.active_gaps[0].from_id == res.remaining[0]
    incidents = orch.event_store.get_events_by_type("GAP_RECOVERY_INCOMPLETE")
    assert incidents and incidents[-1]["payload"]["remaining"] == list(res.remaining)


def test_strategy_evaluation_blocked_while_desynced(orch):
    orch.health.data_desynced = True
    orch.gap_detector.is_desynced = True
    candle = orch.signal_engine.candle_history[-1]
    from live_engine.market_data.models import Candle

    nxt = Candle(
        symbol=SYMBOL, timeframe="5m", open_time=candle.open_time + 300_000,
        close_time=candle.open_time + 599_999, open=candle.close, high=candle.close,
        low=candle.close, close=candle.close, volume=Decimal("1"), trade_count=1, is_closed=True,
    )
    before = len(orch.signal_engine.candle_history)
    assert orch.process_candle(nxt) == []
    assert len(orch.signal_engine.candle_history) == before  # never reached the strategy


def test_restart_with_persisted_last_id_detects_the_next_missing_interval(orch):
    orch.event_store.set_state("last_accepted_agg_trade_id", 500)
    orch.gap_detector.reset()
    orch.gap_detector.restore_last_accepted_id(orch.event_store.get_state("last_accepted_agg_trade_id"))

    status = orch.gap_detector.process_trade(AggTrade(
        event_type="aggTrade", event_time=1, symbol=SYMBOL, agg_trade_id=510,
        price=Decimal("100"), quantity=Decimal("1"), first_trade_id=510, last_trade_id=510,
        trade_time=1700000000000, buyer_is_market_maker=False, received_at=1,
    ))
    assert status.value == "GAP"
    assert (orch.gap_detector.active_gaps[0].from_id, orch.gap_detector.active_gaps[0].to_id) == (501, 509)


def test_slow_recovery_does_not_block_the_event_loop(orch):
    """Recovery runs in a worker thread, so heartbeats keep being served meanwhile."""
    def slow_page(from_id, limit):
        time.sleep(0.25)
        return [row(i) for i in range(from_id, 106)]

    orch.gap_recovery._fetch_page = slow_page
    beats = []

    async def scenario():
        task = asyncio.ensure_future(orch._recover_gap_async(101, 105))
        while not task.done():
            beats.append(time.monotonic())
            await asyncio.sleep(0.02)
        return await task

    result = asyncio.run(scenario())
    assert result.complete is True
    assert len(beats) > 3  # the loop kept running during the blocking fetch


def test_recovered_trades_are_batch_persisted(orch):
    orch.gap_recovery._fetch_page = lambda f, l: [row(i) for i in range(f, 121)]
    res = orch.recover_gap(101, 120)
    assert res.complete is True
    stored = orch.event_store.get_aggtrade_ids(101, 120)
    assert stored == set(range(101, 121))


def test_restart_with_stale_persisted_last_id_starts_fresh_baseline(orch):
    stale_time_ms = int((time.time() - 25000) * 1000)
    with orch.event_store._get_connection() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO engine_state (key, value_json, updated_at) VALUES (?, ?, ?);",
            ("last_accepted_agg_trade_id", "500", stale_time_ms),
        )
    orch.gap_detector.reset()
    orch.initialize()

    assert orch.gap_detector.last_agg_trade_id is None

    status = orch.gap_detector.process_trade(AggTrade(
        event_type="aggTrade", event_time=1, symbol=SYMBOL, agg_trade_id=600000,
        price=Decimal("100"), quantity=Decimal("1"), first_trade_id=600000, last_trade_id=600000,
        trade_time=1700000000000, buyer_is_market_maker=False, received_at=1,
    ))
    assert status.value == "VALID"
    assert not orch.gap_detector.is_desynced
    assert len(orch.gap_detector.active_gaps) == 0


def test_pathological_gap_size_exceeding_limit_does_not_deadlock(orch):
    orch.gap_detector.last_agg_trade_id = 100
    orch.process_aggtrade(AggTrade(
        event_type="aggTrade", event_time=1, symbol=SYMBOL, agg_trade_id=700000,
        price=Decimal("100"), quantity=Decimal("1"), first_trade_id=700000, last_trade_id=700000,
        trade_time=1700000000000, buyer_is_market_maker=False, received_at=1,
    ))
    assert not orch.gap_detector.is_desynced
    assert len(orch.gap_detector.active_gaps) == 0
