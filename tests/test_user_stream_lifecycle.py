"""Section 3 regressions: authenticated user-data stream lifecycle.

The old client built ``wss://fstream.binance.com/private/ws/<key>`` (not an endpoint),
swallowed keepalive failures, and left background tasks running after shutdown.
"""
import asyncio
import logging
import urllib.error
from typing import List

import pytest

from live_engine.account.user_stream import (
    BinanceUserDataStream,
    backoff_with_jitter,
    build_stream_url,
)

SECRET_KEY = "SUPER-SECRET-API-KEY"
LISTEN_KEY = "abcdef0123456789listenkey"


def test_stream_url_for_production_and_testnet():
    assert build_stream_url(LISTEN_KEY, testnet=False) == f"wss://fstream.binance.com/ws/{LISTEN_KEY}"
    assert build_stream_url(LISTEN_KEY, testnet=True) == f"wss://stream.binancefuture.com/ws/{LISTEN_KEY}"


@pytest.mark.parametrize("bad", ["", "   ", None, 123])
def test_empty_listen_key_is_rejected(bad):
    with pytest.raises(ValueError):
        build_stream_url(bad)


def test_create_listen_key_rejects_empty_response(monkeypatch):
    stream = BinanceUserDataStream(api_key=SECRET_KEY)
    monkeypatch.setattr(stream, "_listen_key_request", lambda method: {"listenKey": ""})
    with pytest.raises(RuntimeError, match="empty or non-string listenKey"):
        stream.create_listen_key()


def test_keepalive_failure_rotates_key_and_triggers_reconnect():
    async def scenario():
        events: List[str] = []
        stream = BinanceUserDataStream(
            api_key=SECRET_KEY, on_lifecycle_event=lambda k, p: events.append(k)
        )
        stream.listen_key = LISTEN_KEY

        def failing_put(method):
            raise urllib.error.HTTPError("u", 400, "listenKey does not exist", None, None)

        stream._listen_key_request = failing_put
        assert stream.keepalive_listen_key() is False

        await stream._rotate_listen_key("keepalive_failed")
        assert stream.listen_key is None
        assert stream._rotate.is_set()
        assert "LISTEN_KEY_RENEWAL_FAILED" in events
        assert "LISTEN_KEY_ROTATING" in events

    asyncio.run(scenario())


def test_listen_key_expired_event_rotates_key():
    async def scenario():
        events: List[str] = []
        stream = BinanceUserDataStream(
            api_key=SECRET_KEY, on_lifecycle_event=lambda k, p: events.append(k)
        )
        stream.listen_key = LISTEN_KEY
        stream._handle_message('{"e": "listenKeyExpired"}')
        assert stream.listen_key is None
        assert stream._rotate.is_set()
        assert "LISTEN_KEY_EXPIRED" in events

    asyncio.run(scenario())


def test_backoff_is_bounded_and_jittered():
    values = [backoff_with_jitter(a) for a in range(1, 12) for _ in range(20)]
    assert all(0.0 <= v <= 60.0 for v in values)
    assert len(set(values)) > 1  # jitter, not a fixed ladder


def test_shutdown_leaves_no_orphan_tasks(monkeypatch):
    async def scenario():
        stream = BinanceUserDataStream(api_key=SECRET_KEY)
        monkeypatch.setattr(stream, "create_listen_key", lambda: setattr(stream, "listen_key", LISTEN_KEY) or LISTEN_KEY)
        # Replace the socket loop with something that stays alive until cancelled.
        async def idle():
            while True:
                await asyncio.sleep(0.01)

        monkeypatch.setattr(stream, "_stream_loop", idle)
        await stream.start()
        await asyncio.sleep(0.05)
        await stream.stop()

        leftover = [t for t in asyncio.all_tasks() if t is not asyncio.current_task() and not t.done()]
        assert leftover == []
        assert stream._keepalive_task is None and stream._ws_task is None

    asyncio.run(scenario())


def test_lifecycle_logs_never_contain_credentials_or_listen_keys(caplog, monkeypatch):
    async def scenario():
        stream = BinanceUserDataStream(api_key=SECRET_KEY, testnet=True)
        monkeypatch.setattr(stream, "_listen_key_request", lambda method: {"listenKey": LISTEN_KEY})
        with caplog.at_level(logging.DEBUG):
            stream.create_listen_key()
            stream.keepalive_listen_key()
            stream._event("CONNECTING", host=stream.ws_url)
            stream._handle_message('{"e": "listenKeyExpired"}')
        captured = caplog.text
        assert SECRET_KEY not in captured
        assert LISTEN_KEY not in captured

    asyncio.run(scenario())


def test_freshness_reported_for_health_state():
    stream = BinanceUserDataStream(api_key=SECRET_KEY)
    assert stream.freshness_s is None
    stream._handle_message('{"e": "ACCOUNT_UPDATE"}')
    assert stream.freshness_s is not None and stream.freshness_s < 5.0


def test_user_stream_on_heartbeat_invoked():
    heartbeat_count = 0

    def on_beat():
        nonlocal heartbeat_count
        heartbeat_count += 1

    stream = BinanceUserDataStream(api_key=SECRET_KEY, on_heartbeat=on_beat)
    assert stream.on_heartbeat is not None
    stream.on_heartbeat()
    assert heartbeat_count == 1

