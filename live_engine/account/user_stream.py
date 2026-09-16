"""Binance USD-M Futures user data stream.

Endpoints (current official USD-M Futures WebSocket API):

- production  base ``wss://fstream.binance.com``  -> ``/ws/<listenKey>``
- testnet     base ``wss://stream.binancefuture.com`` -> ``/ws/<listenKey>``

A listen key is valid for 60 minutes and is renewed with ``PUT /fapi/v1/listenKey``.
When renewal fails, the key expires, or the stream delivers ``listenKeyExpired``, the
only safe response is to mint a fresh key and reconnect: private execution events are
the engine's only view of real fills.

Nothing here ever logs an API key or a listen key. Lifecycle events are emitted as
structured records so the health supervisor can tell a live stream from a dead one.
"""
from __future__ import annotations

import asyncio
import json
import logging
import random
import time
import urllib.error
import urllib.request
import ssl
try:
    import certifi
    DEFAULT_SSL_CONTEXT = ssl.create_default_context(cafile=certifi.where())
except Exception:
    DEFAULT_SSL_CONTEXT = None
from typing import Any, Callable, Dict, Optional

logger = logging.getLogger(__name__)

# Base WebSocket hosts per environment.
WS_BASE = {
    False: "wss://fstream.binance.com",
    True: "wss://stream.binancefuture.com",
}
REST_BASE = {
    False: "https://fapi.binance.com",
    True: "https://testnet.binancefuture.com",
}

# A listen key lives 60 minutes; renew at half-life so one failed renewal is survivable.
KEEPALIVE_INTERVAL_S = 1800.0
BACKOFF_BASE_S = 1.0
BACKOFF_MAX_S = 60.0


def build_stream_url(listen_key: str, testnet: bool = False, base_ws_url: Optional[str] = None) -> str:
    """Composes the user data stream URL for the given environment."""
    if not isinstance(listen_key, str) or not listen_key.strip():
        raise ValueError("listenKey must be a non-empty string")
    base = (base_ws_url or WS_BASE[bool(testnet)]).rstrip("/")
    if base.endswith("/ws"):
        return f"{base}/{listen_key}"
    return f"{base}/ws/{listen_key}"


def backoff_with_jitter(attempt: int, base: float = BACKOFF_BASE_S, cap: float = BACKOFF_MAX_S) -> float:
    """Bounded exponential backoff with full jitter, so reconnects do not synchronise."""
    ceiling = min(cap, base * (2 ** max(0, attempt - 1)))
    return random.uniform(0.0, ceiling)


class BinanceUserDataStream:
    """Manages the Binance USD-M user data stream lifecycle."""

    def __init__(
        self,
        api_key: str,
        testnet: bool = False,
        base_rest_url: Optional[str] = None,
        base_ws_url: Optional[str] = None,
        on_order_trade_update: Optional[Callable[[Dict[str, Any]], None]] = None,
        on_account_update: Optional[Callable[[Dict[str, Any]], None]] = None,
        on_lifecycle_event: Optional[Callable[[str, Dict[str, Any]], None]] = None,
        on_heartbeat: Optional[Callable[[], None]] = None,
        keepalive_interval_s: float = KEEPALIVE_INTERVAL_S,
    ):
        self.api_key = api_key
        self.testnet = bool(testnet)
        self.rest_url = (base_rest_url or REST_BASE[self.testnet]).rstrip("/")
        self._base_ws_url = base_ws_url
        self.ws_url = WS_BASE[self.testnet] if base_ws_url is None else base_ws_url

        self.on_order_trade_update = on_order_trade_update
        self.on_account_update = on_account_update
        self.on_lifecycle_event = on_lifecycle_event
        self.on_heartbeat = on_heartbeat
        self.keepalive_interval_s = keepalive_interval_s

        self.listen_key: Optional[str] = None
        self.is_connected = False
        self.last_event_ms: Optional[int] = None
        self.reconnect_count = 0
        self.key_rotation_count = 0

        self._running = False
        self._keepalive_task: Optional[asyncio.Task] = None
        self._ws_task: Optional[asyncio.Task] = None
        self._rotate = asyncio.Event()

    # -- lifecycle reporting ------------------------------------------------

    def _event(self, kind: str, **fields: Any) -> None:
        """Structured lifecycle record. Keys and listen keys are never included."""
        payload = {"stream": "USER_DATA", "testnet": self.testnet, **fields}
        logger.info("USER_STREAM %s %s", kind, payload)
        if self.on_lifecycle_event is not None:
            try:
                self.on_lifecycle_event(kind, payload)
            except Exception as exc:
                logger.warning("User stream lifecycle callback failed: %s", exc)

    @property
    def freshness_s(self) -> Optional[float]:
        """Seconds since the last private event, or None if none has arrived."""
        if self.last_event_ms is None:
            return None
        return max(0.0, (time.time() * 1000 - self.last_event_ms) / 1000.0)

    # -- listen key REST lifecycle (blocking calls, always off the loop) -----

    def _listen_key_request(self, method: str) -> Dict[str, Any]:
        req = urllib.request.Request(
            f"{self.rest_url}/fapi/v1/listenKey",
            data=b"",
            headers={"X-MBX-APIKEY": self.api_key, "User-Agent": "Escanor-LiveBot/1.0"},
            method=method,
        )
        urlopen_kwargs: Dict[str, Any] = {"timeout": 10}
        if DEFAULT_SSL_CONTEXT:
            urlopen_kwargs["context"] = DEFAULT_SSL_CONTEXT
        with urllib.request.urlopen(req, **urlopen_kwargs) as resp:
            body = resp.read().decode("utf-8")
        return json.loads(body) if body.strip() else {}

    def create_listen_key(self) -> str:
        """Requests a new listenKey and validates it before use."""
        data = self._listen_key_request("POST")
        key = data.get("listenKey")
        if not isinstance(key, str) or not key.strip():
            raise RuntimeError("Binance returned an empty or non-string listenKey")
        self.listen_key = key
        self.key_rotation_count += 1
        self._event("LISTEN_KEY_CREATED", rotation=self.key_rotation_count)
        return key

    def keepalive_listen_key(self) -> bool:
        """Renews the listenKey. Returns False when renewal failed or the key is gone."""
        if not self.listen_key:
            return False
        try:
            self._listen_key_request("PUT")
            self._event("LISTEN_KEY_RENEWED")
            return True
        except urllib.error.HTTPError as exc:
            # -1125 "listenKey does not exist" and any 4xx mean the key is unusable.
            self._event("LISTEN_KEY_RENEWAL_FAILED", http_status=exc.code)
            return False
        except (urllib.error.URLError, TimeoutError, OSError, ValueError) as exc:
            self._event("LISTEN_KEY_RENEWAL_FAILED", error=type(exc).__name__)
            return False

    async def _create_listen_key_async(self) -> str:
        return await asyncio.to_thread(self.create_listen_key)

    async def _rotate_listen_key(self, reason: str) -> None:
        """Mints a fresh key and asks the socket loop to reconnect with it."""
        self._event("LISTEN_KEY_ROTATING", reason=reason)
        self.listen_key = None
        self._rotate.set()

    # -- background tasks ---------------------------------------------------

    async def _keepalive_loop(self) -> None:
        """Renews the key on schedule; a failed renewal rotates the key immediately."""
        while self._running:
            try:
                await asyncio.sleep(self.keepalive_interval_s)
                if not self._running:
                    break
                ok = await asyncio.to_thread(self.keepalive_listen_key)
                if not ok:
                    await self._rotate_listen_key("keepalive_failed")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("User stream keepalive loop error: %s", type(exc).__name__)

    async def _stream_loop(self) -> None:
        """Connects, consumes private events, and reconnects with bounded jittered backoff."""
        try:
            import websockets
        except ImportError:
            self._event("TERMINAL_FAILURE", reason="websockets package unavailable")
            return

        attempt = 0
        while self._running:
            try:
                if not self.listen_key:
                    await self._create_listen_key_async()
                self._rotate.clear()
                endpoint = build_stream_url(self.listen_key, self.testnet, self._base_ws_url)

                # The URL embeds the listen key, so only the host is ever logged.
                ws_kwargs: dict = {
                    "ping_interval": 20,
                    "ping_timeout": 20,
                    "close_timeout": 5,
                    "open_timeout": 35,
                }
                if DEFAULT_SSL_CONTEXT:
                    ws_kwargs["ssl"] = DEFAULT_SSL_CONTEXT
                async with websockets.connect(endpoint, **ws_kwargs) as ws:
                    self.is_connected = True
                    attempt = 0
                    self.last_event_ms = int(time.time() * 1000)
                    self._event("CONNECTED", host=self.ws_url)
                    last_beat_time = time.time()
                    if self.on_heartbeat:
                        try:
                            self.on_heartbeat()
                        except Exception as exc:
                            logger.warning("User stream heartbeat callback failed: %s", exc)

                    while self._running and not self._rotate.is_set():
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=1.0)
                        except asyncio.TimeoutError:
                            now = time.time()
                            if now - last_beat_time >= 15.0:
                                last_beat_time = now
                                if self.on_heartbeat:
                                    try:
                                        self.on_heartbeat()
                                    except Exception as exc:
                                        logger.warning("User stream heartbeat callback failed: %s", exc)
                            continue
                        last_beat_time = time.time()
                        if self.on_heartbeat:
                            try:
                                self.on_heartbeat()
                            except Exception as exc:
                                logger.warning("User stream heartbeat callback failed: %s", exc)
                        self._handle_message(msg)
                        if self._rotate.is_set():
                            break

                self.is_connected = False
                self._event("DISCONNECTED", reason="rotation" if self._rotate.is_set() else "closed")

            except asyncio.CancelledError:
                self.is_connected = False
                raise
            except Exception as exc:
                self.is_connected = False
                if not self._running:
                    break
                attempt += 1
                self.reconnect_count += 1
                delay = backoff_with_jitter(attempt)
                self._event("RECONNECTING", attempt=attempt, delay_s=round(delay, 2),
                            error=type(exc).__name__)
                try:
                    await asyncio.sleep(delay)
                except asyncio.CancelledError:
                    raise

    def _handle_message(self, msg: str) -> None:
        """Dispatches one private event, rotating the key when the exchange expires it."""
        try:
            data = json.loads(msg)
        except (TypeError, ValueError):
            logger.error("Malformed user stream payload discarded")
            return

        self.last_event_ms = int(time.time() * 1000)
        event_type = data.get("e")

        if event_type == "listenKeyExpired":
            self._event("LISTEN_KEY_EXPIRED")
            self.listen_key = None
            self._rotate.set()
            return
        if event_type == "ORDER_TRADE_UPDATE" and self.on_order_trade_update:
            self.on_order_trade_update(data)
        elif event_type == "ACCOUNT_UPDATE" and self.on_account_update:
            self.on_account_update(data)

    # -- start / stop -------------------------------------------------------

    async def start(self) -> None:
        """Starts the stream and its keepalive without blocking the event loop."""
        self._running = True
        self._rotate = asyncio.Event()
        await self._create_listen_key_async()
        self._keepalive_task = asyncio.create_task(self._keepalive_loop(), name="user-stream-keepalive")
        self._ws_task = asyncio.create_task(self._stream_loop(), name="user-stream-socket")

    async def stop(self) -> None:
        """Cancels and awaits every background task, leaving no orphans behind."""
        self._running = False
        self._rotate.set()
        for task in (self._keepalive_task, self._ws_task):
            if task is None or task.done():
                continue
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        self._keepalive_task = None
        self._ws_task = None
        self.is_connected = False
        self._event("STOPPED")
