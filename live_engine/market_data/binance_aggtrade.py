"""Asynchronous public Binance WebSocket aggTrade stream client."""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Awaitable, Callable, Optional

import websockets
from websockets.exceptions import ConnectionClosed

from live_engine.market_data.models import AggTrade

logger = logging.getLogger("escanor.market_data.aggtrade")

# Binance WebSocket endpoints for market data streams
# Reference: https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Connect
# Note: aggTrade streams for USD-M Futures route through: wss://fstream.binance.com/market/ws
WS_ENDPOINTS = {
    "usdm_futures": "wss://fstream.binance.com/market/ws",  # USD-M Futures public market data
    "spot": "wss://stream.binance.com:9443/ws",  # Spot public market data
}


class BinanceAggTradeStream:
    """Robust async WebSocket client consuming Binance aggregate trades."""

    def __init__(
        self,
        symbol: str,
        market_type: str = "usdm_futures",
        on_trade_callback: Optional[Callable[[AggTrade], Awaitable[None] | None]] = None,
        base_ws_url: Optional[str] = None,
        max_reconnect_attempts: int = 10,
        backoff_base_s: float = 1.0,
        backoff_max_s: float = 30.0,
    ):
        self.symbol = symbol.lower()
        self.market_type = market_type.lower()
        self.on_trade_callback = on_trade_callback
        self.base_ws_url = base_ws_url or WS_ENDPOINTS.get(self.market_type, WS_ENDPOINTS["usdm_futures"])
        self.max_reconnect_attempts = max_reconnect_attempts
        self.backoff_base_s = backoff_base_s
        self.backoff_max_s = backoff_max_s

        if "market/ws" in self.base_ws_url:
            self.stream_url = self.base_ws_url
        else:
            self.stream_url = f"{self.base_ws_url}/{self.symbol}@aggTrade"
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._ws: Optional[Any] = None

        self.is_connected = False
        self.total_received = 0
        self.last_trade_time: Optional[int] = None
        self.last_agg_trade_id: Optional[int] = None
        self.reconnect_count = 0

    async def start(self) -> None:
        """Start the WebSocket stream in the background."""
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self) -> None:
        """Gracefully close and stop the WebSocket stream."""
        self._running = False
        self.is_connected = False
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:
                pass
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _run_loop(self) -> None:
        attempt = 0
        while self._running:
            try:
                logger.info(f"Connecting to Binance aggTrade stream: {self.stream_url}")
                async with websockets.connect(
                    self.stream_url,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=5,
                    open_timeout=20,
                ) as ws:
                    self._ws = ws
                    self.is_connected = True
                    attempt = 0
                    logger.info(f"Connected to Binance aggTrade stream for {self.symbol.upper()}")

                    # Subscribe to symbol aggTrade stream on connect
                    if "market/ws" in self.stream_url or self.stream_url.endswith("/ws"):
                        sub_payload = {"method": "SUBSCRIBE", "params": [f"{self.symbol}@aggTrade"], "id": 1}
                        await ws.send(json.dumps(sub_payload))

                    async for msg in ws:
                        if not self._running:
                            break
                        try:
                            payload = json.loads(msg)
                            if "result" in payload and "id" in payload:
                                logger.info(f"Stream subscription acknowledged: {payload}")
                                continue
                            trade = AggTrade.from_binance_ws(payload)
                            self.total_received += 1
                            self.last_trade_time = trade.trade_time
                            self.last_agg_trade_id = trade.agg_trade_id

                            if self.on_trade_callback is not None:
                                res = self.on_trade_callback(trade)
                                if asyncio.iscoroutine(res):
                                    await res
                        except Exception as parse_err:
                            logger.error(f"Error parsing trade message: {parse_err}")

                    self.is_connected = False
                    if self._running:
                        self.reconnect_count += 1
                        logger.warning("Binance aggTrade stream connection closed by peer. Reconnecting...")

            except (ConnectionClosed, asyncio.TimeoutError, OSError) as e:
                self.is_connected = False
                self.reconnect_count += 1
                if not self._running:
                    break
                attempt += 1
                delay = min(self.backoff_max_s, self.backoff_base_s * (2 ** (attempt - 1)))
                logger.warning(
                    f"Binance stream disconnected ({e}). Reconnecting attempt {attempt} in {delay:.1f}s..."
                )
                await asyncio.sleep(delay)
            except Exception as e:
                self.is_connected = False
                if not self._running:
                    break
                self.reconnect_count += 1
                logger.error(f"Unexpected error in aggTrade stream: {e}")
                await asyncio.sleep(2.0)
