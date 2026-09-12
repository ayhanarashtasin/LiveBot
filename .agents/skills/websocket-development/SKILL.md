---
name: websocket-development
description: Binance WebSocket clients, subscriptions, reconnects, timeouts, heartbeat behavior, stream lifecycle, backpressure, and disconnect recovery in Escanor.
---

# WebSocket Development Skill

Guidelines for building, maintaining, and debugging real-time Binance WebSocket connections in Escanor.

## 1. When to Use This Skill

Activate this skill when:
- Modifying or debugging `BinanceAggTradeStream`, `BinanceKlineValidator`, or `BinanceUserDataStream`.
- Tuning reconnection intervals, exponential backoff (1s to 30s), or ping/pong heartbeats.
- Handling network disconnects, TCP connection resets, or backpressure in trade pipelines.
- Managing Binance user data stream listenKey 30-minute keepalive loops.

## 2. Binance WebSocket Architecture Rules

1. **Exponential Backoff Reconnection**:
   - Never hammer the Binance WebSocket endpoint immediately upon disconnection.
   - Start with 1.0s delay, double on subsequent failures up to a maximum cap (30.0s), with slight random jitter.
2. **Ping / Pong Heartbeats**:
   - Maintain client-side ping intervals (e.g. 20s) and pong timeouts (e.g. 10s).
   - If no message or pong is received within the timeout window, cleanly close the socket (`ws.close()`) and trigger reconnect.
3. **ListenKey Lifecycle**:
   - The user data stream `listenKey` expires after 60 minutes if not kept alive.
   - Run a periodic background task renewing the listenKey via `PUT /fapi/v1/listenKey` every 30 minutes.
   - If renewal returns an error, create a fresh listenKey and re-subscribe immediately.
4. **Gap Detection Coupling**:
   - The aggregate trade stream must feed directly into `AggTradeGapDetector`.
   - On reconnect, inspect the first trade ID against the last seen trade ID; if $A_{\text{new}} - A_{\text{last}} > 1$, immediately flag `is_desynced = True` and trigger REST backfill.

## 3. Reference WebSocket Lifecycle Pattern

```python
import asyncio
import logging
import websockets
import json

logger = logging.getLogger("escanor.ws")

async def run_ws_stream(url: str, handler_callback, stop_event: asyncio.Event):
    backoff = 1.0
    while not stop_event.is_set():
        try:
            logger.info(f"Connecting to {url}...")
            async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                backoff = 1.0  # Reset on successful connection
                logger.info("Connected.")
                while not stop_event.is_set():
                    msg = await ws.recv()
                    data = json.loads(msg)
                    await handler_callback(data)
        except (websockets.ConnectionClosed, OSError, asyncio.TimeoutError) as err:
            if stop_event.is_set():
                break
            logger.warning(f"WebSocket disconnected: {err}. Reconnecting in {backoff:.1f}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2.0, 30.0)
```
