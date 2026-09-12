"""Secondary Binance 1m kline validation client."""
from __future__ import annotations

import asyncio
import json
import logging
from decimal import Decimal
from typing import Any, Callable, Dict, Optional, Tuple

import websockets
from websockets.exceptions import ConnectionClosed

from live_engine.market_data.models import Candle, KlineValidationResult

logger = logging.getLogger("escanor.market_data.kline_validator")

WS_ENDPOINTS = {
    "usdm_futures": "wss://fstream.binance.com/market/ws",
    "spot": "wss://stream.binance.com:9443/ws",
}


class BinanceKlineValidator:
    """Listens to official Binance 1m klines and validates reconstructed candles on close."""

    def __init__(
        self,
        symbol: str,
        market_type: str = "usdm_futures",
        on_validation_callback: Optional[Callable[[KlineValidationResult], None]] = None,
        max_price_tolerance: float = 0.50,
        max_volume_tolerance_pct: float = 1.0,
        event_store: Optional[Any] = None,
    ):
        self.symbol = symbol.lower()
        self.market_type = market_type.lower()
        self.on_validation_callback = on_validation_callback
        self.max_price_tolerance = Decimal(str(max_price_tolerance))
        self.max_volume_tolerance_pct = Decimal(str(max_volume_tolerance_pct))
        self.event_store = event_store

        base_url = WS_ENDPOINTS.get(self.market_type, WS_ENDPOINTS["usdm_futures"])
        if "market/ws" in base_url:
            self.stream_url = base_url
        else:
            self.stream_url = f"{base_url}/{self.symbol}@kline_1m"

        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._ws: Optional[Any] = None

        self.reconstructed_candles: Dict[int, Candle] = {}
        self.total_validated = 0
        self.match_count = 0
        self.mismatch_count = 0
        self.has_active_mismatch = False

    def register_reconstructed_candle(self, candle: Candle) -> None:
        """Cache a reconstructed 1m candle for subsequent kline comparison."""
        self.reconstructed_candles[candle.open_time] = candle
        # Retain last 120 candles
        if len(self.reconstructed_candles) > 120:
            oldest = min(self.reconstructed_candles.keys())
            del self.reconstructed_candles[oldest]

    def validate_closed_kline(self, kline_data: Dict[str, Any]) -> Optional[KlineValidationResult]:
        """Validate a closed Binance 1m kline payload against local reconstructed candle."""
        is_closed = kline_data.get("x", False)
        if not is_closed:
            return None

        open_time = int(kline_data["t"])
        b_close_time = int(kline_data.get("T", open_time + 59999))
        b_open = Decimal(str(kline_data["o"]))
        b_high = Decimal(str(kline_data["h"]))
        b_low = Decimal(str(kline_data["l"]))
        b_close = Decimal(str(kline_data["c"]))
        b_vol = Decimal(str(kline_data["v"]))

        recon = self.reconstructed_candles.get(open_time)
        if recon is None:
            res = KlineValidationResult(
                symbol=self.symbol.upper(),
                open_time=open_time,
                reconstructed_ohlcv=(Decimal(0), Decimal(0), Decimal(0), Decimal(0), Decimal(0)),
                binance_ohlcv=(b_open, b_high, b_low, b_close, b_vol),
                status="UNVERIFIED",
                discrepancy_details="No reconstructed candle found in buffer for open_time",
            )
            return res

        r_ohlc = (recon.open, recon.high, recon.low, recon.close, recon.volume)
        b_ohlc = (b_open, b_high, b_low, b_close, b_vol)

        diff_o = abs(recon.open - b_open)
        diff_h = abs(recon.high - b_high)
        diff_l = abs(recon.low - b_low)
        diff_c = abs(recon.close - b_close)
        diff_v = abs(recon.volume - b_vol)
        diff_close_boundary = abs(recon.close_time - b_close_time)

        self.total_validated += 1

        price_match = (
            diff_o <= self.max_price_tolerance
            and diff_h <= self.max_price_tolerance
            and diff_l <= self.max_price_tolerance
            and diff_c <= self.max_price_tolerance
        )
        boundary_match = diff_close_boundary <= 1000

        # Volume tolerance: max of percentage-based and 0.5 units
        max_vol_diff = max(b_vol * (self.max_volume_tolerance_pct / Decimal("100.0")), Decimal("0.5"))
        vol_match = diff_v <= max_vol_diff

        is_match = price_match and boundary_match and vol_match

        if is_match:
            self.match_count += 1
            self.has_active_mismatch = False
            res = KlineValidationResult(
                symbol=self.symbol.upper(),
                open_time=open_time,
                reconstructed_ohlcv=r_ohlc,
                binance_ohlcv=b_ohlc,
                status="MATCH",
            )
        else:
            self.mismatch_count += 1
            self.has_active_mismatch = True
            details = (
                f"Diffs: O={diff_o}, H={diff_h}, L={diff_l}, C={diff_c} (max px tol={self.max_price_tolerance}); "
                f"Vol diff={diff_v} (recon: {recon.volume}, binance: {b_vol}, max allowed: {max_vol_diff}); "
                f"Close boundary diff={diff_close_boundary}ms"
            )
            logger.warning(f"KLINE MISMATCH at {open_time}: {details}")
            res = KlineValidationResult(
                symbol=self.symbol.upper(),
                open_time=open_time,
                reconstructed_ohlcv=r_ohlc,
                binance_ohlcv=b_ohlc,
                status="MISMATCH",
                discrepancy_details=details,
            )
            if self.event_store is not None:
                try:
                    self.event_store.record_incident("KLINE_VALIDATOR_MISMATCH", "MEDIUM", details)
                except Exception:
                    pass

        if self.event_store is not None:
            try:
                self.event_store.log_event("KLINE_VALIDATION", {
                    "symbol": self.symbol.upper(),
                    "open_time": open_time,
                    "status": res.status,
                    "details": res.discrepancy_details,
                })
            except Exception:
                pass

        if self.on_validation_callback:
            self.on_validation_callback(res)

        return res

    async def start(self) -> None:
        """Starts the WebSocket kline validation listener task."""
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self) -> None:
        """Stops the WebSocket kline validation listener cleanly."""
        self._running = False
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
        while self._running:
            try:
                async with websockets.connect(self.stream_url, ping_interval=20, ping_timeout=20, open_timeout=20) as ws:
                    self._ws = ws
                    logger.info(f"Connected to Binance 1m Kline stream: {self.stream_url}")
                    if "market/ws" in self.stream_url or self.stream_url.endswith("/ws"):
                        sub_payload = {"method": "SUBSCRIBE", "params": [f"{self.symbol}@kline_1m"], "id": 2}
                        await ws.send(json.dumps(sub_payload))

                    async for msg in ws:
                        if not self._running:
                            break
                        try:
                            payload = json.loads(msg)
                            if "result" in payload and "id" in payload:
                                continue
                            k_data = payload.get("k", payload)
                            self.validate_closed_kline(k_data)
                        except Exception as e:
                            logger.error(f"Error handling kline stream message: {e}")
            except (ConnectionClosed, asyncio.TimeoutError, OSError) as e:
                if not self._running:
                    break
                logger.warning(f"Kline validator stream disconnected: {e}. Reconnecting in 5s...")
                await asyncio.sleep(5)
            except Exception as e:
                if not self._running:
                    break
                logger.error(f"Unexpected error in kline validator: {e}")
                await asyncio.sleep(5)

