"""Market event data models for Escanor."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, Optional


@dataclass(frozen=True, slots=True)
class AggTrade:
    """Strongly-typed normalized Binance aggregate trade event."""

    event_type: str
    event_time: int
    symbol: str
    agg_trade_id: int
    price: Decimal
    quantity: Decimal
    first_trade_id: int
    last_trade_id: int
    trade_time: int
    buyer_is_market_maker: bool
    received_at: int
    raw_payload: Optional[str] = None

    @classmethod
    def from_binance_ws(
        cls, payload: Dict[str, Any], received_at: Optional[int] = None
    ) -> AggTrade:
        """Parse and strictly validate from official Binance WebSocket aggTrade JSON."""
        if not payload or not isinstance(payload, dict):
            raise ValueError("Payload must be a non-empty dictionary")

        data = payload.get("data", payload)
        if not isinstance(data, dict):
            raise ValueError("Payload data must be a dictionary")

        symbol = str(data.get("s", data.get("symbol", ""))).upper()
        if not symbol:
            raise ValueError("AggTrade missing required symbol ('s')")

        agg_id_raw = data.get("a", data.get("agg_trade_id"))
        if agg_id_raw is None or int(agg_id_raw) <= 0:
            raise ValueError(f"Invalid or missing agg_trade_id ('a'): {agg_id_raw}")
        agg_trade_id = int(agg_id_raw)

        price_raw = data.get("p", data.get("price"))
        if price_raw is None:
            raise ValueError("AggTrade missing price ('p')")
        price = Decimal(str(price_raw))
        if price <= Decimal("0"):
            raise ValueError(f"AggTrade price must be positive, got {price}")

        qty_raw = data.get("q", data.get("quantity"))
        if qty_raw is None:
            raise ValueError("AggTrade missing quantity ('q')")
        quantity = Decimal(str(qty_raw))
        if quantity <= Decimal("0"):
            raise ValueError(f"AggTrade quantity must be positive, got {quantity}")

        trade_time_raw = data.get("T", data.get("trade_time"))
        if trade_time_raw is None or int(trade_time_raw) <= 0:
            raise ValueError(f"Invalid or missing trade_time ('T'): {trade_time_raw}")
        trade_time = int(trade_time_raw)

        event_time = int(data.get("E", data.get("event_time", trade_time)))
        event_type = str(data.get("e", "aggTrade"))
        first_trade_id = int(data.get("f", data.get("first_trade_id", 0)))
        last_trade_id = int(data.get("l", data.get("last_trade_id", 0)))
        buyer_maker = bool(data.get("m", data.get("buyer_is_market_maker", data.get("is_buyer_maker", False))))

        now_ms = (
            received_at
            if received_at is not None
            else int(datetime.now(timezone.utc).timestamp() * 1000)
        )

        return cls(
            event_type=event_type,
            event_time=event_time,
            symbol=symbol,
            agg_trade_id=agg_trade_id,
            price=price,
            quantity=quantity,
            first_trade_id=first_trade_id,
            last_trade_id=last_trade_id,
            trade_time=trade_time,
            buyer_is_market_maker=buyer_maker,
            received_at=now_ms,
        )

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> AggTrade:
        """Parse from internal database or dictionary representation."""
        return cls(
            event_type=str(data.get("event_type", "aggTrade")),
            event_time=int(data.get("event_time", 0)),
            symbol=str(data["symbol"]).upper(),
            agg_trade_id=int(data["agg_trade_id"]),
            price=Decimal(str(data["price"])),
            quantity=Decimal(str(data["quantity"])),
            first_trade_id=int(data.get("first_trade_id", 0)),
            last_trade_id=int(data.get("last_trade_id", 0)),
            trade_time=int(data["trade_time"]),
            buyer_is_market_maker=bool(data.get("buyer_is_market_maker", False)),
            received_at=int(data.get("received_at", data["trade_time"])),
            raw_payload=data.get("raw_payload"),
        )

    @property
    def trade_datetime_utc(self) -> datetime:
        """Convert trade_time ms into UTC datetime."""
        return datetime.fromtimestamp(self.trade_time / 1000.0, tz=timezone.utc)

    @property
    def latency_ms(self) -> int:
        """Compute network ingestion latency in milliseconds."""
        return max(0, self.received_at - self.trade_time)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dict with serializable numeric primitives."""
        return {
            "event_type": self.event_type,
            "event_time": self.event_time,
            "symbol": self.symbol,
            "agg_trade_id": self.agg_trade_id,
            "price": str(self.price),
            "quantity": str(self.quantity),
            "first_trade_id": self.first_trade_id,
            "last_trade_id": self.last_trade_id,
            "trade_time": self.trade_time,
            "buyer_is_market_maker": int(self.buyer_is_market_maker),
            "received_at": self.received_at,
        }


@dataclass(slots=True)
class Candle:
    """Strongly-typed canonical OHLCV candle."""

    symbol: str
    timeframe: str
    open_time: int
    close_time: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    trade_count: int = 0
    is_closed: bool = False
    # Where this bar's OHLCV came from. "aggtrade" is the canonical local reconstruction;
    # "rest_kline" is the exchange's own bar, which a trade-stream gap cannot corrupt.
    provenance: str = "aggtrade"
    # Base volume initiated by buyers (Binance kline field ``V``). HYPE's frozen
    # order-flow rule needs this; zero keeps older candles/backtests compatible.
    taker_buy_base_volume: Decimal = Decimal("0")

    @property
    def open_datetime_utc(self) -> datetime:
        return datetime.fromtimestamp(self.open_time / 1000.0, tz=timezone.utc)

    @property
    def close_datetime_utc(self) -> datetime:
        return datetime.fromtimestamp(self.close_time / 1000.0, tz=timezone.utc)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "open_time": self.open_time,
            "close_time": self.close_time,
            "open": float(self.open),
            "high": float(self.high),
            "low": float(self.low),
            "close": float(self.close),
            "volume": float(self.volume),
            "trade_count": self.trade_count,
            "is_closed": self.is_closed,
            "provenance": self.provenance,
            "taker_buy_base_volume": float(self.taker_buy_base_volume),
        }


@dataclass(frozen=True, slots=True)
class KlineValidationResult:
    """Comparison result between reconstructed candle and official Binance kline."""

    symbol: str
    open_time: int
    reconstructed_ohlcv: Tuple[Decimal, Decimal, Decimal, Decimal, Decimal]
    binance_ohlcv: Tuple[Decimal, Decimal, Decimal, Decimal, Decimal]
    status: str  # MATCH, MISMATCH, UNVERIFIED
    discrepancy_details: Optional[str] = None
