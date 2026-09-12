"""Execution data structures and lifecycle states."""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, Optional


class SignalAction(str, Enum):
    ENTER_LONG = "ENTER_LONG"
    EXIT_LONG = "EXIT_LONG"
    ENTER_SHORT = "ENTER_SHORT"
    EXIT_SHORT = "EXIT_SHORT"
    NO_ACTION = "NO_ACTION"


class OrderSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(str, Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    STOP_MARKET = "STOP_MARKET"
    TAKE_PROFIT_MARKET = "TAKE_PROFIT_MARKET"


class OrderStatus(str, Enum):
    CREATED = "CREATED"
    SUBMITTING = "SUBMITTING"
    NEW = "NEW"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    PENDING_CANCEL = "PENDING_CANCEL"
    CANCELED = "CANCELED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True, slots=True)
class SignalEvent:
    """Deterministic, immutable strategy signal event."""

    signal_id: str
    benchmark_id: str
    strategy_hash: str
    symbol: str
    timeframe: str
    candle_open_time: int
    candle_close_time: int
    generated_at: int
    action: SignalAction
    reference_price: Decimal
    reason: str
    requested_position_size: Optional[Decimal] = None
    indicator_snapshot: Optional[Dict[str, float]] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "signal_id": self.signal_id,
            "benchmark_id": self.benchmark_id,
            "strategy_hash": self.strategy_hash,
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "candle_open_time": self.candle_open_time,
            "candle_close_time": self.candle_close_time,
            "generated_at": self.generated_at,
            "action": self.action.value,
            "reference_price": str(self.reference_price),
            "reason": self.reason,
            "requested_position_size": (
                str(self.requested_position_size)
                if self.requested_position_size is not None
                else None
            ),
            "indicator_snapshot": self.indicator_snapshot,
        }


@dataclass(slots=True)
class Order:
    """Mutable exchange/paper order model."""

    client_order_id: str
    symbol: str
    side: OrderSide
    order_type: OrderType
    quantity: Decimal
    price: Optional[Decimal] = None
    stop_price: Optional[Decimal] = None
    status: OrderStatus = OrderStatus.CREATED
    exchange_order_id: Optional[str] = None
    signal_id: Optional[str] = None
    created_at: int = 0
    submitted_at: Optional[int] = None
    acknowledged_at: Optional[int] = None
    filled_at: Optional[int] = None
    filled_quantity: Decimal = Decimal("0")
    accumulated_fees: Decimal = Decimal("0")
    avg_fill_price: Optional[Decimal] = None
    rejection_reason: Optional[str] = None
    updated_at: Optional[Any] = None
    # Cumulative filled quantity already applied to PositionManager. The delta between this
    # and a new event's cumulative quantity is the only thing that ever moves the position,
    # so replaying an event twice moves nothing.
    applied_cumulative_qty: Decimal = Decimal("0")
    reduce_only: bool = False
    position_side: Optional[str] = None

    @property
    def average_fill_price(self) -> Optional[Decimal]:
        return self.avg_fill_price

    @average_fill_price.setter
    def average_fill_price(self, val: Optional[Decimal]) -> None:
        self.avg_fill_price = val

    @property
    def fee(self) -> Decimal:
        return self.accumulated_fees

    @fee.setter
    def fee(self, val: Decimal) -> None:
        self.accumulated_fees = val

    @property
    def is_terminal(self) -> bool:
        return self.status in (
            OrderStatus.FILLED,
            OrderStatus.CANCELED,
            OrderStatus.REJECTED,
            OrderStatus.EXPIRED,
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "client_order_id": self.client_order_id,
            "exchange_order_id": self.exchange_order_id,
            "signal_id": self.signal_id,
            "symbol": self.symbol,
            "side": self.side.value,
            "order_type": self.order_type.value,
            "quantity": str(self.quantity),
            "price": str(self.price) if self.price is not None else None,
            "stop_price": str(self.stop_price) if self.stop_price is not None else None,
            "status": self.status.value,
            "filled_quantity": str(self.filled_quantity),
            "accumulated_fees": str(self.accumulated_fees),
            "avg_fill_price": str(self.avg_fill_price) if self.avg_fill_price is not None else None,
            "created_at": self.created_at,
            "submitted_at": self.submitted_at,
            "acknowledged_at": self.acknowledged_at,
            "filled_at": self.filled_at,
            "rejection_reason": self.rejection_reason,
            "applied_cumulative_qty": str(self.applied_cumulative_qty),
            "reduce_only": self.reduce_only,
            "position_side": self.position_side,
        }
