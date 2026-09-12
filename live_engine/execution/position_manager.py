"""Position state manager derived strictly from execution fills.

Enforces:
- No state mutation on order submission (only on verified fills).
- High precision Decimal arithmetic for quantities, entry prices, and PnL.
- Multi-direction support (Long / Short / Flat).
"""
from decimal import Decimal
from typing import Optional, Dict, Any
from live_engine.execution.models import OrderSide


class PositionManager:
    """Maintains position state derived strictly from verified execution fills."""

    def __init__(self, symbol: str):
        self.symbol = symbol
        self.side: Optional[OrderSide] = None
        self.quantity: Decimal = Decimal("0")
        self.entry_price: Decimal = Decimal("0")
        self.realized_pnl: Decimal = Decimal("0")
        self.unrealized_pnl: Decimal = Decimal("0")
        self.total_fees_paid: Decimal = Decimal("0")
        self.trade_count: int = 0

    @property
    def is_flat(self) -> bool:
        return self.quantity == Decimal("0") or self.side is None

    @property
    def is_long(self) -> bool:
        return not self.is_flat and self.side == OrderSide.BUY

    @property
    def is_short(self) -> bool:
        return not self.is_flat and self.side == OrderSide.SELL

    def update_unrealized_pnl(self, current_price: Decimal) -> Decimal:
        """Updates and returns unrealized PnL based on latest mark price."""
        if self.is_flat:
            self.unrealized_pnl = Decimal("0")
            return self.unrealized_pnl

        if self.side == OrderSide.BUY:
            self.unrealized_pnl = (current_price - self.entry_price) * self.quantity
        else:
            self.unrealized_pnl = (self.entry_price - current_price) * self.quantity

        return self.unrealized_pnl

    def on_fill(
        self,
        fill_side: OrderSide,
        fill_qty: Decimal,
        fill_price: Decimal,
        fee: Decimal = Decimal("0"),
        reduce_only: bool = False,
    ) -> None:
        """Processes a verified execution fill event.

        reduce_only=True clamps the fill to the open quantity, so an oversized exit closes
        the position instead of silently opening a reverse one. Every broker (shadow, paper,
        exchange) routes fills through here, so the invariant holds in all modes.
        """
        if fill_qty <= Decimal("0"):
            return

        if reduce_only and not self.is_flat and fill_side != self.side:
            fill_qty = min(fill_qty, self.quantity)
        elif reduce_only and self.is_flat:
            self.total_fees_paid += fee
            return

        self.total_fees_paid += fee

        if self.is_flat:
            # New opening position
            self.side = fill_side
            self.quantity = fill_qty
            self.entry_price = fill_price
            self.unrealized_pnl = Decimal("0")
            return

        # Position already open
        if self.side == fill_side:
            # Increasing existing position (weighted average entry price)
            total_qty = self.quantity + fill_qty
            total_cost = (self.quantity * self.entry_price) + (fill_qty * fill_price)
            self.entry_price = total_cost / total_qty
            self.quantity = total_qty
        else:
            # Reducing or closing existing position
            close_qty = min(self.quantity, fill_qty)
            if self.side == OrderSide.BUY:
                pnl = (fill_price - self.entry_price) * close_qty
            else:
                pnl = (self.entry_price - fill_price) * close_qty

            self.realized_pnl += (pnl - fee)
            remaining_qty = self.quantity - close_qty

            if remaining_qty == Decimal("0"):
                self.quantity = Decimal("0")
                self.side = None
                self.entry_price = Decimal("0")
                self.unrealized_pnl = Decimal("0")
                self.trade_count += 1
            else:
                self.quantity = remaining_qty

            # If order flipped position (overshoot)
            overshoot = fill_qty - close_qty
            if overshoot > Decimal("0"):
                self.side = fill_side
                self.quantity = overshoot
                self.entry_price = fill_price
                self.unrealized_pnl = Decimal("0")

    def sync_from_exchange(
        self,
        exchange_side: Optional[OrderSide],
        exchange_qty: Decimal,
        exchange_entry_price: Decimal,
    ) -> None:
        """Reconciles internal state with authoritative exchange balance/position."""
        if exchange_qty <= Decimal("0") or exchange_side is None:
            self.side = None
            self.quantity = Decimal("0")
            self.entry_price = Decimal("0")
            self.unrealized_pnl = Decimal("0")
        else:
            self.side = exchange_side
            self.quantity = exchange_qty
            self.entry_price = exchange_entry_price

    def to_dict(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "side": self.side.value if self.side else None,
            "quantity": str(self.quantity),
            "entry_price": str(self.entry_price),
            "realized_pnl": str(self.realized_pnl),
            "unrealized_pnl": str(self.unrealized_pnl),
            "total_fees_paid": str(self.total_fees_paid),
            "trade_count": self.trade_count,
            "is_flat": self.is_flat,
        }
