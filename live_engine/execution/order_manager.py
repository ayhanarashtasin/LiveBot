"""Order lifecycle state machine and tracker.

Enforces:
- Strict state transitions:
    CREATED -> SUBMITTING -> NEW -> PARTIALLY_FILLED -> FILLED
                          -> UNKNOWN -> FILLED / NEW / CANCELED / REJECTED
                          -> CANCELED / REJECTED / EXPIRED
- Terminal states (FILLED, CANCELED, REJECTED, EXPIRED) are absorbing and never regress.
- Cumulative filled quantities and fees are monotonically non-decreasing.
- Deterministic client_order_id indexing.
- Durable state auditability.
"""
from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, List, Optional, Any
import logging

from live_engine.execution.models import Order, OrderStatus, OrderSide, OrderType

logger = logging.getLogger(__name__)

# Valid transitions
VALID_TRANSITIONS = {
    OrderStatus.CREATED: {
        OrderStatus.SUBMITTING,
        OrderStatus.NEW,
        OrderStatus.PARTIALLY_FILLED,
        OrderStatus.FILLED,
        OrderStatus.REJECTED,
        OrderStatus.CANCELED,
    },

    OrderStatus.SUBMITTING: {
        OrderStatus.NEW,
        OrderStatus.FILLED,
        OrderStatus.PARTIALLY_FILLED,
        OrderStatus.REJECTED,
        OrderStatus.CANCELED,
        OrderStatus.UNKNOWN,
    },
    OrderStatus.NEW: {
        OrderStatus.PARTIALLY_FILLED,
        OrderStatus.FILLED,
        OrderStatus.CANCELED,
        OrderStatus.EXPIRED,
        OrderStatus.REJECTED,
        OrderStatus.UNKNOWN,
    },
    OrderStatus.PARTIALLY_FILLED: {
        OrderStatus.PARTIALLY_FILLED,
        OrderStatus.FILLED,
        OrderStatus.CANCELED,
        OrderStatus.EXPIRED,
        OrderStatus.UNKNOWN,
    },
    OrderStatus.UNKNOWN: {
        OrderStatus.NEW,
        OrderStatus.PARTIALLY_FILLED,
        OrderStatus.FILLED,
        OrderStatus.CANCELED,
        OrderStatus.REJECTED,
        OrderStatus.EXPIRED,
    },
    OrderStatus.FILLED: set(),
    OrderStatus.CANCELED: set(),
    OrderStatus.REJECTED: set(),
    OrderStatus.EXPIRED: set(),
}


class OrderManager:
    """Manages order lifecycles, active state, and transition validation."""

    def __init__(self):
        self._orders_by_client_id: Dict[str, Order] = {}
        self._orders_by_exchange_id: Dict[str, Order] = {}

    def register_order(self, order: Order) -> Order:
        """Registers a newly created order."""
        if order.client_order_id in self._orders_by_client_id:
            raise ValueError(f"Duplicate client_order_id: {order.client_order_id}")
        self._orders_by_client_id[order.client_order_id] = order
        if order.exchange_order_id:
            self._orders_by_exchange_id[order.exchange_order_id] = order
        return order

    def get_order_by_client_id(self, client_order_id: str) -> Optional[Order]:
        return self._orders_by_client_id.get(client_order_id)

    def get_order_by_exchange_id(self, exchange_order_id: str) -> Optional[Order]:
        return self._orders_by_exchange_id.get(exchange_order_id)

    def get_open_orders(self) -> List[Order]:
        return [
            o for o in self._orders_by_client_id.values()
            if o.status in (OrderStatus.CREATED, OrderStatus.SUBMITTING, OrderStatus.NEW, OrderStatus.PARTIALLY_FILLED, OrderStatus.UNKNOWN)
        ]

    def transition_status(
        self,
        order: Order,
        new_status: OrderStatus,
        exchange_order_id: Optional[str] = None,
        filled_qty: Optional[Decimal] = None,
        avg_price: Optional[Decimal] = None,
        fee: Optional[Decimal] = None,
        error_msg: Optional[str] = None,
    ) -> Order:
        """Transitions order status enforcing strict state machine and monotonic fills."""
        current_status = order.status

        # 1. If order is already terminal, reject resurrection to active state
        if order.is_terminal and new_status != current_status:
            logger.warning(
                f"Ignoring transition for terminal order {order.client_order_id}: "
                f"already {current_status.value}, cannot transition to {new_status.value}"
            )
            return order

        # 2. Validate transition before changing status
        if new_status != current_status:
            allowed = VALID_TRANSITIONS.get(current_status, set())
            if new_status not in allowed:
                logger.warning(
                    f"Invalid transition rejected for order {order.client_order_id}: "
                    f"{current_status.value} -> {new_status.value}. Allowed: {[s.value for s in allowed]}"
                )
                return order
            order.status = new_status

        # 3. Monotonic fill updates (cumulative filled quantity cannot decrease)
        if filled_qty is not None:
            if filled_qty >= order.filled_quantity:
                order.filled_quantity = filled_qty
            else:
                logger.warning(
                    f"Ignoring stale lower filled_qty {filled_qty} for order {order.client_order_id} "
                    f"(current cumulative: {order.filled_quantity})"
                )

        if avg_price is not None:
            order.avg_fill_price = avg_price
        if fee is not None and fee >= order.accumulated_fees:
            order.accumulated_fees = fee
        if error_msg:
            order.rejection_reason = error_msg

        if exchange_order_id:
            order.exchange_order_id = exchange_order_id
            self._orders_by_exchange_id[exchange_order_id] = order

        order.updated_at = datetime.now(timezone.utc)
        return order

    def upsert_order(self, order: Order) -> Order:
        """Inserts order or updates existing order status and fill progression."""
        existing = self._orders_by_client_id.get(order.client_order_id)
        if existing is None:
            self._orders_by_client_id[order.client_order_id] = order
            if order.exchange_order_id:
                self._orders_by_exchange_id[order.exchange_order_id] = order
            return order
        return self.transition_status(
            order=existing,
            new_status=order.status,
            exchange_order_id=order.exchange_order_id,
            filled_qty=order.filled_quantity,
            avg_price=order.avg_fill_price,
            # Fees are accounted for exactly once, by FillApplier.apply, which adds each
            # execution's commission to the running total. A broker response carries the
            # order's *cumulative* fee, so writing it here too made apply() add it on top
            # of itself: every order ended up at 2x its real commission.
            fee=None,
            error_msg=order.rejection_reason,
        )

    def update_order_status(
        self,
        client_order_id: str,
        new_status: OrderStatus,
        rejection_reason: Optional[str] = None,
    ) -> Optional[Order]:
        """Updates order status by client_order_id."""
        order = self.get_order_by_client_id(client_order_id)
        if order is None:
            return None
        return self.transition_status(order=order, new_status=new_status, error_msg=rejection_reason)

    def update_from_stream(
        self,
        client_order_id: str,
        status: OrderStatus,
        exchange_order_id: Optional[str] = None,
        filled_qty: Optional[Decimal] = None,
        avg_price: Optional[Decimal] = None,
        fee: Optional[Decimal] = None,
        error_msg: Optional[str] = None,
    ) -> Optional[Order]:
        """Applies order execution stream event to tracked order."""
        order = self.get_order_by_client_id(client_order_id)
        if order is None:
            logger.warning(f"Received stream update for untracked client_order_id: {client_order_id}")
            return None
        return self.transition_status(
            order=order,
            new_status=status,
            exchange_order_id=exchange_order_id,
            filled_qty=filled_qty,
            avg_price=avg_price,
            fee=fee,
            error_msg=error_msg,
        )

