"""Single delta-based fill application path.

Binance reports `z` (cumulative filled quantity) on every execution event, and the same
fill reaches Escanor twice: once in the synchronous REST order response and again on the
user data stream. Applying `z` as if it were new quantity doubles the position.

Every source of fill information — REST responses, ORDER_TRADE_UPDATE events and
reconciliation queries — goes through :meth:`FillApplier.apply`, which:

- appends the raw execution to the immutable ``fills`` ledger (unique per execution),
- applies ``delta = new_cumulative - already_applied`` and nothing else,
- accumulates commission per event rather than overwriting a running total,
- clamps a reduce-only fill so it can never reverse the position.
"""
from __future__ import annotations

import logging
import time
from decimal import Decimal
from typing import Any, Dict, Optional

from live_engine.execution.models import Order, OrderSide, OrderStatus, OrderType

logger = logging.getLogger(__name__)

ZERO = Decimal("0")

_STATUS_MAP = {
    "NEW": OrderStatus.NEW,
    "PARTIALLY_FILLED": OrderStatus.PARTIALLY_FILLED,
    "FILLED": OrderStatus.FILLED,
    "CANCELED": OrderStatus.CANCELED,
    "REJECTED": OrderStatus.REJECTED,
    "EXPIRED": OrderStatus.EXPIRED,
}


def binance_status(raw: Optional[str]) -> OrderStatus:
    """Maps a Binance order status string onto the internal enum."""
    return _STATUS_MAP.get(str(raw or "").upper(), OrderStatus.UNKNOWN)


class FillApplier:
    """Applies execution events to order and position state exactly once."""

    def __init__(self, order_manager, position_manager, event_store, symbol: str):
        self.order_manager = order_manager
        self.position_manager = position_manager
        self.event_store = event_store
        self.symbol = symbol

    # -- public API ---------------------------------------------------------

    def apply_order_response(self, order: Order, source: str, commission: Optional[Decimal] = None) -> Decimal:
        """Applies a broker order object (REST response or reconciliation query)."""
        return self.apply(
            client_order_id=order.client_order_id,
            symbol=order.symbol,
            side=order.side,
            status=order.status,
            cumulative_qty=order.filled_quantity,
            avg_price=order.avg_fill_price,
            last_price=order.avg_fill_price,
            commission=order.accumulated_fees if commission is None else commission,
            exchange_order_id=order.exchange_order_id,
            reduce_only=order.reduce_only,
            position_side=order.position_side,
            source=source,
        )

    def apply_stream_event(self, payload: Dict[str, Any]) -> Decimal:
        """Applies a Binance ORDER_TRADE_UPDATE payload (the whole event, not just ``o``)."""
        o = payload.get("o", {})
        cid = o.get("c")
        if not cid:
            return ZERO
        return self.apply(
            client_order_id=str(cid),
            symbol=str(o.get("s") or self.symbol),
            side=OrderSide(str(o.get("S", "BUY")).upper()),
            status=binance_status(o.get("X")),
            cumulative_qty=_dec(o.get("z")),
            last_qty=_dec(o.get("l")),
            last_price=_dec(o.get("L")) or None,
            avg_price=_dec(o.get("ap")) or None,
            commission=_dec(o.get("n")),
            commission_asset=o.get("N"),
            exchange_order_id=str(o.get("i")) if o.get("i") is not None else None,
            trade_id=str(o.get("t")) if o.get("t") not in (None, "", 0, "0", -1, "-1") else None,
            reduce_only=bool(o.get("R", False)),
            position_side=o.get("ps"),
            event_time=payload.get("E"),
            transaction_time=payload.get("T") or o.get("T"),
            source="USER_STREAM",
        )

    def apply(
        self,
        *,
        client_order_id: str,
        symbol: str,
        side: OrderSide,
        status: OrderStatus,
        cumulative_qty: Decimal,
        last_qty: Optional[Decimal] = None,
        last_price: Optional[Decimal] = None,
        avg_price: Optional[Decimal] = None,
        commission: Decimal = ZERO,
        commission_asset: Optional[str] = None,
        exchange_order_id: Optional[str] = None,
        trade_id: Optional[str] = None,
        reduce_only: bool = False,
        position_side: Optional[str] = None,
        event_time: Optional[int] = None,
        transaction_time: Optional[int] = None,
        source: str = "UNKNOWN",
        record: bool = True,
    ) -> Decimal:
        """Applies one execution event. Returns the position delta actually applied."""
        cumulative_qty = _dec(cumulative_qty)
        commission = _dec(commission)
        now_ms = int(time.time() * 1000)

        # 1. Immutable ledger + deduplication. The exchange trade ID is the strongest key;
        #    a REST response carries none, so the cumulative quantity stands in for it.
        dedup_key = f"T:{trade_id}" if trade_id else f"CUM:{cumulative_qty}:{status.value}"
        if record and self.event_store is not None:
            is_new = self.event_store.record_fill({
                "client_order_id": client_order_id,
                "exchange_order_id": exchange_order_id,
                "dedup_key": dedup_key,
                "trade_id": trade_id,
                "symbol": symbol,
                "side": side.value,
                "last_qty": last_qty if last_qty is not None else ZERO,
                "cumulative_qty": cumulative_qty,
                "last_price": last_price,
                "avg_price": avg_price,
                "commission": commission,
                "commission_asset": commission_asset,
                "order_status": status.value,
                "reduce_only": reduce_only,
                "position_side": position_side,
                "event_time": event_time,
                "transaction_time": transaction_time,
                "received_at": now_ms,
                "source": source,
            })
            if not is_new:
                logger.info(
                    "Duplicate execution ignored for %s (dedup_key=%s); position, fees and PnL unchanged.",
                    client_order_id, dedup_key,
                )
                return ZERO

        order = self.order_manager.get_order_by_client_id(client_order_id)
        if order is None:
            order = self._adopt_unknown_order(client_order_id, symbol, side, cumulative_qty, reduce_only, position_side)

        # 2. Delta is the only thing that ever moves the position.
        delta = cumulative_qty - order.applied_cumulative_qty
        applied = ZERO

        if delta > ZERO:
            price = self._delta_price(order, cumulative_qty, delta, last_price, avg_price)
            requested = delta
            if reduce_only or order.reduce_only:
                capped = self._cap_reduce_only(delta)
                if capped < delta:
                    self._incident(
                        "FILL_OVERSIZE_CLAMPED",
                        f"Reduce-only fill {delta} on {client_order_id} exceeded open position "
                        f"{self.position_manager.quantity}; clamped to {capped} to prevent reversal.",
                    )
                delta = capped
            if delta > ZERO:
                self.position_manager.on_fill(
                    side, delta, price, commission,
                    reduce_only=bool(reduce_only or order.reduce_only),
                )
                applied = delta
            elif commission > ZERO:
                self.position_manager.total_fees_paid += commission
            order.applied_cumulative_qty = cumulative_qty
            if requested != applied:
                logger.warning(
                    "Fill delta clamped for %s: requested %s applied %s", client_order_id, requested, applied
                )
        elif commission > ZERO:
            # Stale or duplicate-cumulative event that still reports a real commission for
            # its own trade: the position must not move, but the fee is still paid.
            self.position_manager.total_fees_paid += commission

        # 3. Order state: fees accumulate per event, cumulative quantity never regresses,
        #    terminal states stay absorbing (transition_status enforces that).
        new_fees = order.accumulated_fees + commission
        self.order_manager.transition_status(
            order=order,
            new_status=status,
            exchange_order_id=exchange_order_id,
            filled_qty=cumulative_qty,
            avg_price=avg_price or last_price,
            fee=new_fees,
        )
        # A terminal order refuses further status transitions, but a proven-real execution
        # is still a fact: keep the cumulative quantity truthful.
        if order.filled_quantity < cumulative_qty:
            order.filled_quantity = cumulative_qty
        if reduce_only:
            order.reduce_only = True
        if position_side:
            order.position_side = position_side
        if status == OrderStatus.FILLED and order.filled_at is None:
            order.filled_at = transaction_time or now_ms

        if self.event_store is not None:
            self.event_store.save_order(order)
            if applied > ZERO:
                self.event_store.log_event("POSITION_UPDATED", self.position_manager.to_dict())
        return applied

    def replay_from_store(self) -> int:
        """Rebuilds order and position state from the stored fill ledger.

        Used after a restart: the ledger is append-only and the applier is delta-based, so
        replaying it reproduces exactly the state the engine had before it died.
        """
        fills = self.event_store.get_fills()

        # Re-seed each touched order at its pre-fill intent, otherwise the persisted
        # "already applied" cumulative would make every replayed delta zero.
        for cid in dict.fromkeys(row["client_order_id"] for row in fills):
            stored = self.event_store.get_order_by_client_id(cid)
            if stored is None:
                continue
            stored.applied_cumulative_qty = ZERO
            stored.filled_quantity = ZERO
            stored.accumulated_fees = ZERO
            stored.avg_fill_price = None
            stored.status = OrderStatus.SUBMITTING
            self.order_manager.upsert_order(stored)

        for row in fills:
            self.apply(
                client_order_id=row["client_order_id"],
                symbol=row["symbol"],
                side=OrderSide(row["side"]),
                status=binance_status(row["order_status"]),
                cumulative_qty=_dec(row["cumulative_qty"]),
                last_qty=_dec(row["last_qty"]),
                last_price=_dec(row["last_price"]) if row["last_price"] else None,
                avg_price=_dec(row["avg_price"]) if row["avg_price"] else None,
                commission=_dec(row["commission"]),
                commission_asset=row["commission_asset"],
                exchange_order_id=row["exchange_order_id"],
                trade_id=row["trade_id"],
                reduce_only=bool(row["reduce_only"]),
                position_side=row["position_side"],
                event_time=row["event_time"],
                transaction_time=row["transaction_time"],
                source="REPLAY",
                record=False,
            )
        return len(fills)

    # -- internals ----------------------------------------------------------

    def _adopt_unknown_order(self, cid, symbol, side, cumulative_qty, reduce_only, position_side) -> Order:
        """Registers an order the engine has no local record of (recovered by reconciliation)."""
        stored = self.event_store.get_order_by_client_id(cid) if self.event_store else None
        order = stored or Order(
            client_order_id=cid,
            symbol=symbol,
            side=side,
            order_type=OrderType.MARKET,
            quantity=cumulative_qty,
            status=OrderStatus.UNKNOWN,
            created_at=int(time.time() * 1000),
            reduce_only=reduce_only,
            position_side=position_side,
        )
        self.order_manager.upsert_order(order)
        return self.order_manager.get_order_by_client_id(cid) or order

    def _cap_reduce_only(self, delta: Decimal) -> Decimal:
        """A reduce-only fill may close at most the currently open opposite quantity."""
        pm = self.position_manager
        if pm.is_flat:
            return ZERO
        return min(delta, pm.quantity)

    @staticmethod
    def _delta_price(order: Order, cumulative_qty: Decimal, delta: Decimal,
                     last_price: Optional[Decimal], avg_price: Optional[Decimal]) -> Decimal:
        """Price for the newly filled quantity only.

        The event's last-fill price is exact. When only a running average is available the
        incremental price is backed out of the average progression, so a second partial is
        not booked at the blended price of the whole order.
        """
        if last_price and last_price > ZERO:
            return last_price
        prev_cum = order.applied_cumulative_qty
        prev_avg = order.avg_fill_price
        if avg_price and avg_price > ZERO:
            if prev_cum > ZERO and prev_avg and prev_avg > ZERO and delta > ZERO:
                incremental = (cumulative_qty * avg_price - prev_cum * prev_avg) / delta
                if incremental > ZERO:
                    return incremental
            return avg_price
        return order.price or ZERO

    def _incident(self, category: str, details: str) -> None:
        logger.error("%s: %s", category, details)
        if self.event_store is not None:
            self.event_store.record_incident(category, "HIGH", details)


def _dec(value: Any) -> Decimal:
    """Tolerant Decimal conversion for exchange payload fields."""
    if value is None or value == "":
        return ZERO
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))
