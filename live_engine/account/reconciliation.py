"""Account, order and fill reconciliation against authoritative exchange state.

Reconciliation exists because the engine's local view can be wrong in three ways:

- an order was submitted but the outcome is UNKNOWN (timeout or 5xx),
- a fill happened while the private stream was down,
- the process died between submission and acknowledgement.

Every repair routes through the shared delta-based fill path, so running reconciliation
twice - or once per restart - mutates position, fees and PnL at most once.

**Absence proof policy.** An Escanor order is treated as absent only when *all* of the
following hold: ``ABSENCE_PROOF_QUERIES`` consecutive queries by client order ID return
NOT_FOUND, those queries are spaced by bounded backoff covering at least
``ABSENCE_PROOF_MIN_WINDOW_S`` seconds of exchange indexing delay, and no account trade
references that client order ID. Until then the order stays UNKNOWN and blocks new
entries. Nothing is ever resubmitted on a weaker signal than that.
"""
from __future__ import annotations

import logging
import time
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from live_engine.execution.broker import ExecutionBroker
from live_engine.execution.models import Order, OrderSide, OrderStatus
from live_engine.execution.position_manager import PositionManager
from live_engine.risk.kill_switch import KillSwitch

logger = logging.getLogger(__name__)

# Deterministic absence proof (see module docstring).
ABSENCE_PROOF_QUERIES = 3
ABSENCE_PROOF_MIN_WINDOW_S = 3.0
QUERY_BACKOFF_BASE_S = 1.0
QUERY_BACKOFF_MAX_S = 8.0

# Escanor stamps every order it owns with this client-order-ID prefix. Anything else on
# the account is a manual or third-party order: reported, never modified.
OWNED_PREFIX = "ESC"


def is_escanor_order(client_order_id: Optional[str]) -> bool:
    return bool(client_order_id) and str(client_order_id).startswith(OWNED_PREFIX)


class AccountReconciler:
    """Reconciles local engine state with exchange authoritative state."""

    def __init__(
        self,
        broker: ExecutionBroker,
        position_manager: PositionManager,
        kill_switch: Optional[KillSwitch] = None,
        max_qty_discrepancy: Decimal = Decimal("0.0001"),
        event_store: Optional[Any] = None,
        order_manager: Optional[Any] = None,
        fill_applier: Optional[Any] = None,
        health: Optional[Any] = None,
    ):
        self.broker = broker
        self.position_manager = position_manager
        self.kill_switch = kill_switch
        self.max_qty_discrepancy = max_qty_discrepancy
        self.event_store = event_store
        self.order_manager = order_manager
        self.fill_applier = fill_applier
        self.health = health

    # -- position / balance --------------------------------------------------

    def reconcile_position(self) -> Dict[str, Any]:
        """Performs position audit against exchange or broker simulator."""
        symbol = self.position_manager.symbol
        exch_side, exch_qty, exch_entry_px = self.broker.get_position(symbol)

        local_side = self.position_manager.side
        local_qty = self.position_manager.quantity
        local_entry_px = self.position_manager.entry_price

        qty_diff = abs(local_qty - exch_qty)
        side_match = (local_side == exch_side) or (local_qty == Decimal("0") and exch_qty == Decimal("0"))

        result = {
            "symbol": symbol,
            "mode": self.broker.mode,
            "matched": side_match and qty_diff <= self.max_qty_discrepancy,
            "local": {
                "side": local_side.value if local_side else None,
                "quantity": str(local_qty),
                "entry_price": str(local_entry_px),
            },
            "exchange": {
                "side": exch_side.value if exch_side else None,
                "quantity": str(exch_qty),
                "entry_price": str(exch_entry_px),
            },
            "quantity_difference": str(qty_diff),
        }

        if not result["matched"]:
            # Check if this apparent mismatch is caused by fills or open orders that arrived while
            # the private stream was reconnecting or lagging.
            if self.fill_applier and self.broker.mode in ("LIVE", "TESTNET"):
                try:
                    self.reconcile_recent_fills(symbol)
                    if self.order_manager:
                        open_ords = [o for o in self.order_manager.orders.values() if not o.is_terminal]
                        if open_ords:
                            self.reconcile_open_orders(open_ords)
                except Exception as rec_err:
                    logger.warning(f"Pre-mismatch fill recovery failed: {rec_err}")

                local_side = self.position_manager.side
                local_qty = self.position_manager.quantity
                local_entry_px = self.position_manager.entry_price
                qty_diff = abs(local_qty - exch_qty)
                side_match = (local_side == exch_side) or (local_qty == Decimal("0") and exch_qty == Decimal("0"))
                if side_match and qty_diff <= self.max_qty_discrepancy:
                    result["matched"] = True
                    result["local"] = {
                        "side": local_side.value if local_side else None,
                        "quantity": str(local_qty),
                        "entry_price": str(local_entry_px),
                    }
                    result["quantity_difference"] = str(qty_diff)
                    result["action_taken"] = "Resolved via REST fill reconciliation"
                    logger.info(f"Position mismatch on {symbol} resolved via REST fill reconciliation")

        if not result["matched"]:
            msg = (
                f"POSITION RECONCILIATION MISMATCH on {symbol}! "
                f"Local: ({local_side}, {local_qty}) vs Exchange: ({exch_side}, {exch_qty})"
            )
            logger.error(msg)

            if self.broker.mode == "LIVE" and self.kill_switch:
                self.kill_switch.trigger(reason=msg, triggered_by="RECONCILER")

            if self.broker.mode in ("LIVE", "TESTNET", "PAPER"):
                self.position_manager.sync_from_exchange(exch_side, exch_qty, exch_entry_px)
                result["action_taken"] = "Synced local state to authoritative broker truth"
            elif self.broker.mode == "SHADOW":
                if hasattr(self.broker, "_get_pm"):
                    broker_pm = self.broker._get_pm(symbol)
                    broker_pm.sync_from_exchange(local_side, local_qty, local_entry_px)
                result["action_taken"] = "Synchronized shadow broker to local expected position"
        else:
            logger.debug(f"Position reconciliation matched for {symbol}")

        self._log("POSITION_RECONCILIATION", result)
        if self.health is not None:
            self.health.beat("reconciliation", "OK" if result["matched"] else "MISMATCH")
        return result

    def reconcile_balance(self) -> Dict[str, Any]:
        """Audits current account balances."""
        balances = self.broker.get_account_balance()
        result = {"mode": self.broker.mode, "balances": {k: str(v) for k, v in balances.items()}}
        self._log("BALANCE_RECONCILIATION", result)
        return result

    def reconcile_account_configuration(self, symbol: str) -> Dict[str, Any]:
        """Reconciles position mode, leverage, margin mode and authoritative equity.

        These are the account facts the risk guards depend on. Anything unreadable is a
        reconciliation failure, not a default.
        """
        result: Dict[str, Any] = {"symbol": symbol, "mode": self.broker.mode, "matched": True, "issues": []}

        get_state = getattr(self.broker, "get_account_state", None)
        if get_state is None:
            result["note"] = "Simulated broker: no exchange account configuration to reconcile"
            self._log("ACCOUNT_CONFIG_RECONCILIATION", result)
            return result

        try:
            state = get_state()
        except Exception as exc:
            result["matched"] = False
            result["issues"].append(f"account state unreadable: {exc}")
            self._log("ACCOUNT_CONFIG_RECONCILIATION", result)
            return result

        result["available_balance"] = str(state.get("available_balance", "0"))
        result["total_margin_balance"] = str(state.get("total_margin_balance", "0"))
        result["total_unrealized_pnl"] = str(state.get("total_unrealized_pnl", "0"))
        result["position_mode"] = getattr(self.broker, "position_mode", "UNKNOWN")

        clean = symbol.replace("/", "").replace(":", "")
        for pos in state.get("positions", []) or []:
            if pos.get("symbol") != clean:
                continue
            result["leverage"] = str(pos.get("leverage", ""))
            result["margin_mode"] = "ISOLATED" if pos.get("isolated") else "CROSSED"
            result["exchange_entry_price"] = str(pos.get("entryPrice", "0"))
            result["exchange_position_amt"] = str(pos.get("positionAmt", "0"))
            result["unrealized_pnl"] = str(pos.get("unrealizedProfit", "0"))
            break
        else:
            result["issues"].append(f"no position record returned for {clean}")

        if result["position_mode"] not in ("ONE_WAY", "HEDGE"):
            result["matched"] = False
            result["issues"].append(f"unverified position mode {result['position_mode']}")

        result["matched"] = result["matched"] and not result["issues"]
        self._log("ACCOUNT_CONFIG_RECONCILIATION", result)
        if self.health is not None:
            self.health.beat("account_state", "OK" if result["matched"] else "MISMATCH")
        return result

    # -- orders --------------------------------------------------------------

    def query_with_backoff(
        self,
        symbol: str,
        client_order_id: str,
        attempts: int = ABSENCE_PROOF_QUERIES,
        sleep=time.sleep,
    ) -> Tuple[Optional[Order], int, float]:
        """Queries an order by client ID with bounded backoff.

        Returns (order, queries_made, elapsed_s). An exchange that has not yet indexed a
        just-submitted order answers NOT_FOUND for a short while, so a single query can
        never prove absence.
        """
        started = time.monotonic()
        for attempt in range(attempts):
            try:
                found = self.broker.query_order(symbol, client_order_id)
            except Exception as exc:
                logger.warning("Order query failed for %s: %s", client_order_id, exc)
                found = None
            if found is not None:
                return found, attempt + 1, time.monotonic() - started
            if attempt < attempts - 1:
                sleep(min(QUERY_BACKOFF_MAX_S, QUERY_BACKOFF_BASE_S * (2 ** attempt)))
        return None, attempts, time.monotonic() - started

    def resolve_unknown_order(self, symbol: str, client_order_id: str, sleep=time.sleep) -> Dict[str, Any]:
        """Resolves an UNKNOWN submission outcome, applying any fill exactly once.

        Outcome is one of FOUND (state repaired), ABSENT (proven not to exist, safe to
        resubmit) or UNRESOLVED (still unknown - entries stay blocked).
        """
        order, queries, elapsed = self.query_with_backoff(symbol, client_order_id, sleep=sleep)

        if order is not None:
            applied = Decimal("0")
            if self.fill_applier is not None and order.filled_quantity > Decimal("0"):
                applied = self.fill_applier.apply_order_response(order, source="RECONCILIATION")
            elif self.order_manager is not None:
                self.order_manager.upsert_order(order)
            if self.event_store is not None:
                self.event_store.save_order(order)
            self._clear_unknown(client_order_id)
            result = {
                "client_order_id": client_order_id, "outcome": "FOUND",
                "exchange_status": order.status.value, "filled_quantity": str(order.filled_quantity),
                "applied_delta": str(applied), "queries": queries,
            }
        elif queries >= ABSENCE_PROOF_QUERIES and elapsed >= ABSENCE_PROOF_MIN_WINDOW_S and not self._has_account_trade(symbol, client_order_id):
            if self.order_manager is not None:
                self.order_manager.update_order_status(
                    client_order_id, OrderStatus.REJECTED,
                    rejection_reason="Proven absent on exchange after bounded absence proof",
                )
            self._clear_unknown(client_order_id)
            result = {
                "client_order_id": client_order_id, "outcome": "ABSENT",
                "queries": queries, "window_s": round(elapsed, 2),
            }
        else:
            self._mark_unknown(client_order_id)
            result = {
                "client_order_id": client_order_id, "outcome": "UNRESOLVED",
                "queries": queries, "window_s": round(elapsed, 2),
            }

        self._log("UNKNOWN_ORDER_RESOLUTION", result)
        return result

    def reconcile_open_orders(self, open_orders: list) -> Dict[str, Any]:
        """Audits unresolved and open orders, repairing state through the shared fill path."""
        reconciled: List[Dict[str, Any]] = []
        for ord_obj in open_orders:
            if not is_escanor_order(ord_obj.client_order_id):
                # Manual/third-party order: reported for the operator, never touched.
                reconciled.append({
                    "client_order_id": ord_obj.client_order_id,
                    "ownership": "EXTERNAL",
                    "action": "REPORTED_ONLY",
                    "local_status": ord_obj.status.value,
                })
                continue

            try:
                exch_ord = self.broker.query_order(ord_obj.symbol, ord_obj.client_order_id)
            except Exception as exc:
                exch_ord = None
                logger.warning("Order query failed during reconciliation: %s", exc)

            if exch_ord:
                applied = Decimal("0")
                if self.fill_applier is not None and exch_ord.filled_quantity > Decimal("0"):
                    applied = self.fill_applier.apply_order_response(exch_ord, source="RECONCILIATION")
                elif self.order_manager is not None:
                    self.order_manager.upsert_order(exch_ord)
                if self.event_store is not None:
                    self.event_store.save_order(exch_ord)
                if exch_ord.is_terminal:
                    self._clear_unknown(ord_obj.client_order_id)
                reconciled.append({
                    "client_order_id": ord_obj.client_order_id,
                    "ownership": "ESCANOR",
                    "local_status": ord_obj.status.value,
                    "exchange_status": exch_ord.status.value,
                    "filled_qty": str(exch_ord.filled_quantity),
                    "applied_delta": str(applied),
                })
            else:
                if ord_obj.status in (OrderStatus.UNKNOWN, OrderStatus.SUBMITTING):
                    self._mark_unknown(ord_obj.client_order_id)
                reconciled.append({
                    "client_order_id": ord_obj.client_order_id,
                    "ownership": "ESCANOR",
                    "local_status": ord_obj.status.value,
                    "exchange_status": "NOT_FOUND",
                })

        result = {"open_orders_audited": len(open_orders), "details": reconciled}
        self._log("ORDERS_RECONCILIATION", result)
        return result

    def reconcile_recent_fills(self, symbol: str, since_ms: Optional[int] = None) -> Dict[str, Any]:
        """Applies any account trade the engine never saw, exactly once.

        Idempotent by construction: the fill ledger keys on the exchange trade ID, so a
        second run inserts nothing and applies no delta.
        """
        get_trades = getattr(self.broker, "get_user_trades", None)
        if get_trades is None or self.fill_applier is None:
            return {"applied": "0", "trades_seen": 0, "note": "no authenticated trade source"}

        try:
            trades = get_trades(symbol, since_ms)
        except Exception as exc:
            result = {"error": str(exc), "trades_seen": 0, "applied": "0"}
            self._log("FILLS_RECONCILIATION", result)
            return result

        applied_total = Decimal("0")
        recovered = 0
        for trade in sorted(trades, key=lambda t: int(t.get("time", 0))):
            cid = str(trade.get("clientOrderId") or "")
            if not is_escanor_order(cid):
                eid = str(trade.get("orderId")) if trade.get("orderId") is not None else None
                if eid:
                    if self.order_manager:
                        ord_obj = self.order_manager.get_order_by_exchange_id(eid)
                        if ord_obj:
                            cid = ord_obj.client_order_id
                    if (not cid or not is_escanor_order(cid)) and self.event_store:
                        lookup_fn = getattr(self.event_store, "get_order_by_exchange_id", None)
                        if lookup_fn:
                            ord_obj = lookup_fn(eid)
                            if ord_obj:
                                cid = ord_obj.client_order_id
            if not is_escanor_order(cid):
                continue
            qty = Decimal(str(trade.get("qty", "0")))
            delta = self.fill_applier.apply(
                client_order_id=cid,
                symbol=str(trade.get("symbol", symbol)),
                side=OrderSide(str(trade.get("side", "BUY")).upper()),
                status=OrderStatus.PARTIALLY_FILLED,
                cumulative_qty=self._cumulative_after(cid, qty),
                last_qty=qty,
                last_price=Decimal(str(trade.get("price", "0"))),
                commission=Decimal(str(trade.get("commission", "0"))),
                commission_asset=trade.get("commissionAsset"),
                exchange_order_id=str(trade.get("orderId")) if trade.get("orderId") is not None else None,
                trade_id=str(trade.get("id")) if trade.get("id") is not None else None,
                transaction_time=trade.get("time"),
                source="RECONCILIATION_TRADES",
            )
            if delta > Decimal("0"):
                recovered += 1
            applied_total += delta

        result = {"trades_seen": len(trades), "recovered_fills": recovered, "applied": str(applied_total)}
        self._log("FILLS_RECONCILIATION", result)
        return result

    def reconcile_all(self, symbol: str, open_orders: Optional[list] = None,
                      since_ms: Optional[int] = None) -> Dict[str, Any]:
        """Full reconciliation pass, safe to repeat on startup and on a timer."""
        report = {
            "balance": self.reconcile_balance(),
            "account": self.reconcile_account_configuration(symbol),
            "fills": self.reconcile_recent_fills(symbol, since_ms),
            "orders": self.reconcile_open_orders(open_orders or []),
            "position": self.reconcile_position(),
        }
        unresolved = [
            d["client_order_id"] for d in report["orders"]["details"]
            if d.get("ownership") == "ESCANOR" and d.get("exchange_status") == "NOT_FOUND"
        ]
        report["unresolved_orders"] = unresolved
        report["reconciled"] = (
            report["position"]["matched"] and report["account"].get("matched", True) and not unresolved
        )

        if not report["reconciled"]:
            self._require_reconciliation(
                f"Reconciliation incomplete for {symbol}: unresolved={unresolved}, "
                f"position_matched={report['position']['matched']}"
            )
        elif self.health is not None:
            self.health.reconciliation_required = False

        self._log("RECONCILIATION_COMPLETED", report)
        return report

    # -- helpers -------------------------------------------------------------

    def _cumulative_after(self, client_order_id: str, qty: Decimal) -> Decimal:
        """Cumulative quantity implied by adding one recovered trade to an order."""
        order = self.order_manager.get_order_by_client_id(client_order_id) if self.order_manager else None
        base = order.applied_cumulative_qty if order else Decimal("0")
        return base + qty

    def _has_account_trade(self, symbol: str, client_order_id: str) -> bool:
        """Whether the exchange knows any trade for this client order ID."""
        get_trades = getattr(self.broker, "get_user_trades", None)
        if get_trades is None:
            return False
        target_eid = None
        if self.order_manager:
            ord_obj = self.order_manager.get_order_by_client_id(client_order_id)
            if ord_obj and ord_obj.exchange_order_id:
                target_eid = str(ord_obj.exchange_order_id)
        if not target_eid and self.event_store:
            lookup_fn = getattr(self.event_store, "get_order_by_client_id", None)
            if lookup_fn:
                ord_obj = lookup_fn(client_order_id)
                if ord_obj and ord_obj.exchange_order_id:
                    target_eid = str(ord_obj.exchange_order_id)
        try:
            for t in get_trades(symbol, None):
                if str(t.get("clientOrderId")) == client_order_id:
                    return True
                if target_eid and str(t.get("orderId")) == target_eid:
                    return True
            return False
        except Exception:
            # Unreadable trade history cannot prove absence: assume it may exist.
            return True

    def _mark_unknown(self, client_order_id: str) -> None:
        if self.health is not None:
            self.health.unknown_orders.add(client_order_id)

    def _clear_unknown(self, client_order_id: str) -> None:
        if self.health is not None:
            self.health.unknown_orders.discard(client_order_id)

    def _require_reconciliation(self, reason: str) -> None:
        logger.error("RECONCILIATION_REQUIRED: %s", reason)
        if self.health is not None:
            self.health.reconciliation_required = True
        if self.event_store is not None:
            self.event_store.record_incident("RECONCILIATION_REQUIRED", "HIGH", reason)
        if self.broker.mode == "LIVE" and self.kill_switch is not None:
            self.kill_switch.trigger(reason=reason, triggered_by="RECONCILER")

    def _log(self, event_type: str, payload: Dict[str, Any]) -> None:
        if self.event_store is None:
            return
        try:
            self.event_store.log_event(event_type, payload)
        except Exception as exc:
            logger.warning("Failed to log %s: %s", event_type, exc)
