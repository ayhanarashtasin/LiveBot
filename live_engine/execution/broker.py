"""Execution broker abstraction supporting SHADOW, PAPER, TESTNET, and BINANCE LIVE modes.

Safety constraints:
- Default mode is SHADOW.
- Real order placement strictly requires mode="LIVE" and ESCANOR_LIVE_TRADING_ENABLED="true".
- TESTNET mode uses isolated testnet credentials and endpoints, never falling back to live.
- Server time offset synchronization prevents clock drift errors.
- Network timeouts on order submission produce UNKNOWN status and require query-before-retry.
"""
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional, Dict, Any, Tuple
import hashlib
import hmac
import json
import logging
import os
import time
import urllib.parse
import urllib.request
import urllib.error
import ssl
try:
    import certifi
    DEFAULT_SSL_CONTEXT = ssl.create_default_context(cafile=certifi.where())
except Exception:
    DEFAULT_SSL_CONTEXT = None

from live_engine.execution.models import Order, OrderSide, OrderStatus, OrderType
from live_engine.execution.position_manager import PositionManager

logger = logging.getLogger(__name__)

# Binance documents 429 (rate limit), 418 (IP auto-ban) and the 5xx gateway codes as
# retryable with backoff. Anything else is a definite answer and must not be retried.
RETRYABLE_HTTP_CODES = (429, 418, 502, 504)
RETRY_ATTEMPTS = 4
RETRY_BASE_S = 1.0
RETRY_MAX_S = 60.0


def retry_after_seconds(error: Any, attempt: int) -> float:
    """Backoff for a rate-limited response: Retry-After when supplied, else exponential."""
    header = None
    try:
        header = error.headers.get("Retry-After") if getattr(error, "headers", None) else None
    except Exception:
        header = None
    if header:
        try:
            return min(RETRY_MAX_S, max(0.0, float(header)))
        except (TypeError, ValueError):
            pass
    return min(RETRY_MAX_S, RETRY_BASE_S * (2 ** attempt))


class BinanceUnknownOrderError(Exception):
    """Raised when an order submission result is unknown due to network timeout or 5xx."""
    def __init__(self, message: str, client_order_id: str):
        super().__init__(message)
        self.client_order_id = client_order_id


class PaperExecutionModel:
    """Explicit paper-execution assumptions.

    The approved manifests declare ``slippage.model: zero_or_measured``, so the default is
    a zero-impact model. That is *benchmark replay*, not a claim about real fills, and it
    is labelled as such everywhere it is persisted or displayed.
    """

    def __init__(
        self,
        label: str = "benchmark_replay",
        spread_bps: Decimal = Decimal("0"),
        slippage_bps: Decimal = Decimal("0"),
        latency_ms: int = 0,
    ):
        self.label = label
        self.spread_bps = Decimal(str(spread_bps))
        self.slippage_bps = Decimal(str(slippage_bps))
        self.latency_ms = int(latency_ms)

    @property
    def is_zero_impact(self) -> bool:
        return self.spread_bps == Decimal("0") and self.slippage_bps == Decimal("0")

    def arrival_price(self, side: OrderSide, reference: Decimal) -> Decimal:
        """Market-order arrival price: cross half the spread, then pay slippage."""
        if reference <= Decimal("0"):
            return reference
        impact = (self.spread_bps / Decimal("2") + self.slippage_bps) / Decimal("10000")
        return reference * (Decimal("1") + impact) if side == OrderSide.BUY else reference * (Decimal("1") - impact)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "label": self.label,
            "spread_bps": str(self.spread_bps),
            "slippage_bps": str(self.slippage_bps),
            "latency_ms": self.latency_ms,
            "simulated": True,
        }


class UnsupportedPositionModeError(Exception):
    """Raised when the account's futures position mode is unsupported or undeterminable."""


class ExecutionBroker(ABC):
    """Abstract base broker interface."""

    def __init__(self, mode: str):
        self.mode = mode.upper()

    # One-way is the only position mode Escanor's single-long sizing model is defined for;
    # brokers that talk to an exchange overwrite this after querying the account.
    position_mode: str = "ONE_WAY"

    @abstractmethod
    def place_order(
        self,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        quantity: Decimal,
        price: Optional[Decimal] = None,
        client_order_id: Optional[str] = None,
        reduce_only: bool = False,
        position_side: Optional[str] = None,
    ) -> Order:
        """Submits an order to the execution venue or simulator.

        reduce_only marks an exit: it must never be able to open a reverse position.
        """
        pass

    @abstractmethod
    def cancel_order(self, symbol: str, client_order_id: str) -> bool:
        """Cancels an existing open order."""
        pass

    @abstractmethod
    def query_order(self, symbol: str, client_order_id: str) -> Optional[Order]:
        """Queries the status of an order."""
        pass

    @abstractmethod
    def get_account_balance(self) -> Dict[str, Decimal]:
        """Fetches current account balances."""
        pass

    @abstractmethod
    def get_position(self, symbol: str) -> Tuple[Optional[OrderSide], Decimal, Decimal]:
        """Returns (side, quantity, entry_price) for symbol."""
        pass


class ShadowBroker(ExecutionBroker):
    """Shadow broker that simulates orders and maintains expected strategy position without exchange interaction."""

    def __init__(self):
        super().__init__("SHADOW")
        self.positions: Dict[str, PositionManager] = {}
        self.orders: Dict[str, Order] = {}

    def _get_pm(self, symbol: str) -> PositionManager:
        if symbol not in self.positions:
            self.positions[symbol] = PositionManager(symbol)
        return self.positions[symbol]

    def place_order(
        self,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        quantity: Decimal,
        price: Optional[Decimal] = None,
        client_order_id: Optional[str] = None,
        reduce_only: bool = False,
        position_side: Optional[str] = None,
    ) -> Order:
        cid = client_order_id or f"SHADOW-{int(time.time()*1000)}"
        exec_price = price or Decimal("0")
        pm = self._get_pm(symbol)
        # SHADOW enforces the same no-reversal invariant as the exchange: a reduce-only
        # exit closes at most what is open.
        if reduce_only:
            quantity = min(quantity, pm.quantity) if not pm.is_flat and pm.side != side else Decimal("0")
        order = Order(
            client_order_id=cid,
            symbol=symbol,
            side=side,
            order_type=order_type,
            quantity=quantity,
            price=price,
            status=OrderStatus.FILLED,
            filled_quantity=quantity,
            avg_fill_price=exec_price,
            accumulated_fees=Decimal("0"),
            created_at=int(time.time() * 1000),
            filled_at=int(time.time() * 1000),
            reduce_only=reduce_only,
            position_side=position_side,
        )
        self.orders[cid] = order
        pm.on_fill(side, quantity, exec_price, reduce_only=reduce_only)
        logger.info(f"[SHADOW ORDER] {side.value} {quantity} {symbol} @ {exec_price} (ID: {cid})")
        return order

    def cancel_order(self, symbol: str, client_order_id: str) -> bool:
        logger.info(f"[SHADOW CANCEL] Order {client_order_id}")
        return True

    def query_order(self, symbol: str, client_order_id: str) -> Optional[Order]:
        return self.orders.get(client_order_id)

    def get_account_balance(self) -> Dict[str, Decimal]:
        return {"USDT": Decimal("10000.0")}

    def get_position(self, symbol: str) -> Tuple[Optional[OrderSide], Decimal, Decimal]:
        pm = self._get_pm(symbol)
        return (pm.side, pm.quantity, pm.entry_price)


class PaperBroker(ExecutionBroker):
    """Paper trading broker simulating USD-M Futures execution with realistic margin, fees, and state persistence."""

    def __init__(
        self,
        initial_balance: Decimal = Decimal("10000.0"),
        taker_fee: Decimal = Decimal("0.0005"),
        leverage: int = 1,
        event_store: Optional[Any] = None,
        execution_model: Optional[PaperExecutionModel] = None,
        filters: Optional[Any] = None,
    ):
        super().__init__("PAPER")
        self.balance = initial_balance
        self.initial_balance = initial_balance
        self.taker_fee = taker_fee
        self.leverage = leverage
        self.event_store = event_store
        self.execution_model = execution_model or PaperExecutionModel()
        self.filters = filters
        self.funding_paid = Decimal("0")
        self.positions: Dict[str, PositionManager] = {}
        self.orders: Dict[str, Order] = {}

        if self.event_store is not None:
            self._restore_from_store()

    def _get_pm(self, symbol: str) -> PositionManager:
        if symbol not in self.positions:
            self.positions[symbol] = PositionManager(symbol)
        return self.positions[symbol]

    def _restore_from_store(self) -> None:
        """Restores paper account balance and open position from persistent event store."""
        try:
            events = self.event_store.get_events_by_type("PAPER_ACCOUNT_SNAPSHOT")
            if events:
                last_snap = events[-1]["payload"]
                self.balance = Decimal(str(last_snap.get("balance", self.initial_balance)))
                positions_data = last_snap.get("positions", {})
                for sym, pos in positions_data.items():
                    pm = self._get_pm(sym)
                    side_str = pos.get("side")
                    side = OrderSide(side_str) if side_str else None
                    pm.sync_from_exchange(
                        exchange_side=side,
                        exchange_qty=Decimal(str(pos.get("quantity", "0"))),
                        exchange_entry_price=Decimal(str(pos.get("entry_price", "0"))),
                    )
                    pm.realized_pnl = Decimal(str(pos.get("realized_pnl", "0")))
                    pm.trade_count = int(pos.get("trade_count", 0))
                logger.info(f"Restored PAPER account state from event store: balance={self.balance}")
        except Exception as e:
            logger.warning(f"Failed to restore paper account state: {e}")

    def _save_to_store(self) -> None:
        """Persists paper account state snapshot into event store."""
        if self.event_store is None:
            return
        snap = {
            "balance": str(self.balance),
            "positions": {sym: pm.to_dict() for sym, pm in self.positions.items()},
            # Everything in a PAPER snapshot is simulated. Labelling it here means no
            # dashboard or report can present it as an observed exchange value.
            "simulated": True,
            "value_source": "SIMULATED",
            "execution_model": self.execution_model.to_dict(),
            "funding_paid": str(self.funding_paid),
        }
        try:
            self.event_store.log_event("PAPER_ACCOUNT_SNAPSHOT", snap)
        except Exception as e:
            logger.warning(f"Failed to persist paper account snapshot: {e}")

    def place_order(
        self,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        quantity: Decimal,
        price: Optional[Decimal] = None,
        client_order_id: Optional[str] = None,
        reduce_only: bool = False,
        position_side: Optional[str] = None,
    ) -> Order:
        cid = client_order_id or f"PAPER-{int(time.time()*1000)}"
        exec_price = self.simulated_fill_price(side, price)
        pm = self._get_pm(symbol)

        # PAPER enforces the same no-reversal invariant: a reduce-only exit can only
        # close what is actually open.
        if reduce_only:
            quantity = min(quantity, pm.quantity) if not pm.is_flat and pm.side != side else Decimal("0")
            if quantity <= Decimal("0"):
                rejected = Order(
                    client_order_id=cid, symbol=symbol, side=side, order_type=order_type,
                    quantity=Decimal("0"), price=price, status=OrderStatus.REJECTED,
                    rejection_reason="Reduce-only exit rejected: no opposing position open",
                    created_at=int(time.time() * 1000), reduce_only=True, position_side=position_side,
                )
                self.orders[cid] = rejected
                return rejected

        if self.filters is not None:
            valid, why = self.filters.validate_order(quantity, exec_price)
            if not valid:
                rejected = Order(
                    client_order_id=cid, symbol=symbol, side=side, order_type=order_type,
                    quantity=quantity, price=price, status=OrderStatus.REJECTED,
                    rejection_reason=f"Exchange filter rejected: {why}",
                    created_at=int(time.time() * 1000), reduce_only=reduce_only,
                    position_side=position_side,
                )
                self.orders[cid] = rejected
                logger.warning("[PAPER REJECT] %s", rejected.rejection_reason)
                return rejected

        notional = quantity * exec_price
        fee = notional * self.taker_fee

        # Margin & Balance validation for opening / increasing positions
        if pm.is_flat or pm.side == side:
            required_margin = (notional / Decimal(str(self.leverage))) + fee
            if self.balance < required_margin:
                logger.warning(
                    f"Paper order rejected: Insufficient margin (available: {self.balance}, required: {required_margin})"
                )
                order = Order(
                    client_order_id=cid,
                    symbol=symbol,
                    side=side,
                    order_type=order_type,
                    quantity=quantity,
                    price=price,
                    status=OrderStatus.REJECTED,
                    rejection_reason=f"Insufficient margin (available: {self.balance}, required: {required_margin})",
                    created_at=int(time.time() * 1000),
                )
                self.orders[cid] = order
                return order

        order = Order(
            client_order_id=cid,
            symbol=symbol,
            side=side,
            order_type=order_type,
            quantity=quantity,
            price=price,
            status=OrderStatus.FILLED,
            filled_quantity=quantity,
            avg_fill_price=exec_price,
            accumulated_fees=fee,
            created_at=int(time.time() * 1000),
            filled_at=int(time.time() * 1000),
            reduce_only=reduce_only,
            position_side=position_side,
        )
        self.orders[cid] = order

        # Track position state before fill
        was_open = not pm.is_flat and pm.quantity > 0
        prev_side = pm.side
        prev_entry_price = pm.entry_price

        # Apply fill to position manager
        pm.on_fill(side, quantity, exec_price, fee, reduce_only=reduce_only)

        # Update balance based on transaction:
        # For opening: deduct notional/leverage (margin) + fee
        # For closing: release margin + add PnL - fee
        if not was_open or (was_open and prev_side == side):
            # Opening new position: deduct notional/leverage (margin) + fee
            margin_required = (notional / Decimal(str(self.leverage)))
            self.balance -= (margin_required + fee)
        else:
            # Closing position: release margin + add PnL - fee
            entry_notional = prev_entry_price * quantity
            exit_notional = quantity * exec_price
            margin_locked = entry_notional / Decimal(str(self.leverage))
            # Calculate PnL based on position direction
            if prev_side == OrderSide.BUY:
                pnl = exit_notional - entry_notional
            else:
                pnl = entry_notional - exit_notional
            self.balance += margin_locked + pnl - fee

        self._save_to_store()
        logger.info(f"[PAPER FILL] {side.value} {quantity} {symbol} @ {exec_price} fee: {fee} balance: {self.balance} (ID: {cid})")
        return order

    def simulated_fill_price(self, side: OrderSide, reference: Optional[Decimal]) -> Decimal:
        """Simulated market fill price under the declared execution model.

        The price is always derived from the reference the order was actually submitted
        with; a paper fill is never backfilled at a price that was only knowable before
        the order became actionable.
        """
        ref = reference if reference is not None else Decimal("60000.0")
        return self.execution_model.arrival_price(side, ref)

    def mark_position(self, symbol: str, mark_price: Decimal) -> Decimal:
        """Marks the open position to the current market/mark price."""
        return self._get_pm(symbol).update_unrealized_pnl(mark_price)

    def apply_funding(self, symbol: str, funding_rate: Decimal, mark_price: Decimal) -> Decimal:
        """Applies one USD-M funding settlement over the evaluated holding period."""
        pm = self._get_pm(symbol)
        if pm.is_flat:
            return Decimal("0")
        payment = pm.quantity * mark_price * Decimal(str(funding_rate))
        if pm.side == OrderSide.SELL:
            payment = -payment
        self.balance -= payment
        self.funding_paid += payment
        self._save_to_store()
        return payment

    def cancel_order(self, symbol: str, client_order_id: str) -> bool:
        order = self.orders.get(client_order_id)
        if order and not order.is_terminal:
            order.status = OrderStatus.CANCELED
            return True
        return False

    def query_order(self, symbol: str, client_order_id: str) -> Optional[Order]:
        return self.orders.get(client_order_id)

    def get_account_balance(self) -> Dict[str, Decimal]:
        return {"USDT": self.balance}

    def get_position(self, symbol: str) -> Tuple[Optional[OrderSide], Decimal, Decimal]:
        pm = self._get_pm(symbol)
        return (pm.side, pm.quantity, pm.entry_price)


class BaseBinanceBroker(ExecutionBroker):
    """Base authenticated Binance USD-M Futures REST client."""

    def __init__(self, mode: str, api_key: str, api_secret: str, base_url: str):
        super().__init__(mode)
        if not api_key or not api_secret:
            raise ValueError("Both api_key and api_secret are required for Binance brokers")
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = base_url
        self._time_offset_ms = 0
        self.sync_time_offset()

    def sync_time_offset(self) -> int:
        """Calculates server time offset to eliminate timestamp drift issues."""
        try:
            req = urllib.request.Request(f"{self.base_url}/fapi/v1/time", method="GET")
            urlopen_kwargs: Dict[str, Any] = {"timeout": 5}
            if DEFAULT_SSL_CONTEXT:
                urlopen_kwargs["context"] = DEFAULT_SSL_CONTEXT
            with urllib.request.urlopen(req, **urlopen_kwargs) as resp:
                server_time = json.loads(resp.read().decode("utf-8")).get("serverTime", 0)
                if server_time:
                    self._time_offset_ms = server_time - int(time.time() * 1000)
                    logger.debug(f"Binance server time offset synchronized: {self._time_offset_ms}ms")
        except Exception as e:
            logger.warning(f"Could not synchronize server time: {e}")
        return self._time_offset_ms

    def _sign_query(self, params: Dict[str, Any]) -> str:
        """Generates HMAC-SHA256 signature for Binance API query."""
        params["timestamp"] = int(time.time() * 1000) + self._time_offset_ms
        query_string = urllib.parse.urlencode(params)
        signature = hmac.new(
            self.api_secret.encode("utf-8"),
            query_string.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        return f"{query_string}&signature={signature}"

    def _request(self, method: str, endpoint: str, params: Optional[Dict[str, Any]] = None,
                 attempt: int = 0) -> Dict[str, Any]:
        """Makes a signed HTTPS request, honouring documented rate-limit backoff.

        429/418/502/504 are retried with bounded exponential backoff, preferring the
        server's Retry-After header when it supplies one. Order submission is excluded
        from blind retry: an ambiguous POST becomes UNKNOWN and is reconciled instead.
        """
        params = params or {}
        original_params = {k: v for k, v in params.items() if k != "timestamp"}
        signed_query = self._sign_query(params)

        if method == "GET":
            url = f"{self.base_url}{endpoint}?{signed_query}"
            data = None
        else:
            url = f"{self.base_url}{endpoint}"
            data = signed_query.encode("utf-8")

        req = urllib.request.Request(
            url,
            data=data,
            headers={
                "X-MBX-APIKEY": self.api_key,
                "Content-Type": "application/x-www-form-urlencoded",
                "User-Agent": "Escanor-LiveBot/1.0",
            },
            method=method,
        )

        try:
            urlopen_kwargs: Dict[str, Any] = {"timeout": 10}
            if DEFAULT_SSL_CONTEXT:
                urlopen_kwargs["context"] = DEFAULT_SSL_CONTEXT
            with urllib.request.urlopen(req, **urlopen_kwargs) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            err_content = e.read().decode("utf-8")
            # Never echo the signed query string (it carries the signature) into the log.
            logger.error(f"Binance API HTTP error {e.code} on {endpoint}: {err_content[:400]}")
            is_order_post = method == "POST" and "/order" in endpoint
            if e.code in RETRYABLE_HTTP_CODES and not is_order_post and attempt < RETRY_ATTEMPTS - 1:
                delay = retry_after_seconds(e, attempt)
                logger.warning(
                    f"Binance returned {e.code} on {endpoint}; backing off {delay:.1f}s "
                    f"(attempt {attempt + 1}/{RETRY_ATTEMPTS})"
                )
                time.sleep(delay)
                return self._request(method, endpoint, dict(original_params), attempt + 1)
            try:
                err_json = json.loads(err_content)
                code = err_json.get("code")
                msg = err_json.get("msg")
                # Error codes where order placement outcome is unknown
                if e.code in (500, 502, 503, 504) or code in (-1001, -1003, -1007, -1021):
                    cid = params.get("newClientOrderId", "")
                    raise BinanceUnknownOrderError(f"Binance order outcome unknown (HTTP {e.code}, code {code}: {msg})", cid)
                raise RuntimeError(f"Binance error code {code}: {msg}")
            except BinanceUnknownOrderError:
                raise
            except Exception:
                raise RuntimeError(f"Binance HTTP {e.code}: {err_content}")
        except (urllib.error.URLError, TimeoutError) as e:
            cid = params.get("newClientOrderId", "")
            if method == "POST" and "/order" in endpoint:
                raise BinanceUnknownOrderError(f"Order submission network timeout/error: {e}", cid)
            raise

    def detect_position_mode(self) -> str:
        """Reads and validates the account's futures position mode.

        Escanor's sizing and exit model is defined for one-way mode (reduceOnly exits) and
        for hedge mode with an explicit positionSide. Anything else - including a mode that
        cannot be determined - fails closed before the engine can reach READY.
        """
        try:
            resp = self._request("GET", "/fapi/v1/positionSide/dual", {})
        except Exception as exc:
            raise UnsupportedPositionModeError(
                f"UNSUPPORTED POSITION MODE: could not determine Binance position mode ({exc}). "
                f"Refusing to start authenticated trading with an unverified position mode."
            ) from exc

        dual = resp.get("dualSidePosition")
        if dual is None:
            raise UnsupportedPositionModeError(
                "UNSUPPORTED POSITION MODE: /fapi/v1/positionSide/dual response lacks "
                "dualSidePosition; position mode is unverified."
            )
        if isinstance(dual, str):
            if dual.lower() not in ("true", "false"):
                raise UnsupportedPositionModeError(
                    f"UNSUPPORTED POSITION MODE: unparseable dualSidePosition value {dual}."
                )
            dual = dual.lower() == "true"
        self.position_mode = "HEDGE" if bool(dual) else "ONE_WAY"
        logger.info(f"Binance futures position mode detected: {self.position_mode}")
        return self.position_mode

    def exit_order_params(self, position_side_of_entry: str = "LONG") -> Dict[str, Any]:
        """Exchange parameters that make an exit incapable of opening a reverse position.

        One-way mode uses reduceOnly. Hedge mode rejects reduceOnly and instead requires an
        explicit positionSide, which is inherently position-scoped.
        """
        if self.position_mode == "ONE_WAY":
            return {"reduceOnly": "true"}
        if self.position_mode == "HEDGE":
            return {"positionSide": position_side_of_entry}
        raise UnsupportedPositionModeError(
            f"UNSUPPORTED POSITION MODE: {self.position_mode}. Detect the position mode "
            f"before submitting exits."
        )

    def get_mark_price(self, symbol: str) -> Tuple[Decimal, int]:
        """Current Binance mark price and its event time in ms."""
        resp = self._request("GET", "/fapi/v1/premiumIndex", {"symbol": symbol.replace("/", "").replace(":", "")})
        if isinstance(resp, list):
            resp = resp[0] if resp else {}
        return Decimal(str(resp.get("markPrice", "0"))), int(resp.get("time", time.time() * 1000))

    def get_account_state(self) -> Dict[str, Any]:
        """Authoritative account snapshot used by the equity and margin guards."""
        resp = self._request("GET", "/fapi/v2/account", {})
        return {
            "total_wallet_balance": Decimal(str(resp.get("totalWalletBalance", "0"))),
            "total_margin_balance": Decimal(str(resp.get("totalMarginBalance", "0"))),
            "total_unrealized_pnl": Decimal(str(resp.get("totalUnrealizedProfit", "0"))),
            "available_balance": Decimal(str(resp.get("availableBalance", "0"))),
            "positions": resp.get("positions", []),
            "fetched_at_ms": int(time.time() * 1000),
        }

    def get_user_trades(self, symbol: str, start_time_ms: Optional[int] = None, limit: int = 500) -> list:
        """Recent account trades for this symbol (used by reconciliation to recover fills)."""
        params: Dict[str, Any] = {"symbol": symbol.replace("/", "").replace(":", ""), "limit": limit}
        if start_time_ms is not None:
            params["startTime"] = int(start_time_ms)
        resp = self._request("GET", "/fapi/v1/userTrades", params)
        return resp if isinstance(resp, list) else []

    def place_order(
        self,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        quantity: Decimal,
        price: Optional[Decimal] = None,
        client_order_id: Optional[str] = None,
        reduce_only: bool = False,
        position_side: Optional[str] = None,
    ) -> Order:
        cid = client_order_id or f"ESC-{int(time.time()*1000)}"
        params: Dict[str, Any] = {
            "symbol": symbol.replace("/", "").replace(":", ""),
            "side": side.value,
            "type": order_type.value,
            "quantity": str(quantity),
            "newClientOrderId": cid,
        }

        if reduce_only:
            params.update(self.exit_order_params(position_side or "LONG"))
        elif position_side and self.position_mode == "HEDGE":
            params["positionSide"] = position_side

        if order_type == OrderType.LIMIT:
            if price is None:
                raise ValueError("LIMIT order requires price")
            params["price"] = str(price)
            params["timeInForce"] = "GTC"

        logger.info(f"Submitting order to {self.mode}: {params}")
        resp = self._request("POST", "/fapi/v1/order", params)

        status_map = {
            "NEW": OrderStatus.NEW,
            "PARTIALLY_FILLED": OrderStatus.PARTIALLY_FILLED,
            "FILLED": OrderStatus.FILLED,
            "CANCELED": OrderStatus.CANCELED,
            "REJECTED": OrderStatus.REJECTED,
            "EXPIRED": OrderStatus.EXPIRED,
        }

        exchange_status = resp.get("status", "NEW")
        order_status = status_map.get(exchange_status, OrderStatus.NEW)
        executed_qty = Decimal(str(resp.get("executedQty", "0")))
        avg_price = Decimal(str(resp.get("avgPrice", "0"))) if resp.get("avgPrice") else None

        if order_type == OrderType.MARKET and order_status == OrderStatus.NEW:
            time.sleep(0.1)
            try:
                queried = self.query_order(symbol, cid)
                if queried and queried.status in (OrderStatus.FILLED, OrderStatus.PARTIALLY_FILLED):
                    order_status = queried.status
                    executed_qty = queried.filled_quantity
                    avg_price = queried.average_fill_price
            except Exception as q_err:
                logger.debug(f"Immediate fill query for {cid} deferred: {q_err}")

        return Order(
            client_order_id=cid,
            exchange_order_id=str(resp.get("orderId")),
            symbol=symbol,
            side=side,
            order_type=order_type,
            quantity=quantity,
            price=price,
            status=order_status,
            filled_quantity=executed_qty,
            avg_fill_price=avg_price,
            created_at=int(time.time() * 1000),
            acknowledged_at=int(time.time() * 1000),
            reduce_only=bool(resp.get("reduceOnly", reduce_only)),
            position_side=resp.get("positionSide", params.get("positionSide")),
        )

    def cancel_order(self, symbol: str, client_order_id: str) -> bool:
        params = {
            "symbol": symbol.replace("/", "").replace(":", ""),
            "origClientOrderId": client_order_id,
        }
        try:
            resp = self._request("DELETE", "/fapi/v1/order", params)
            return resp.get("status") in ("CANCELED", "NEW")
        except Exception as e:
            logger.error(f"Failed to cancel order {client_order_id}: {e}")
            return False

    def query_order(self, symbol: str, client_order_id: str) -> Optional[Order]:
        """Queries order status by origClientOrderId. Handles code -2013 gracefully."""
        params = {
            "symbol": symbol.replace("/", "").replace(":", ""),
            "origClientOrderId": client_order_id,
        }
        try:
            resp = self._request("GET", "/fapi/v1/order", params)
            status_map = {
                "NEW": OrderStatus.NEW,
                "PARTIALLY_FILLED": OrderStatus.PARTIALLY_FILLED,
                "FILLED": OrderStatus.FILLED,
                "CANCELED": OrderStatus.CANCELED,
                "REJECTED": OrderStatus.REJECTED,
                "EXPIRED": OrderStatus.EXPIRED,
            }
            return Order(
                client_order_id=client_order_id,
                exchange_order_id=str(resp.get("orderId")),
                symbol=symbol,
                side=OrderSide(resp.get("side", "BUY")),
                order_type=OrderType(resp.get("type", "MARKET")),
                quantity=Decimal(str(resp.get("origQty", "0"))),
                price=Decimal(str(resp.get("price", "0"))) if resp.get("price") else None,
                status=status_map.get(resp.get("status", "NEW"), OrderStatus.UNKNOWN),
                filled_quantity=Decimal(str(resp.get("executedQty", "0"))),
                avg_fill_price=Decimal(str(resp.get("avgPrice", "0"))) if resp.get("avgPrice") else None,
                created_at=int(resp.get("time", time.time() * 1000)),
                reduce_only=bool(resp.get("reduceOnly", False)),
                position_side=resp.get("positionSide"),
            )
        except Exception as e:
            if "-2013" in str(e) or "Order does not exist" in str(e):
                return None
            logger.error(f"Failed to query order {client_order_id}: {e}")
            raise

    def get_account_balance(self) -> Dict[str, Decimal]:
        resp = self._request("GET", "/fapi/v2/balance")
        balances = {}
        for b in resp:
            asset = b.get("asset")
            balance = Decimal(str(b.get("balance", "0")))
            balances[asset] = balance
        return balances

    def get_position(self, symbol: str) -> Tuple[Optional[OrderSide], Decimal, Decimal]:
        params = {"symbol": symbol.replace("/", "").replace(":", "")}
        resp = self._request("GET", "/fapi/v2/positionRisk", params)
        for p in resp:
            amt = Decimal(str(p.get("positionAmt", "0")))
            entry_px = Decimal(str(p.get("entryPrice", "0")))
            if amt > Decimal("0"):
                return (OrderSide.BUY, amt, entry_px)
            elif amt < Decimal("0"):
                return (OrderSide.SELL, abs(amt), entry_px)
        return (None, Decimal("0"), Decimal("0"))

    def get_symbol_filters(self, symbol: str) -> Optional["SymbolFilters"]:
        """Fetches live exchange filters for a symbol from Binance."""
        try:
            resp = self._request("GET", "/fapi/v1/exchangeInfo", {})
            symbols_data = resp.get("symbols", [])
            for sym_data in symbols_data:
                if sym_data.get("symbol") == symbol.replace("/", "").replace(":", ""):
                    filters_list = sym_data.get("filters", [])
                    filters_dict = {f["filterType"]: f for f in filters_list}

                    tick_size = Decimal(str(filters_dict.get("PRICE_FILTER", {}).get("tickSize", "0.10")))
                    step_size = Decimal(str(filters_dict.get("LOT_SIZE", {}).get("stepSize", "0.001")))
                    min_qty = Decimal(str(filters_dict.get("LOT_SIZE", {}).get("minQty", "0.001")))
                    max_qty = Decimal(str(filters_dict.get("LOT_SIZE", {}).get("maxQty", "1000.0")))
                    min_notional = Decimal(str(filters_dict.get("MIN_NOTIONAL", {}).get("notional", "5.0")))

                    from live_engine.execution.order_filters import SymbolFilters
                    return SymbolFilters(
                        symbol=symbol,
                        tick_size=tick_size,
                        step_size=step_size,
                        min_qty=min_qty,
                        max_qty=max_qty,
                        min_notional=min_notional,
                    )
            return None
        except Exception as e:
            logger.warning(f"Failed to fetch symbol filters for {symbol}: {e}")
            return None


class BinanceTestnetBroker(BaseBinanceBroker):
    """Binance USD-M Futures Testnet broker with isolated testnet credentials and endpoints."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
    ):
        testnet_key = api_key or os.environ.get("BINANCE_TESTNET_API_KEY", "")
        testnet_secret = api_secret or os.environ.get("BINANCE_TESTNET_API_SECRET", "")
        if not testnet_key or not testnet_secret:
            logger.warning("BinanceTestnetBroker initialized without testnet credentials.")
        super().__init__(
            mode="TESTNET",
            api_key=testnet_key,
            api_secret=testnet_secret,
            base_url="https://testnet.binancefuture.com",
        )


class BinanceLiveBroker(BaseBinanceBroker):
    """Production Binance USD-M Futures REST execution broker."""

    def __init__(
        self,
        api_key: str,
        api_secret: str,
    ):
        super().__init__(
            mode="LIVE",
            api_key=api_key,
            api_secret=api_secret,
            base_url="https://fapi.binance.com",
        )
        enabled = os.environ.get("ESCANOR_LIVE_TRADING_ENABLED", "").lower() == "true"
        if not enabled:
            logger.warning("BinanceLiveBroker initialized in live mode but ESCANOR_LIVE_TRADING_ENABLED != true")

    def place_order(
        self,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        quantity: Decimal,
        price: Optional[Decimal] = None,
        client_order_id: Optional[str] = None,
        reduce_only: bool = False,
        position_side: Optional[str] = None,
    ) -> Order:
        enabled = os.environ.get("ESCANOR_LIVE_TRADING_ENABLED", "").lower() == "true"
        if not enabled:
            raise PermissionError(
                "Live order placement blocked: ESCANOR_LIVE_TRADING_ENABLED must be set to 'true'"
            )
        return super().place_order(
            symbol=symbol,
            side=side,
            order_type=order_type,
            quantity=quantity,
            price=price,
            client_order_id=client_order_id,
            reduce_only=reduce_only,
            position_side=position_side,
        )
