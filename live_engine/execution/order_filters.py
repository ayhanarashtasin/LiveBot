"""Binance exchange filters and decimal rounding logic."""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from typing import Any, Dict, Optional, Tuple
import json
import logging
import re
import urllib.request
import urllib.error

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class SymbolFilters:
    """Exchange rules and precision limits for a trading symbol."""

    symbol: str
    tick_size: Decimal
    step_size: Decimal
    min_qty: Decimal
    max_qty: Decimal
    min_notional: Decimal
    max_notional: Optional[Decimal] = None
    price_precision: int = 2
    qty_precision: int = 3

    @classmethod
    def from_exchange_info(cls, symbol_info: Dict[str, Any]) -> SymbolFilters:
        """Parse Binance symbol information dictionary. Fail closed if critical filters missing."""
        symbol = str(symbol_info.get("symbol", "UNKNOWN")).upper()
        price_precision = int(symbol_info.get("pricePrecision", 2))
        qty_precision = int(symbol_info.get("quantityPrecision", 3))

        # Extract required filters, track if all were found
        filters_by_type = {f.get("filterType"): f for f in symbol_info.get("filters", [])}

        # PRICE_FILTER is mandatory
        price_f = filters_by_type.get("PRICE_FILTER")
        if not price_f or "tickSize" not in price_f:
            raise ValueError(f"Missing or malformed PRICE_FILTER for {symbol}")
        tick_size = Decimal(str(price_f["tickSize"]))

        # LOT_SIZE is mandatory
        lot_f = filters_by_type.get("LOT_SIZE")
        if not lot_f:
            raise ValueError(f"Missing LOT_SIZE filter for {symbol}")
        if "stepSize" not in lot_f or "minQty" not in lot_f or "maxQty" not in lot_f:
            raise ValueError(f"LOT_SIZE filter incomplete for {symbol}: missing stepSize/minQty/maxQty")

        step_size = Decimal(str(lot_f["stepSize"]))
        min_qty = Decimal(str(lot_f["minQty"]))
        max_qty = Decimal(str(lot_f["maxQty"]))

        # MIN_NOTIONAL is mandatory
        notional_f = filters_by_type.get("MIN_NOTIONAL") or filters_by_type.get("NOTIONAL")
        if not notional_f:
            raise ValueError(f"Missing MIN_NOTIONAL filter for {symbol}")
        min_notional_key = "minNotional" if "minNotional" in notional_f else "notional"
        if min_notional_key not in notional_f:
            raise ValueError(f"MIN_NOTIONAL filter lacks {min_notional_key} for {symbol}")
        min_notional = Decimal(str(notional_f[min_notional_key]))

        # Validate no zero/invalid values
        if tick_size <= Decimal("0") or step_size <= Decimal("0") or min_qty < Decimal("0") or min_notional < Decimal("0"):
            raise ValueError(f"Invalid filter values for {symbol}: tick={tick_size}, step={step_size}, minQty={min_qty}, minNotional={min_notional}")

        return cls(
            symbol=symbol,
            tick_size=tick_size,
            step_size=step_size,
            min_qty=min_qty,
            max_qty=max_qty,
            min_notional=min_notional,
            max_notional=None,
            price_precision=price_precision,
            qty_precision=qty_precision,
        )

    def round_price(self, price: Decimal) -> Decimal:
        """Rounds price down to nearest valid tick size."""
        if self.tick_size <= Decimal("0"):
            return price
        # Round to tick size multiples
        num_ticks = (price / self.tick_size).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
        return num_ticks * self.tick_size

    def round_quantity(self, quantity: Decimal) -> Decimal:
        """Rounds quantity down to nearest valid step size to prevent exceeding allocation."""
        if self.step_size <= Decimal("0"):
            return quantity
        num_steps = (quantity / self.step_size).quantize(Decimal("1"), rounding=ROUND_DOWN)
        return num_steps * self.step_size

    def validate_order(
        self, quantity: Decimal, price: Decimal
    ) -> Tuple[bool, Optional[str]]:
        """Validates rounded quantity and notional against exchange filters."""
        if quantity < self.min_qty:
            return False, f"Quantity {quantity} is below minQty {self.min_qty}"
        if quantity > self.max_qty:
            return False, f"Quantity {quantity} exceeds maxQty {self.max_qty}"

        notional = quantity * price
        if notional < self.min_notional:
            return False, f"Notional {notional} is below minNotional {self.min_notional}"
        if self.max_notional is not None and notional > self.max_notional:
            return False, f"Notional {notional} exceeds maxNotional {self.max_notional}"

        return True, None


def validate_symbol_for_path(symbol: str) -> bool:
    """Validates symbol is safe for use in file paths and URLs."""
    if not symbol:
        return False
    if len(symbol) > 20:
        return False
    if not re.match(r"^[A-Z0-9]+$", symbol):
        return False
    return True


def fetch_public_exchange_info(symbol: str, timeout: int = 30, base_url: Optional[str] = None) -> Optional[SymbolFilters]:
    """Fetches public Binance USD-M Futures exchange info without authentication.

    Returns SymbolFilters for the symbol or None if not found/failed.
    Fails closed on any error.
    """
    if not validate_symbol_for_path(symbol):
        logger.error(f"Invalid symbol format for exchange info: {symbol}")
        return None

    import time
    try:
        import ssl
        import certifi
        ssl_ctx = ssl.create_default_context(cafile=certifi.where())
    except Exception:
        ssl_ctx = None

    candidate_hosts = []
    if base_url:
        candidate_hosts.append(base_url.rstrip("/"))
    if "https://fapi.binance.com" not in candidate_hosts:
        candidate_hosts.append("https://fapi.binance.com")
    if "https://testnet.binancefuture.com" not in candidate_hosts:
        candidate_hosts.append("https://testnet.binancefuture.com")

    for host in candidate_hosts:
        for attempt in range(1, 4):
            try:
                url = f"{host}/fapi/v1/exchangeInfo?symbol={symbol}"
                req = urllib.request.Request(
                    url,
                    method="GET",
                    headers={"User-Agent": "Escanor-LiveBot/1.0"},
                )

                urlopen_kwargs = {"timeout": timeout}
                if ssl_ctx is not None:
                    urlopen_kwargs["context"] = ssl_ctx

                with urllib.request.urlopen(req, **urlopen_kwargs) as resp:
                    data = json.loads(resp.read().decode("utf-8"))

                symbols = data.get("symbols", [])
                for sym_info in symbols:
                    if sym_info.get("symbol") == symbol and sym_info.get("status") == "TRADING":
                        # Check if perpetual USD-M futures
                        if sym_info.get("contractType") == "PERPETUAL":
                            return SymbolFilters.from_exchange_info(sym_info)

                logger.error(f"Symbol {symbol} not found or not tradable in USD-M Futures on {host}")
                break

            except urllib.error.HTTPError as e:
                if e.code == 451:
                    logger.warning(f"Exchange info on {host} returned 451 (restricted location). Trying fallback host...")
                    break
                if attempt < 3:
                    backoff = 2.0 ** (attempt - 1)
                    logger.warning(f"Network glitch fetching exchange info for {symbol} on {host} (attempt {attempt}/3): {e}. Retrying in {backoff}s...")
                    time.sleep(backoff)
                    continue
                logger.error(f"Network error fetching exchange info for {symbol} on {host}: {e}")
                break
            except (urllib.error.URLError, TimeoutError) as e:
                if attempt < 3:
                    backoff = 2.0 ** (attempt - 1)
                    logger.warning(f"Network glitch fetching exchange info for {symbol} on {host} (attempt {attempt}/3): {e}. Retrying in {backoff}s...")
                    time.sleep(backoff)
                    continue
                logger.error(f"Network error fetching exchange info for {symbol} on {host}: {e}")
                break
            except json.JSONDecodeError as e:
                logger.error(f"Malformed response fetching exchange info for {symbol} on {host}: {e}")
                break
            except Exception as e:
                logger.error(f"Failed to fetch exchange info for {symbol} on {host}: {e}")
                break
    return None
