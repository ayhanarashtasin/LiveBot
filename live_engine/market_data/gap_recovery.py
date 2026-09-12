"""Exact aggTrade gap recovery.

A sequence gap means Escanor's reconstructed candles are missing real trades. The old
implementation declared a gap "recovered" as soon as a page came back empty or short,
which silently accepted permanently wrong candles.

Recovery here is exact: a gap closes only when *every* aggregate trade ID in
``[from_id, to_id]`` has been fetched and validated. Anything less leaves the runtime
DATA_DESYNCED with the precise remaining interval recorded.

Everything in this module is synchronous and network-bound on purpose: the caller runs
it off the event loop (``asyncio.to_thread``) so a slow recovery cannot starve websocket
heartbeat processing.
"""
from __future__ import annotations

import json
import logging
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Dict, List, Optional, Tuple

from live_engine.market_data.models import AggTrade

logger = logging.getLogger(__name__)

# Binance documents these as retryable; everything else is a definite answer.
RETRYABLE_HTTP_CODES = (429, 418, 502, 504)
MAX_ATTEMPTS = 5
BACKOFF_BASE_S = 1.0
BACKOFF_MAX_S = 60.0
PAGE_LIMIT = 1000
# Bound on total pages for one gap, so a pathological range cannot spin forever.
MAX_PAGES = 500


class RateLimitedError(RuntimeError):
    """Raised when the exchange kept rate-limiting past the bounded retry policy."""


@dataclass
class RecoveryResult:
    """Outcome of one gap-recovery attempt."""

    from_id: int
    to_id: int
    trades: List[AggTrade] = field(default_factory=list)
    complete: bool = False
    remaining: Optional[Tuple[int, int]] = None
    reason: str = ""
    rejected: Dict[str, int] = field(default_factory=dict)
    pages: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "from_id": self.from_id,
            "to_id": self.to_id,
            "recovered_count": len(self.trades),
            "complete": self.complete,
            "remaining": list(self.remaining) if self.remaining else None,
            "reason": self.reason,
            "rejected": self.rejected,
            "pages": self.pages,
        }


def retry_delay(error: Optional[BaseException], attempt: int) -> float:
    """Retry-After when the server supplies one, else bounded exponential backoff."""
    header = None
    headers = getattr(error, "headers", None)
    if headers is not None:
        try:
            header = headers.get("Retry-After")
        except Exception:
            header = None
    if header:
        try:
            return min(BACKOFF_MAX_S, max(0.0, float(header)))
        except (TypeError, ValueError):
            pass
    return min(BACKOFF_MAX_S, BACKOFF_BASE_S * (2 ** attempt))


class AggTradeGapRecovery:
    """Fetches and validates every missing aggregate trade in a gap."""

    def __init__(
        self,
        symbol: str,
        base_url: str = "https://fapi.binance.com",
        fetch_page: Optional[Callable[[int, int], Any]] = None,
        sleep: Callable[[float], None] = time.sleep,
        max_attempts: int = MAX_ATTEMPTS,
    ):
        self.symbol = symbol.upper().replace("/", "").replace(":", "")
        self.base_url = base_url.rstrip("/")
        self._fetch_page = fetch_page or self._http_fetch_page
        self._sleep = sleep
        self.max_attempts = max_attempts

    # -- network ------------------------------------------------------------

    def _http_fetch_page(self, from_id: int, limit: int) -> Any:
        params = urllib.parse.urlencode({"symbol": self.symbol, "fromId": from_id, "limit": limit})
        req = urllib.request.Request(
            f"{self.base_url}/fapi/v1/aggTrades?{params}",
            headers={"User-Agent": "Escanor-LiveBot/1.0"},
            method="GET",
        )
        with urllib.request.urlopen(req, timeout=20) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def fetch_page(self, from_id: int, limit: int) -> Any:
        """One page with bounded retry on documented rate-limit and gateway errors."""
        last_error: Optional[BaseException] = None
        for attempt in range(self.max_attempts):
            try:
                return self._fetch_page(from_id, limit)
            except urllib.error.HTTPError as exc:
                last_error = exc
                if exc.code not in RETRYABLE_HTTP_CODES:
                    raise
                if attempt == self.max_attempts - 1:
                    break
                delay = retry_delay(exc, attempt)
                logger.warning(
                    "aggTrades recovery got HTTP %s at fromId=%s; retrying in %.1fs (%s/%s)",
                    exc.code, from_id, delay, attempt + 1, self.max_attempts,
                )
                self._sleep(delay)
            except (urllib.error.URLError, TimeoutError, OSError, ValueError) as exc:
                last_error = exc
                if attempt == self.max_attempts - 1:
                    break
                self._sleep(retry_delay(exc, attempt))
        raise RateLimitedError(
            f"aggTrades page fromId={from_id} unavailable after {self.max_attempts} attempts: {last_error}"
        )

    # -- validation ---------------------------------------------------------

    def parse_page(self, payload: Any, from_id: int, to_id: int) -> Tuple[Dict[int, AggTrade], Dict[str, int]]:
        """Validates one page, returning accepted trades by ID plus rejection counts.

        Rejects malformed rows, wrong symbols, duplicates and IDs outside the requested
        range. Out-of-order rows are accepted but re-sorted by the caller: the exchange
        does not guarantee page ordering, but a hole is still a hole.
        """
        accepted: Dict[int, AggTrade] = {}
        rejected = {"malformed": 0, "wrong_symbol": 0, "out_of_range": 0, "duplicate": 0}

        if not isinstance(payload, list):
            rejected["malformed"] += 1
            return accepted, rejected

        now_ms = int(time.time() * 1000)
        for row in payload:
            if not isinstance(row, dict):
                rejected["malformed"] += 1
                continue
            row_symbol = str(row.get("s", self.symbol)).upper()
            if row_symbol != self.symbol:
                rejected["wrong_symbol"] += 1
                continue
            try:
                agg_id = int(row["a"])
                price = Decimal(str(row["p"]))
                qty = Decimal(str(row["q"]))
                trade_time = int(row["T"])
            except (KeyError, TypeError, ValueError, InvalidOperation):
                rejected["malformed"] += 1
                continue
            if price <= 0 or qty <= 0 or trade_time <= 0:
                rejected["malformed"] += 1
                continue
            if agg_id < from_id or agg_id > to_id:
                rejected["out_of_range"] += 1
                continue
            if agg_id in accepted:
                rejected["duplicate"] += 1
                continue

            accepted[agg_id] = AggTrade(
                event_type="aggTrade",
                event_time=int(row.get("E", trade_time)),
                symbol=self.symbol,
                agg_trade_id=agg_id,
                price=price,
                quantity=qty,
                first_trade_id=int(row.get("f", agg_id)),
                last_trade_id=int(row.get("l", agg_id)),
                trade_time=trade_time,
                buyer_is_market_maker=bool(row.get("m", False)),
                received_at=now_ms,
            )
        return accepted, rejected

    # -- recovery -----------------------------------------------------------

    def recover(self, from_id: int, to_id: int, already_have: Optional[set] = None) -> RecoveryResult:
        """Recovers ``[from_id, to_id]`` exactly, or reports precisely what is still missing.

        ``already_have`` lets a repeated request for the same range skip trades that are
        durably stored, which is what makes recovery idempotent.
        """
        result = RecoveryResult(from_id=from_id, to_id=to_id)
        if to_id < from_id:
            result.complete = True
            result.reason = "empty range"
            return result

        collected: Dict[int, AggTrade] = {}
        have = set(already_have or ())
        rejected_total = {"malformed": 0, "wrong_symbol": 0, "out_of_range": 0, "duplicate": 0}
        cursor = from_id

        while cursor <= to_id and result.pages < MAX_PAGES:
            if cursor in have and cursor not in collected:
                # Already durably stored: advance without re-requesting.
                cursor += 1
                continue

            limit = min(to_id - cursor + 1, PAGE_LIMIT)
            try:
                payload = self.fetch_page(cursor, limit)
            except RateLimitedError as exc:
                result.reason = str(exc)
                break
            except Exception as exc:
                result.reason = f"fetch failed at fromId={cursor}: {exc}"
                break
            result.pages += 1

            accepted, rejected = self.parse_page(payload, from_id, to_id)
            for key in rejected_total:
                rejected_total[key] += rejected[key]

            if not accepted:
                # An empty or entirely invalid page proves nothing was recovered here.
                # The old code treated this as success; it is a hard stop.
                result.reason = result.reason or f"empty or invalid page at fromId={cursor}"
                break

            collected.update(accepted)
            # Advance from the highest *contiguous* ID, not the largest returned one:
            # a page with a hole in the middle must be requested again.
            next_cursor = cursor
            while next_cursor in collected or next_cursor in have:
                next_cursor += 1
            if next_cursor <= cursor:
                result.reason = f"no forward progress at fromId={cursor}"
                break
            cursor = next_cursor

        result.rejected = rejected_total
        result.trades = [collected[i] for i in sorted(collected)]

        expected = set(range(from_id, to_id + 1))
        missing = sorted(expected - set(collected) - have)
        if not missing:
            result.complete = True
            result.remaining = None
            result.reason = result.reason or "exact recovery"
        else:
            result.complete = False
            result.remaining = (missing[0], missing[-1])
            if not result.reason:
                result.reason = f"{len(missing)} aggregate trade ID(s) still missing"
            if result.pages >= MAX_PAGES:
                result.reason += f" (page limit {MAX_PAGES} reached)"
        return result
