"""Immutable execution-quality telemetry.

One record per submission attempt, capturing the milestones goal.md requires:

    T0 local submission start
    T1 signed request dispatch
    T2 exchange acknowledgement
    T3 first fill
    T4 terminal fill

Durations come from a monotonic high-resolution clock (a wall clock can step backwards
and produce negative latencies); UTC timestamps are recorded alongside for correlation
with exchange event times.

Records are append-only. Dashboards and reports are projections over them, never the
source. A milestone that was not observed stays ``None``: the report may not claim a
measurement the system did not capture.
"""
from __future__ import annotations

import logging
import time
from decimal import Decimal
from typing import Any, Dict, List, Optional

from live_engine.execution.models import Order, OrderSide, SignalEvent

logger = logging.getLogger(__name__)

MILESTONES = ("T0", "T1", "T2", "T3", "T4")
ZERO = Decimal("0")


def _ms(value: Optional[Decimal]) -> Optional[str]:
    return str(value) if value is not None else None


class ExecutionTelemetry:
    """Builds and persists execution-quality records."""

    def __init__(self, event_store: Optional[Any], benchmark_id: str = "UNKNOWN",
                 mode: str = "SHADOW", strategy_hash: str = "", helper_hash: str = "",
                 arrival_benchmark: Optional[Any] = None):
        self.event_store = event_store
        self.benchmark_id = benchmark_id
        self.mode = mode
        self.strategy_hash = strategy_hash
        self.helper_hash = helper_hash
        # Optional callable returning (bid, ask) at submission time. Absent in SHADOW/PAPER,
        # where no order book is consumed - in which case no spread cost is claimed.
        self.arrival_benchmark = arrival_benchmark

    # -- lifecycle ----------------------------------------------------------

    def begin(
        self,
        signal: SignalEvent,
        requested_qty: Decimal,
        reference_price: Decimal,
        client_order_id: str,
        expected_execution_time_ms: int,
        side: OrderSide,
        reduce_only: bool = False,
        position_side: Optional[str] = None,
        guard_decision: str = "PASSED",
        block_reason: Optional[str] = None,
    ) -> Dict[str, Any]:
        bid, ask = self._arrival_quote()
        record: Dict[str, Any] = {
            "benchmark_id": self.benchmark_id,
            "mode": self.mode,
            "strategy_hash": self.strategy_hash,
            "helper_hash": self.helper_hash,
            "signal_id": signal.signal_id,
            "signal_time_ms": signal.candle_close_time,
            "signal_candle_open_time": signal.candle_open_time,
            "expected_action": signal.action.value,
            "expected_execution_time_ms": expected_execution_time_ms,
            "reference_price": str(reference_price),
            "requested_quantity": str(requested_qty),
            "side": side.value,
            "reduce_only": reduce_only,
            "position_side": position_side,
            "guard_decision": guard_decision,
            "block_reason": block_reason,
            "client_order_id": client_order_id,
            "exchange_order_id": None,
            "arrival_bid": _ms(bid),
            "arrival_ask": _ms(ask),
            "submitted_quantity": None,
            "filled_quantity": None,
            "fill_prices": [],
            "vwap": None,
            "fees": None,
            "fee_asset": None,
            "entry_slippage": None,
            "exit_slippage": None,
            "implementation_shortfall": None,
            "spread_crossing_cost": None,
            "realized_pnl": None,
            "exit_reason": None,
            "monotonic_ns": {},
            "utc_ms": {},
            "latency_ms": {},
        }
        self.mark(record, "T0")
        return record

    def mark(self, record: Dict[str, Any], milestone: str) -> None:
        """Stamps a milestone once. Re-marking is ignored so T3 stays the *first* fill."""
        if milestone not in MILESTONES or milestone in record["monotonic_ns"]:
            return
        record["monotonic_ns"][milestone] = time.monotonic_ns()
        record["utc_ms"][milestone] = int(time.time() * 1000)

    def finish(
        self,
        record: Dict[str, Any],
        order: Optional[Order],
        block_reason: Optional[str] = None,
        realized_pnl: Optional[Decimal] = None,
        exit_reason: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Completes and persists the record."""
        if block_reason:
            record["guard_decision"] = "BLOCKED"
            record["block_reason"] = block_reason

        if order is not None:
            record["exchange_order_id"] = order.exchange_order_id
            record["submitted_quantity"] = str(order.quantity)
            record["filled_quantity"] = str(order.filled_quantity)
            record["fees"] = str(order.accumulated_fees)
            record["reduce_only"] = order.reduce_only
            record["position_side"] = order.position_side or record["position_side"]
            fills = self._fill_prices(order.client_order_id)
            record["fill_prices"] = [str(p) for p, _ in fills]
            record["fee_asset"] = self._fee_asset(order.client_order_id)
            vwap = self._vwap(fills)
            if vwap is None and order.avg_fill_price:
                vwap = order.avg_fill_price
            record["vwap"] = _ms(vwap)
            self._compute_costs(record, order, vwap)

        if realized_pnl is not None:
            record["realized_pnl"] = str(realized_pnl)
        if exit_reason is not None:
            record["exit_reason"] = exit_reason

        record["latency_ms"] = self._latencies(record["monotonic_ns"])

        if self.event_store is not None:
            try:
                self.event_store.record_telemetry(record)
            except Exception as exc:
                logger.warning("Execution telemetry persistence failed: %s", exc)
        return record

    # -- derived measures ---------------------------------------------------

    @staticmethod
    def _latencies(monotonic: Dict[str, int]) -> Dict[str, float]:
        """Milestone-to-milestone durations in ms, only for milestones actually observed."""
        out: Dict[str, float] = {}
        pairs = (("T0", "T1", "submit_dispatch_ms"), ("T1", "T2", "ack_ms"),
                 ("T2", "T3", "first_fill_ms"), ("T3", "T4", "fill_completion_ms"),
                 ("T0", "T4", "total_ms"))
        for a, b, name in pairs:
            if a in monotonic and b in monotonic:
                out[name] = (monotonic[b] - monotonic[a]) / 1_000_000.0
        return out

    def _compute_costs(self, record: Dict[str, Any], order: Order, vwap: Optional[Decimal]) -> None:
        reference = Decimal(record["reference_price"])
        if vwap is None or reference <= ZERO:
            return
        signed = (vwap - reference) if order.side == OrderSide.BUY else (reference - vwap)
        slip_pct = (signed / reference) * Decimal("100")
        key = "entry_slippage" if order.side == OrderSide.BUY else "exit_slippage"
        record[key] = str(slip_pct)

        filled = order.filled_quantity
        if filled > ZERO:
            record["implementation_shortfall"] = str(signed * filled + order.accumulated_fees)

        bid, ask = record.get("arrival_bid"), record.get("arrival_ask")
        if bid and ask:
            mid = (Decimal(bid) + Decimal(ask)) / Decimal("2")
            crossed = (vwap - mid) if order.side == OrderSide.BUY else (mid - vwap)
            record["spread_crossing_cost"] = str(crossed * filled)

    def _arrival_quote(self):
        if self.arrival_benchmark is None:
            return None, None
        try:
            bid, ask = self.arrival_benchmark()
            return Decimal(str(bid)), Decimal(str(ask))
        except Exception:
            return None, None

    def _fill_prices(self, client_order_id: str):
        if self.event_store is None:
            return []
        try:
            rows = self.event_store.get_fills(client_order_id)
        except Exception:
            return []
        out = []
        for r in rows:
            price = r.get("last_price") or r.get("avg_price")
            qty = r.get("last_qty")
            if price and qty and Decimal(str(qty)) > ZERO:
                out.append((Decimal(str(price)), Decimal(str(qty))))
        return out

    def _fee_asset(self, client_order_id: str) -> Optional[str]:
        if self.event_store is None:
            return None
        try:
            rows = self.event_store.get_fills(client_order_id)
        except Exception:
            return None
        for r in reversed(rows):
            if r.get("commission_asset"):
                return r["commission_asset"]
        return None

    @staticmethod
    def _vwap(fills) -> Optional[Decimal]:
        total_qty = sum((q for _, q in fills), ZERO)
        if total_qty <= ZERO:
            return None
        return sum((p * q for p, q in fills), ZERO) / total_qty


def build_execution_comparison(
    telemetry_rows: List[Dict[str, Any]],
    benchmark_trades: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """Compares benchmark expectations with observed SHADOW/PAPER/TESTNET execution.

    The report separates strategy/data divergence (missing or extra signals) from
    execution divergence (slippage, latency, fees). Averages are computed only over
    records that actually captured the measurement.
    """
    benchmark_trades = benchmark_trades or []
    entries = [r for r in telemetry_rows if r.get("side") == "BUY"]
    exits = [r for r in telemetry_rows if r.get("side") == "SELL"]
    blocked = [r for r in telemetry_rows if r.get("guard_decision") == "BLOCKED"]

    def avg(rows, key):
        vals = [Decimal(r[key]) for r in rows if r.get(key) not in (None, "")]
        return str(sum(vals) / len(vals)) if vals else None

    def avg_latency(rows, key):
        vals = [r["latency_ms"][key] for r in rows if key in (r.get("latency_ms") or {})]
        return sum(vals) / len(vals) if vals else None

    observed_signals = {r["signal_id"] for r in telemetry_rows}
    benchmark_signals = {t.get("signal_id") for t in benchmark_trades if t.get("signal_id")}

    return {
        "benchmark_trades": len(benchmark_trades),
        "observed_submissions": len(telemetry_rows),
        "entry_signals": len(entries),
        "exit_signals": len(exits),
        "blocked_submissions": len(blocked),
        "block_reasons": sorted({r.get("block_reason") for r in blocked if r.get("block_reason")}),
        "missing_signals": sorted(benchmark_signals - observed_signals),
        "extra_signals": sorted(observed_signals - benchmark_signals) if benchmark_signals else [],
        "avg_entry_slippage_pct": avg(entries, "entry_slippage"),
        "avg_exit_slippage_pct": avg(exits, "exit_slippage"),
        "avg_implementation_shortfall": avg(telemetry_rows, "implementation_shortfall"),
        "avg_ack_latency_ms": avg_latency(telemetry_rows, "ack_ms"),
        "avg_first_fill_latency_ms": avg_latency(telemetry_rows, "first_fill_ms"),
        "total_fees": str(sum((Decimal(r["fees"]) for r in telemetry_rows if r.get("fees")), ZERO)),
        "divergence_attribution": (
            "STRATEGY/DATA" if benchmark_signals and (benchmark_signals - observed_signals)
            else ("EXECUTION" if telemetry_rows else "NONE")
        ),
        "measurements_not_captured": sorted({
            key for r in telemetry_rows for key in
            ("arrival_bid", "spread_crossing_cost", "vwap") if r.get(key) is None
        }),
    }
