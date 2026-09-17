"""Persistent logical lots for approved concurrent-slot strategies."""
from __future__ import annotations

from dataclasses import asdict, dataclass
from decimal import Decimal
from typing import Any, Dict, List, Optional

from live_engine.execution.models import SignalEvent


@dataclass
class StrategySlot:
    slot_id: str
    signal_id: str
    entry_signal_open_time: int
    entry_open_time: int
    quantity: Decimal
    entry_price: Decimal
    atr: Decimal
    entry_fee_remaining: Decimal
    stop_price: Decimal
    take_profit_price: Decimal
    expires_at_open_time: int
    state: str = "OPEN"

    def to_dict(self) -> Dict[str, Any]:
        return {
            **asdict(self),
            "quantity": str(self.quantity),
            "entry_price": str(self.entry_price),
            "atr": str(self.atr),
            "entry_fee_remaining": str(self.entry_fee_remaining),
            "stop_price": str(self.stop_price),
            "take_profit_price": str(self.take_profit_price),
        }

    @classmethod
    def from_dict(cls, value: Dict[str, Any]) -> "StrategySlot":
        return cls(
            slot_id=str(value["slot_id"]),
            signal_id=str(value["signal_id"]),
            entry_signal_open_time=int(value["entry_signal_open_time"]),
            entry_open_time=int(value["entry_open_time"]),
            quantity=Decimal(str(value["quantity"])),
            entry_price=Decimal(str(value["entry_price"])),
            atr=Decimal(str(value["atr"])),
            entry_fee_remaining=Decimal(str(value.get("entry_fee_remaining", "0"))),
            stop_price=Decimal(str(value["stop_price"])),
            take_profit_price=Decimal(str(value["take_profit_price"])),
            expires_at_open_time=int(value["expires_at_open_time"]),
            state=str(value.get("state", "OPEN")),
        )


class SlotBook:
    """Tracks independent lots while Binance holds their aggregate net position."""

    def __init__(self, event_store, manifest: Dict[str, Any], initial_equity: Decimal):
        risk = manifest.get("risk", {}) or {}
        execution = manifest.get("execution_model", {}) or {}
        self.event_store = event_store
        self.benchmark_id = str(manifest.get("benchmark_id", "UNKNOWN"))
        self.state_key = f"slot_book:{self.benchmark_id}"
        self.limit = int(risk.get("slots", 1))
        self.leverage = Decimal(str(risk.get("leverage", 1)))
        self.timeframe_ms = int(execution["strategy_timeframe_ms"])
        self.max_hold = int(execution["time_exit_bars"])
        self.stop_multiplier = Decimal(str(execution["stop_atr_multiplier"]))
        self.take_profit_multiplier = Decimal(str(execution["take_profit_atr_multiplier"]))
        self.realized_equity = Decimal(str(initial_equity))
        self.slots: List[StrategySlot] = []
        self._restore()

    @property
    def open_count(self) -> int:
        return len(self.slots)

    @property
    def can_open(self) -> bool:
        return self.open_count < self.limit

    @property
    def open_quantity(self) -> Decimal:
        return sum((slot.quantity for slot in self.slots), Decimal("0"))

    def has_slot(self, slot_id: str) -> bool:
        return any(slot.slot_id == slot_id for slot in self.slots)

    def sync_equity(self, equity: Decimal) -> None:
        """Syncs realized equity from authoritative account balance when flat."""
        if self.open_count == 0 and equity > Decimal("0"):
            self.realized_equity = Decimal(str(equity))
            self._save()

    def entry_notional(self, taker_fee: Decimal = Decimal("0"), current_equity: Optional[Decimal] = None) -> Decimal:
        equity = current_equity if (current_equity is not None and current_equity > Decimal("0")) else self.realized_equity
        gross = max(Decimal("100"), equity) * self.leverage / self.limit
        return gross / (Decimal("1") + Decimal(str(taker_fee)) * self.leverage)

    def add_fill(
        self,
        signal: SignalEvent,
        quantity: Decimal,
        entry_price: Decimal,
        entry_open_time: int,
        entry_fee: Decimal,
    ) -> StrategySlot:
        if not self.can_open:
            raise RuntimeError(f"SLOT BOOK FULL: {self.open_count}/{self.limit}")
        if any(slot.signal_id == signal.signal_id for slot in self.slots):
            raise RuntimeError(f"Duplicate slot entry for {signal.signal_id}")
        atr_value = (signal.indicator_snapshot or {}).get("atr")
        if atr_value is None or Decimal(str(atr_value)) <= 0:
            raise RuntimeError(f"HYPE entry {signal.signal_id} has no valid ATR snapshot")
        atr = Decimal(str(atr_value))
        slot = StrategySlot(
            slot_id=signal.signal_id,
            signal_id=signal.signal_id,
            entry_signal_open_time=signal.candle_open_time,
            entry_open_time=entry_open_time,
            quantity=Decimal(str(quantity)),
            entry_price=Decimal(str(entry_price)),
            atr=atr,
            entry_fee_remaining=Decimal(str(entry_fee)),
            stop_price=Decimal(str(entry_price)) - self.stop_multiplier * atr,
            take_profit_price=Decimal(str(entry_price)) + self.take_profit_multiplier * atr,
            expires_at_open_time=entry_open_time + (self.max_hold - 1) * self.timeframe_ms,
        )
        self.slots.append(slot)
        self._save()
        self._record("STRATEGY_SLOT_OPENED", slot.to_dict())
        return slot

    def barrier_hits(self, price: Decimal) -> List[tuple[StrategySlot, str]]:
        price = Decimal(str(price))
        hits = []
        for slot in self.slots:
            if slot.state != "OPEN":
                continue
            if price <= slot.stop_price:
                hits.append((slot, "SL"))
            elif price >= slot.take_profit_price:
                hits.append((slot, "TP"))
        return hits

    def time_hits(self, candle_open_time: int) -> List[StrategySlot]:
        return [
            slot for slot in self.slots
            if slot.state == "OPEN" and candle_open_time >= slot.expires_at_open_time
        ]

    def mark_exit_pending(self, slot_id: str) -> None:
        self._slot(slot_id).state = "EXIT_PENDING"
        self._save()

    def reopen(self, slot_id: str) -> None:
        self._slot(slot_id).state = "OPEN"
        self._save()

    def close_fill(
        self, slot_id: str, quantity: Decimal, exit_price: Decimal, exit_fee: Decimal, reason: str
    ) -> Decimal:
        slot = self._slot(slot_id)
        quantity = min(Decimal(str(quantity)), slot.quantity)
        if quantity <= 0:
            slot.state = "OPEN"
            self._save()
            return Decimal("0")
        fee_share = slot.entry_fee_remaining * quantity / slot.quantity
        net = (Decimal(str(exit_price)) - slot.entry_price) * quantity - fee_share - Decimal(str(exit_fee))
        slot.quantity -= quantity
        slot.entry_fee_remaining -= fee_share
        self.realized_equity += net
        payload = {
            **slot.to_dict(), "exit_quantity": str(quantity), "exit_price": str(exit_price),
            "exit_fee": str(exit_fee), "reason": reason, "net_pnl": str(net),
            "realized_equity": str(self.realized_equity),
        }
        if slot.quantity == 0:
            self.slots.remove(slot)
        else:
            slot.state = "OPEN"
        self._save()
        self._record("STRATEGY_SLOT_CLOSED", payload)
        return net

    def reconcile_quantity(self, actual_quantity: Decimal, tolerance: Decimal = Decimal("0")) -> Optional[str]:
        difference = abs(self.open_quantity - Decimal(str(actual_quantity)))
        if difference <= tolerance:
            return None
        return (
            f"Logical slot quantity {self.open_quantity} does not match aggregate position "
            f"{actual_quantity} (difference {difference})"
        )

    def describe(self) -> Dict[str, Any]:
        return {
            "benchmark_id": self.benchmark_id,
            "open_slots": self.open_count,
            "slot_limit": self.limit,
            "open_quantity": str(self.open_quantity),
            "realized_equity": str(self.realized_equity),
        }

    def _slot(self, slot_id: str) -> StrategySlot:
        try:
            return next(slot for slot in self.slots if slot.slot_id == slot_id)
        except StopIteration as exc:
            raise KeyError(f"Unknown strategy slot {slot_id}") from exc

    def _restore(self) -> None:
        saved = self.event_store.get_state(self.state_key) if self.event_store else None
        if not saved:
            return
        self.realized_equity = Decimal(str(saved["realized_equity"]))
        self.slots = [StrategySlot.from_dict(value) for value in saved.get("slots", [])]
        if len(self.slots) > self.limit:
            raise RuntimeError(f"Persisted slot book exceeds limit: {len(self.slots)}/{self.limit}")

    def _save(self) -> None:
        if self.event_store:
            self.event_store.set_state(self.state_key, {
                "realized_equity": str(self.realized_equity),
                "slots": [slot.to_dict() for slot in self.slots],
            })

    def _record(self, event_type: str, payload: Dict[str, Any]) -> None:
        if self.event_store:
            self.event_store.log_event(event_type, payload)
