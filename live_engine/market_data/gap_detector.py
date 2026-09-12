"""Gap and sequence anomaly detector for Binance aggregate trades."""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import List, Optional, Tuple

from live_engine.market_data.models import AggTrade


class TradeSequenceStatus(str, Enum):
    VALID = "VALID"
    DUPLICATE = "DUPLICATE"
    GAP = "GAP"
    REVERSED = "REVERSED"


@dataclass
class GapRecord:
    from_id: int
    to_id: int
    missing_count: int
    detected_at: int
    recovered: bool = False
    # Completed recovery attempts. A gap that stays active forever after one failed attempt
    # latches the runtime DATA_DESYNCED permanently, so attempts are counted and bounded.
    attempts: int = 0


class AggTradeGapDetector:
    """Monitors incoming aggTrade stream for strict chronological and sequence integrity."""

    def __init__(self, symbol: str):
        self.symbol = symbol.upper()
        self.last_agg_trade_id: Optional[int] = None
        self.last_trade_time: Optional[int] = None
        self.is_desynced: bool = False

        self.total_events: int = 0
        self.duplicate_count: int = 0
        self.gap_count: int = 0
        self.missing_ids_total: int = 0
        self.active_gaps: List[GapRecord] = []

    def process_trade(self, trade: AggTrade) -> TradeSequenceStatus:
        """Evaluate next trade sequence continuity.

        Returns TradeSequenceStatus.
        """
        self.total_events += 1

        if self.last_agg_trade_id is None:
            self.last_agg_trade_id = trade.agg_trade_id
            self.last_trade_time = trade.trade_time
            self.is_desynced = False
            return TradeSequenceStatus.VALID

        # Check for duplicate
        if trade.agg_trade_id == self.last_agg_trade_id:
            self.duplicate_count += 1
            return TradeSequenceStatus.DUPLICATE

        # Check for reversed ID
        if trade.agg_trade_id < self.last_agg_trade_id:
            return TradeSequenceStatus.REVERSED

        # Check for gap
        gap_size = trade.agg_trade_id - self.last_agg_trade_id - 1
        if gap_size > 0:
            self.gap_count += 1
            self.missing_ids_total += gap_size
            self.is_desynced = True
            gap_rec = GapRecord(
                from_id=self.last_agg_trade_id + 1,
                to_id=trade.agg_trade_id - 1,
                missing_count=gap_size,
                detected_at=trade.received_at,
            )
            self.active_gaps.append(gap_rec)
            self.last_agg_trade_id = trade.agg_trade_id
            self.last_trade_time = trade.trade_time
            return TradeSequenceStatus.GAP

        # Sequential trade (last_id + 1)
        self.last_agg_trade_id = trade.agg_trade_id
        self.last_trade_time = trade.trade_time
        return TradeSequenceStatus.VALID

    def mark_gap_recovered(self, from_id: int, to_id: int) -> None:
        """Mark an active gap as recovered.

        Only call this once every expected aggregate trade ID in the range has been
        fetched and validated; a short or empty REST page is not recovery.
        """
        for gap in self.active_gaps:
            if gap.from_id == from_id and gap.to_id == to_id:
                gap.recovered = True
        self.active_gaps = [g for g in self.active_gaps if not g.recovered]
        if not self.active_gaps:
            self.is_desynced = False

    def narrow_gap(self, from_id: int, to_id: int, remaining: Optional[Tuple[int, int]]) -> None:
        """Shrinks a partially recovered gap to exactly what is still missing.

        The gap stays active and the detector stays desynced, so a partial recovery can
        never be mistaken for a whole one.
        """
        if remaining is None:
            self.mark_gap_recovered(from_id, to_id)
            return
        new_from, new_to = remaining
        for gap in self.active_gaps:
            if gap.from_id == from_id and gap.to_id == to_id:
                gap.from_id = new_from
                gap.to_id = new_to
                gap.missing_count = max(0, new_to - new_from + 1)
        self.is_desynced = bool(self.active_gaps)

    def record_attempt(self, from_id: int, to_id: int) -> int:
        """Counts one completed recovery attempt for a gap, returning its new total."""
        for gap in self.active_gaps:
            if gap.from_id == from_id and gap.to_id == to_id:
                gap.attempts += 1
                return gap.attempts
        return 0

    def abandon_gap(self, from_id: int, to_id: int) -> bool:
        """Drops a gap that recovery has proven it cannot close.

        The missing trades are gone from the exchange's aggTrade endpoint, so holding the
        runtime DATA_DESYNCED forever only guarantees the engine never trades again. The
        caller is responsible for repairing the affected candles from REST klines, which
        are authoritative and unaffected by a trade-stream hole.
        """
        before = len(self.active_gaps)
        self.active_gaps = [
            g for g in self.active_gaps if not (g.from_id == from_id and g.to_id == to_id)
        ]
        self.is_desynced = bool(self.active_gaps)
        return len(self.active_gaps) < before

    def restore_last_accepted_id(self, last_agg_trade_id: Optional[int]) -> None:
        """Seeds the detector from durable state so a restart detects the real gap.

        Without this the first event after a restart is accepted unconditionally and every
        trade missed while the process was down is lost from the reconstructed candles.
        """
        if last_agg_trade_id is not None and int(last_agg_trade_id) > 0:
            self.last_agg_trade_id = int(last_agg_trade_id)

    def reset(self) -> None:
        self.last_agg_trade_id = None
        self.last_trade_time = None
        self.is_desynced = False
        self.active_gaps.clear()
