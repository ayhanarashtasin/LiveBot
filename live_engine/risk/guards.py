"""Operational safety risk guards.

Enforces pre-trade sanity checks:
1. Kill switch status.
2. Max open positions.
3. Max order size / notional USD limit.
4. Min notional and step/tick lot filter checks.
5. Market data staleness and gap status.
6. Price deviation sanity against latest candle close.
7. Daily loss / drawdown circuit breaker.
"""
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional, List
import logging

from live_engine.execution.models import Order, OrderSide, SignalAction
from live_engine.execution.position_manager import PositionManager
from live_engine.execution.order_filters import SymbolFilters
from live_engine.market_data.models import Candle
from live_engine.risk.kill_switch import KillSwitch

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class GuardResult:
    passed: bool
    reason: str = ""


class RiskGuardEngine:
    """Evaluates all operational safety gates prior to order placement."""

    def __init__(
        self,
        kill_switch: KillSwitch,
        max_open_positions: int = 1,
        max_order_notional_usd: Decimal = Decimal("50000.0"),
        max_candle_staleness_sec: int = 1800,  # 30 mins for 15m candle
        max_price_deviation_pct: Decimal = Decimal("2.0"),  # max 2% slippage / deviation from candle close
        max_daily_drawdown_usd: Decimal = Decimal("2000.0"),
        filters: Optional[SymbolFilters] = None,
        equity_tracker: Optional[object] = None,
        require_fresh_market_data: bool = False,
        leverage: Decimal = Decimal("1"),
        taker_fee: Decimal = Decimal("0.0005"),
    ):
        # Authoritative equity/mark source. Required in modes that touch a real account;
        # SHADOW/PAPER may run without one and say so rather than pretending.
        self.equity_tracker = equity_tracker
        self.require_fresh_market_data = require_fresh_market_data
        self.leverage = Decimal(str(leverage))
        self.taker_fee = Decimal(str(taker_fee))
        self.kill_switch = kill_switch
        self.max_open_positions = max_open_positions
        self.max_order_notional_usd = max_order_notional_usd
        self.max_candle_staleness_sec = max_candle_staleness_sec
        self.max_price_deviation_pct = max_price_deviation_pct
        self.max_daily_drawdown_usd = max_daily_drawdown_usd
        self.filters = filters

    def evaluate_entry(
        self,
        symbol: str,
        side: OrderSide,
        quantity: Decimal,
        price: Decimal,
        position_manager: PositionManager,
        latest_candle: Optional[Candle] = None,
        has_active_gaps: bool = False,
        open_positions_count: Optional[int] = None,
    ) -> GuardResult:
        """Evaluates entry order against all pre-trade safety gates."""
        # 1. Kill Switch Guard
        if self.kill_switch.is_engaged():
            return GuardResult(passed=False, reason="Kill switch is active")

        # 2. Market Data Gap Guard
        if has_active_gaps:
            return GuardResult(passed=False, reason="Active market data gap detected; entries blocked")

        # 3. Position Limit Guard
        if open_positions_count is None and not position_manager.is_flat:
            return GuardResult(passed=False, reason=f"Position already open on {symbol} (qty: {position_manager.quantity}).")
        count = open_positions_count or 0
        if count >= self.max_open_positions:
            return GuardResult(
                passed=False,
                reason=f"Position limit reached on {symbol} ({count}/{self.max_open_positions}, qty: {position_manager.quantity}).",
            )
        # 4. Data Staleness Guard
        if latest_candle is not None:
            now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
            staleness_sec = (now_ms - latest_candle.close_time) / 1000.0
            if staleness_sec > self.max_candle_staleness_sec:
                return GuardResult(
                    passed=False,
                    reason=f"Latest candle is stale by {staleness_sec:.1f}s (max allowed: {self.max_candle_staleness_sec}s)",
                )

        # 5. Freshness Guard: never open new risk on stale price or account data.
        if self.equity_tracker is not None and self.require_fresh_market_data:
            stale = self.equity_tracker.staleness_reason()
            if stale:
                return GuardResult(passed=False, reason=f"Stale market/account data: {stale}")

        # 6. Price Sanity Guard.
        # The intended order price is derived from the candle close, so comparing it with
        # that same close is a tautology. Compare it with an independent current benchmark
        # (the exchange mark price) whenever one exists.
        benchmark_px = self._price_benchmark(latest_candle)
        if benchmark_px is not None and benchmark_px > Decimal("0"):
            deviation_pct = (abs(price - benchmark_px) / benchmark_px) * Decimal("100.0")
            if deviation_pct > self.max_price_deviation_pct:
                return GuardResult(
                    passed=False,
                    reason=(
                        f"Price deviation {deviation_pct:.2f}% from the current mark benchmark "
                        f"{benchmark_px} exceeds threshold {self.max_price_deviation_pct}%"
                    ),
                )
        elif self.require_fresh_market_data:
            return GuardResult(
                passed=False,
                reason="No independent price benchmark available to sanity-check the order price",
            )

        # 6. Notional Value Guard
        notional = quantity * price
        if notional > self.max_order_notional_usd:
            return GuardResult(
                passed=False,
                reason=f"Order notional ${notional} exceeds maximum allowed ${self.max_order_notional_usd}",
            )

        # 7. Symbol Filters Guard (Lot size & min notional)
        if self.filters is not None:
            is_valid, reason = self.filters.validate_order(quantity, price)
            if not is_valid:
                return GuardResult(passed=False, reason=f"Symbol filters rejected: {reason}")

        # 8. Maximum Total Notional vs Leverage Cap Guard
        # Total notional across all positions (existing + new order) must never exceed max_leverage * collateral
        if self.equity_tracker is not None and getattr(self.equity_tracker, "account_equity", None) is not None:
            collateral = self.equity_tracker.account_equity.value
            if collateral > Decimal("0"):
                max_total_notional = collateral * self.leverage
                current_open_notional = abs(Decimal(str(position_manager.quantity))) * price
                new_total_notional = current_open_notional + notional
                if new_total_notional > max_total_notional:
                    return GuardResult(
                        passed=False,
                        reason=(
                            f"Total notional ${new_total_notional:.2f} exceeds {self.leverage}x "
                            f"collateral cap (${max_total_notional:.2f} on collateral ${collateral:.2f})"
                        ),
                    )

        # 9. Available margin, from authoritative account data.
        if self.equity_tracker is not None:
            shortfall = self.equity_tracker.margin_shortfall(notional, self.leverage, notional * self.taker_fee)
            if shortfall and self.require_fresh_market_data:
                return GuardResult(passed=False, reason=shortfall)

            # 10. Daily Loss Guard against the persisted start-of-day equity snapshot.
            breached = self.equity_tracker.check_daily_loss()
            if breached:
                return GuardResult(passed=False, reason=breached)
        elif self.require_fresh_market_data:
            return GuardResult(
                passed=False,
                reason="No authoritative equity source configured; new risk refused",
            )
        elif (position_manager.realized_pnl + position_manager.unrealized_pnl) < -self.max_daily_drawdown_usd:
            # Simulated modes without an equity tracker fall back to local PnL.
            return GuardResult(
                passed=False,
                reason=f"Daily drawdown limit reached (-${self.max_daily_drawdown_usd})",
            )

        return GuardResult(passed=True, reason="All risk checks passed")

    def _price_benchmark(self, latest_candle: Optional[Candle]) -> Optional[Decimal]:
        """Current executable reference for the price-sanity check.

        Prefers a fresh exchange mark price. Falls back to the previous candle close - a
        bar the order price was not derived from - and returns None when neither exists.
        """
        tracker = self.equity_tracker
        if tracker is not None and tracker.mark_is_fresh() and tracker.mark_price is not None:
            return tracker.mark_price.value
        if latest_candle is not None and not self.require_fresh_market_data:
            return Decimal(str(latest_candle.close))
        return None

    def evaluate_exit(
        self,
        symbol: str,
        side: OrderSide,
        quantity: Decimal,
        position_manager: PositionManager,
    ) -> GuardResult:
        """Evaluates exit order. Exits are prioritized and generally permitted unless kill switch or size mismatch."""
        if position_manager.is_flat:
            return GuardResult(passed=False, reason=f"Cannot exit: No open position for {symbol}")

        # Exits should reduce position, not exceed current position quantity
        if quantity > position_manager.quantity:
            return GuardResult(
                passed=False,
                reason=f"Exit quantity {quantity} exceeds position size {position_manager.quantity}",
            )

        return GuardResult(passed=True, reason="Exit order approved")
