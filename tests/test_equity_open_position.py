"""Equity must survive a round trip through an open position.

The paper wallet posts margin on entry and releases it on close, so reading equity
mid-position used to drop the whole posted margin - which silently rebased the
start-of-day snapshot and disarmed the daily-loss breaker for the rest of the day.
"""
from decimal import Decimal

from live_engine.execution.broker import PaperBroker
from live_engine.execution.models import OrderSide, OrderType
from live_engine.risk.equity import EquityTracker


def test_equity_is_unchanged_by_opening_a_position():
    broker = PaperBroker(initial_balance=Decimal("10000.0"), leverage=1)
    pm = broker._get_pm("ZECUSDT")
    tracker = EquityTracker(event_store=None, broker=broker, position_manager=pm,
                            max_daily_drawdown_usd=Decimal("2000.0"))

    flat_equity = tracker._read_authoritative_equity(0, {})
    assert flat_equity == Decimal("10000.0")

    broker.place_order("ZECUSDT", OrderSide.BUY, OrderType.MARKET, Decimal("8.0"),
                       price=Decimal("1000.0"))
    assert flat_equity - broker.balance > Decimal("7900"), "margin should leave the wallet on entry"

    # Marked at the entry price, equity is the pre-trade equity less fees. Nothing else moved.
    tracker.update_mark_price(Decimal(str(pm.entry_price)))
    held_equity = tracker._read_authoritative_equity(0, {})
    assert abs(held_equity - flat_equity) < Decimal("10"), (
        f"equity moved {flat_equity - held_equity} on a flat-to-open transition; "
        "posted margin is missing from the equity reading"
    )

    # A 10% adverse move is worth exactly its mark-to-market, not the whole position.
    tracker.update_mark_price(pm.entry_price * Decimal("0.9"))
    assert abs((held_equity - tracker._read_authoritative_equity(0, {}))
               - pm.entry_price * Decimal("8.0") * Decimal("0.1")) < Decimal("1")


if __name__ == "__main__":
    test_equity_is_unchanged_by_opening_a_position()
    print("OK")
