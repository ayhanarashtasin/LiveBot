"""Unit tests for market data models and event structures."""
from decimal import Decimal
import pytest

from live_engine.market_data.models import AggTrade, Candle
from live_engine.execution.models import Order, OrderSide, OrderStatus, OrderType, SignalAction, SignalEvent


def test_aggtrade_parsing_from_binance_ws():
    payload = {
        "e": "aggTrade",
        "E": 1786752000500,
        "s": "BTCUSDT",
        "a": 3408682818,
        "p": "63009.20",
        "q": "0.150",
        "f": 5000000000,
        "l": 5000000002,
        "T": 1786752000003,
        "m": True,
    }
    trade = AggTrade.from_binance_ws(payload, received_at=1786752000600)

    assert trade.event_type == "aggTrade"
    assert trade.symbol == "BTCUSDT"
    assert trade.agg_trade_id == 3408682818
    assert trade.price == Decimal("63009.20")
    assert trade.quantity == Decimal("0.150")
    assert trade.first_trade_id == 5000000000
    assert trade.last_trade_id == 5000000002
    assert trade.trade_time == 1786752000003
    assert trade.buyer_is_market_maker is True
    assert trade.received_at == 1786752000600
    assert trade.latency_ms == 597
    assert trade.trade_datetime_utc.year == 2026


def test_aggtrade_immutability():
    trade = AggTrade(
        event_type="aggTrade",
        event_time=1000,
        symbol="BTCUSDT",
        agg_trade_id=1,
        price=Decimal("50000.00"),
        quantity=Decimal("1.0"),
        first_trade_id=1,
        last_trade_id=1,
        trade_time=1000,
        buyer_is_market_maker=False,
        received_at=1050,
    )
    with pytest.raises((AttributeError, TypeError)):
        trade.price = Decimal("51000.00")  # type: ignore


def test_candle_properties():
    candle = Candle(
        symbol="BTCUSDT",
        timeframe="1m",
        open_time=1786752000000,
        close_time=1786752059999,
        open=Decimal("63000.00"),
        high=Decimal("63050.00"),
        low=Decimal("62990.00"),
        close=Decimal("63040.00"),
        volume=Decimal("12.500"),
        trade_count=45,
        is_closed=True,
    )
    d = candle.to_dict()
    assert d["open"] == 63000.00
    assert d["high"] == 63050.00
    assert d["low"] == 62990.00
    assert d["close"] == 63040.00
    assert d["is_closed"] is True


def test_signal_event_and_order_model():
    signal = SignalEvent(
        signal_id="SIG_001",
        benchmark_id="BTC_ST_09_5M",
        strategy_hash="ec15e095fc3256a224bb965eb733455cff90ec2cd279f9d2395e6f8ba59c0e0d",
        symbol="BTCUSDT",
        timeframe="5m",
        candle_open_time=1786752000000,
        candle_close_time=1786752899999,
        generated_at=1786752900000,
        action=SignalAction.ENTER_LONG,
        reference_price=Decimal("63045.60"),
        reason="Supertrend Bullish Flip",
    )
    assert signal.action == SignalAction.ENTER_LONG

    order = Order(
        client_order_id="ESC-BTCUSDT-5M-001",
        symbol="BTCUSDT",
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=Decimal("0.158"),
        status=OrderStatus.CREATED,
    )
    assert not order.is_terminal
    order.status = OrderStatus.FILLED
    assert order.is_terminal
