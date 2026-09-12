import json
import urllib.request
from decimal import Decimal
from pathlib import Path

from live_engine.config import load_config, validate_config_against_manifest
from live_engine.dashboards import HYPEDashboard
from live_engine.execution.models import SignalAction, SignalEvent
from live_engine.execution.slot_book import SlotBook
from live_engine.market_data.candle_builder import MinuteCandleBuilder
from live_engine.market_data.models import AggTrade
from live_engine.parity.portfolio import bind_inputs, report_is_stale
from live_engine.persistence.event_store import EventStore
from live_engine.strategy.loader import StrategyLoader


BASE = Path(__file__).resolve().parents[1]
MANIFEST_PATH = BASE / "benchmarks/manifests/HYPE_LUXALGO_RANK12_5M.yaml"


def test_hype_frozen_configuration_and_parity_report():
    strategy, manifest = StrategyLoader.load_strategy(MANIFEST_PATH, BASE)
    configs = [load_config(str(BASE / f"config/hype-{mode}.json")) for mode in ("shadow", "paper")]
    for config in configs:
        validate_config_against_manifest(config, manifest)
    assert strategy.leverage == manifest["risk"]["leverage"] == 3
    assert strategy.slots == manifest["risk"]["slots"] == 12
    assert configs[0].event_store_path != configs[1].event_store_path
    assert manifest["allowed_modes"] == ["SHADOW", "PAPER", "TESTNET", "LIVE"]

    report = json.loads((BASE / "benchmarks/reports/HYPE_LUXALGO_RANK12_5M_parity_report.json").read_text())
    stale, differences = report_is_stale(report, bind_inputs(MANIFEST_PATH, manifest, BASE))
    assert not stale, differences
    assert report["status"] == "PASS"
    assert report["strategy_decision_parity_pct"] == 100.0
    assert report["benchmark_trades_count"] == report["matched_trades_count"] == 1369


def test_hype_dashboard_uses_external_history_and_isolated_database():
    dashboard = HYPEDashboard(mode="SHADOW", base_dir=BASE)
    payload = dashboard.get_dashboard_payload()
    assert dashboard.db_path == BASE / "data/hype_shadow.db"
    assert dashboard.candles_dir == Path("E:/EC/Binance-AggTrades/HYPEUSDT_USDM_DATA/candles")
    assert len(payload["candles"]) == 1000
    assert "taker_buy_base_volume" in payload["candles"][-1]
    assert payload["strategy_name"] == "HYPELuxAlgoRank12_5M"
    assert 0 <= payload["strategy_metrics"]["open_slots"] <= 12
    server = dashboard.start_server(port=0)
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{server.server_port}/api/data", timeout=10) as response:
            assert json.load(response)["symbol"] == "HYPEUSDT"
    finally:
        server.shutdown()
        server.server_close()


def test_hype_dashboard_journal_uses_independent_slot_closes(tmp_path):
    store = EventStore(tmp_path / "hype.db")
    store.log_event("STRATEGY_SLOT_CLOSED", {
        "entry_open_time": 1_700_000_000_000, "exit_quantity": "2",
        "entry_price": "100", "exit_price": "109", "net_pnl": "17.8",
        "realized_equity": "10017.8",
    })
    dashboard = HYPEDashboard(mode="SHADOW", base_dir=BASE)
    dashboard.db_path = tmp_path / "hype.db"
    payload = dashboard.get_dashboard_payload()
    assert payload["completed_trades"] == payload["win_count"] == 1
    assert payload["trade_journal"][0]["qty"] == "2"
    assert payload["trade_journal"][0]["pnl"] == 17.8


def test_taker_buy_volume_survives_candle_building():
    builder = MinuteCandleBuilder("HYPEUSDT")
    for agg_id, maker, quantity in ((1, False, "2"), (2, True, "3")):
        builder.add_trade(AggTrade(
            event_type="aggTrade", event_time=agg_id, symbol="HYPEUSDT", agg_trade_id=agg_id,
            price=Decimal("100"), quantity=Decimal(quantity), first_trade_id=agg_id,
            last_trade_id=agg_id, trade_time=agg_id, buyer_is_market_maker=maker,
            received_at=agg_id,
        ))
    candle = builder.finalize_current_candle()
    assert candle.volume == Decimal("5")
    assert candle.taker_buy_base_volume == Decimal("2")


def test_slot_book_barriers_fees_restart_and_pending_atr(tmp_path):
    store = EventStore(tmp_path / "hype.db")
    manifest = {
        "benchmark_id": "HYPE_LUXALGO_RANK12_5M",
        "risk": {"slots": 12, "leverage": 3},
        "execution_model": {
            "strategy_timeframe_ms": 300_000, "time_exit_bars": 384,
            "stop_atr_multiplier": 6, "take_profit_atr_multiplier": 4.5,
        },
    }
    book = SlotBook(store, manifest, Decimal("10000"))
    assert book.entry_notional() == Decimal("2500")
    assert book.entry_notional(Decimal("0.0005")) < Decimal("2500")
    signal = SignalEvent(
        signal_id="hype-entry-1", benchmark_id=manifest["benchmark_id"], strategy_hash="hash",
        symbol="HYPEUSDT", timeframe="5m", candle_open_time=0, candle_close_time=299_999,
        generated_at=299_999, action=SignalAction.ENTER_LONG, reference_price=Decimal("100"),
        reason="group 1", indicator_snapshot={"atr": 2.0},
    )
    slot = book.add_fill(signal, Decimal("25"), Decimal("100"), 300_000, Decimal("1.25"))
    assert (slot.stop_price, slot.take_profit_price) == (Decimal("88"), Decimal("109.0"))
    assert slot.expires_at_open_time == 300_000 + 383 * 300_000
    assert book.barrier_hits(Decimal("109")) == [(slot, "TP")]
    book.mark_exit_pending(slot.slot_id)
    assert book.close_fill(slot.slot_id, Decimal("25"), Decimal("109"), Decimal("1.3625"), "TP") == Decimal("222.3875")
    restored = SlotBook(store, manifest, Decimal("1"))
    assert restored.open_count == 0 and restored.realized_equity == Decimal("10222.3875")

    store.save_pending_action({
        "signal_id": signal.signal_id, "symbol": signal.symbol, "timeframe": signal.timeframe,
        "action": signal.action.value, "signal_candle_open_time": signal.candle_open_time,
        "expected_execution_open_time": 300_000, "benchmark_reference_price": signal.reference_price,
        "indicator_snapshot": signal.indicator_snapshot, "state": "PENDING", "created_at": 1,
    })
    pending = store.get_pending_actions()[0]
    assert json.loads(pending["indicator_snapshot_json"])["atr"] == 2.0
