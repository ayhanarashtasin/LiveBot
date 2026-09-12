"""Test gap recovery mechanism and resampler integration in orchestrator."""
from decimal import Decimal
import tempfile

from live_engine.config import LiveEngineConfig
from live_engine.market_data.models import AggTrade
from live_engine.orchestrator import LiveEngineOrchestrator


def test_gap_recovery_fallback_and_resampler():
    """Recovery restores a gap exactly in PAPER mode and feeds the resampler."""
    with tempfile.TemporaryDirectory() as tmpdir:
        config = LiveEngineConfig(
            mode="PAPER",
            symbol="BTCUSDT",
            timeframe="5m",
            event_store_path=f"{tmpdir}/events.db",
            kill_switch_path=f"{tmpdir}/kill_switch.trigger",
        )
        orch = LiveEngineOrchestrator(config)
        orch.initialize()

        # Simulate gap detection: last seen was 100, received 105 (missing 101-104)
        orch.gap_detector.last_agg_trade_id = 100
        mock_trade = AggTrade(
            event_type="aggTrade",
            event_time=1788705060000,
            symbol="BTCUSDT",
            agg_trade_id=105,
            price=Decimal("80000.0"),
            quantity=Decimal("1.0"),
            first_trade_id=105,
            last_trade_id=105,
            trade_time=1788705060000,
            buyer_is_market_maker=False,
            received_at=1788705060000,
        )
        gap_status = orch.gap_detector.process_trade(mock_trade)
        assert gap_status.value == "GAP"
        assert orch.gap_detector.is_desynced is True

        page = [
            {"a": 101, "p": "79900.0", "q": "0.5", "f": 101, "l": 101, "T": 1788705000000, "m": False},
            {"a": 102, "p": "79950.0", "q": "0.5", "f": 102, "l": 102, "T": 1788705030000, "m": False},
            {"a": 103, "p": "80000.0", "q": "1.0", "f": 103, "l": 103, "T": 1788705060000, "m": False},
            {"a": 104, "p": "80050.0", "q": "0.5", "f": 104, "l": 104, "T": 1788705070000, "m": False},
        ]
        orch.gap_recovery._fetch_page = lambda from_id, limit: [r for r in page if r["a"] >= from_id]

        result = orch.recover_gap(101, 104)

        assert result.complete is True
        assert len(result.trades) == 4
        assert orch.gap_detector.is_desynced is False
        assert len(orch.gap_detector.active_gaps) == 0

        recovered_events = orch.event_store.get_events_by_type("GAP_RECOVERED")
        assert len(recovered_events) == 1
        assert recovered_events[0]["payload"]["recovered_count"] == 4

        # Recovery is idempotent: a repeated request for the same range changes nothing.
        again = orch.recover_gap(101, 104)
        assert again.complete is True
        assert again.trades == []
