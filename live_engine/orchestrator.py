"""Live engine orchestrator.

Coordinates all core subcomponents:
- Gap detector & MinuteCandleBuilder
- TimeframeResampler (1m -> strategy timeframe)
- Strategy loader & SignalEngine
- Risk guard engine & Kill switch
- Order manager & Position manager
- Execution broker (Shadow / Paper / Binance Live)
- Event store persistence
- Account reconciliation
"""
from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Optional, List, Dict, Any
import asyncio
import json
import logging
import time

from live_engine.config import LiveEngineConfig, validate_config_against_manifest
from live_engine.market_data.models import AggTrade, Candle
from live_engine.market_data.gap_detector import AggTradeGapDetector
from live_engine.market_data.gap_recovery import AggTradeGapRecovery, RecoveryResult
from live_engine.market_data.candle_builder import MinuteCandleBuilder
from live_engine.market_data.resampler import TimeframeResampler, TIMEFRAME_MAP_MS
from live_engine.market_data.warmup import WarmupManager, fetch_closed_klines
from live_engine.strategy.loader import StrategyLoader
from live_engine.strategy.adapter import StrategyAdapter
from live_engine.strategy.signal_engine import SignalEngine
from live_engine.execution.models import Order, OrderSide, OrderStatus, OrderType, SignalAction, SignalEvent
from live_engine.execution.idempotency import generate_client_order_id
from live_engine.execution.position_manager import PositionManager
from live_engine.execution.order_manager import OrderManager
from live_engine.execution.fill_applier import FillApplier
from live_engine.execution.slot_book import SlotBook, StrategySlot
from live_engine.execution.order_filters import SymbolFilters, fetch_public_exchange_info
from live_engine.execution.broker import (
    ExecutionBroker,
    UnsupportedPositionModeError,
    ShadowBroker,
    PaperBroker,
    BinanceLiveBroker,
    BinanceTestnetBroker,
    BinanceUnknownOrderError,
)
from live_engine.market_data.binance_kline import BinanceKlineValidator
from live_engine.risk.kill_switch import KillSwitch
from live_engine.risk.guards import RiskGuardEngine
from live_engine.risk.health import HealthMonitor, HealthState, HealthSupervisor, IncidentAlerter
from live_engine.risk.protective import ProtectiveExitManager
from live_engine.risk.equity import EquityTracker
from live_engine.persistence.event_store import EventStore
from live_engine.monitoring.telemetry import ExecutionTelemetry
from live_engine.account.reconciliation import AccountReconciler
from live_engine.account.user_stream import BinanceUserDataStream
import os

logger = logging.getLogger(__name__)

# Maximum age of persisted last_accepted_agg_trade_id to restore on startup.
# Beyond 5 minutes, the engine was offline; historical continuity is bridged by
# candle warmup and kline healing rather than fetching hundreds of thousands of ticks via REST.
MAX_RESTART_GAP_AGE_S = 300.0
MAX_RECOVERABLE_GAP_SIZE = 500_000
# Recovery attempts for one aggTrade gap before it is declared unrecoverable. Binance drops
# old aggTrade pages, so a gap that has survived this many spaced attempts is gone for good;
# holding the runtime DATA_DESYNCED for it would stop the engine trading permanently.
MAX_GAP_RECOVERY_ATTEMPTS = 3



class LiveEngineOrchestrator:
    """End-to-end runtime orchestrator for live / shadow / paper trading."""

    def __init__(self, config: LiveEngineConfig):
        self.config = config
        self.base_dir = Path(__file__).resolve().parent.parent

        # 1. Event Store
        store_path = (self.base_dir / config.event_store_path).resolve()
        self.event_store = EventStore(store_path)

        # 2. Kill Switch
        ks_path = (self.base_dir / config.kill_switch_path).resolve()
        self.kill_switch = KillSwitch(ks_path)

        # 2b. Health supervision & incident alerting (one owner of "may we take new risk")
        self.health = HealthMonitor(mode=config.mode, event_store=self.event_store)
        self.alerter = IncidentAlerter(
            event_store=self.event_store,
            sink_path=self.base_dir / "logs" / "incidents.jsonl",
        )
        self.supervisor = HealthSupervisor(
            health=self.health, alerter=self.alerter, kill_switch=self.kill_switch,
        )

        # 3. Strategy & Signal Engine
        manifest_p = (self.base_dir / config.manifest_path).resolve()
        self.loader = StrategyLoader(manifest_p)
        strat_instance, self.manifest = self.loader.load()

        # Validate configuration against manifest (fill models included; a manifest without
        # explicit execution semantics fails here rather than defaulting silently).
        validate_config_against_manifest(config, self.manifest)
        execution_model = self.manifest.get("execution_model", {})
        self.entry_fill_model = execution_model["entry_fill_model"]
        self.exit_fill_model = execution_model["exit_fill_model"]
        self.adapter = StrategyAdapter(strat_instance, self.manifest)
        self.signal_engine = SignalEngine(self.adapter)
        slot_count = int(self.manifest.get("risk", {}).get("slots", 1))
        self.slot_book: Optional[SlotBook] = (
            SlotBook(self.event_store, self.manifest, config.stake_amount) if slot_count > 1 else None
        )

        # 4. Market Data Pipeline
        self.gap_detector = AggTradeGapDetector(symbol=config.symbol)
        self.candle_builder = MinuteCandleBuilder(symbol=config.symbol)
        self.resampler = TimeframeResampler(symbol=config.symbol, target_timeframe=config.timeframe)
        self.warmup_manager = WarmupManager(
            symbol=config.symbol,
            timeframe=config.timeframe,
            required_candles=config.warmup_candles,
        )
        self.gap_recovery = AggTradeGapRecovery(
            symbol=config.symbol,
            base_url="https://testnet.binancefuture.com" if config.binance_testnet else "https://fapi.binance.com",
        )
        self._recovery_tasks: set = set()

        # 5. Position & Order Managers
        self.position_manager = PositionManager(symbol=config.symbol)
        self.order_manager = OrderManager()
        # One delta-based path for REST responses, user-stream events and reconciliation.
        self.fill_applier = FillApplier(
            order_manager=self.order_manager,
            position_manager=self.position_manager,
            event_store=self.event_store,
            symbol=config.symbol,
        )

        # 6. Exchange Filters (Default BTCUSDT USD-M specs if not overridden)
        self.filters = SymbolFilters(
            symbol=config.symbol,
            tick_size=Decimal("0.10"),
            step_size=Decimal("0.001"),
            min_qty=Decimal("0.001"),
            max_qty=Decimal("1000.0"),
            min_notional=Decimal("5.0"),
        )

        # 7. Risk Guards (equity tracker attached after the broker exists)
        manifest_risk = self.manifest.get("risk", {}) or {}
        self.risk_guards = RiskGuardEngine(
            kill_switch=self.kill_switch,
            max_open_positions=config.max_open_trades,
            filters=self.filters,
            require_fresh_market_data=config.mode in ("LIVE", "TESTNET"),
            leverage=Decimal(str(manifest_risk.get("leverage", 1))),
            taker_fee=Decimal(str(self.manifest.get("fees", {}).get("taker", "0.0005"))),
        )

        # 8. Execution Broker
        if config.mode == "LIVE":
            if not config.binance_api_key or not config.binance_api_secret:
                raise ValueError("Binance API key and secret are required for LIVE mode")
            self.broker: ExecutionBroker = BinanceLiveBroker(
                api_key=config.binance_api_key,
                api_secret=config.binance_api_secret,
            )
        elif config.mode == "TESTNET":
            testnet_key = os.environ.get("BINANCE_TESTNET_API_KEY", config.binance_api_key or "testnet_key")
            testnet_secret = os.environ.get("BINANCE_TESTNET_API_SECRET", config.binance_api_secret or "testnet_secret")
            self.broker = BinanceTestnetBroker(
                api_key=testnet_key,
                api_secret=testnet_secret,
            )
        elif config.mode == "PAPER":
            self.broker = PaperBroker(
                event_store=self.event_store,
                taker_fee=Decimal(str(self.manifest.get("fees", {}).get("taker", "0.0005"))),
                leverage=int(self.manifest.get("risk", {}).get("leverage", 1)),
                filters=self.filters,
            )
        else:
            self.broker = ShadowBroker()

        # 8b. Authoritative equity / mark price / daily-loss accounting
        self.equity = EquityTracker(
            event_store=self.event_store,
            broker=self.broker,
            position_manager=self.position_manager,
            health=self.health,
            kill_switch=self.kill_switch,
        )
        self.risk_guards.equity_tracker = self.equity
        self.supervisor.equity = self.equity

        # 9. Account Reconciler
        self.reconciler = AccountReconciler(
            broker=self.broker,
            position_manager=self.position_manager,
            kill_switch=self.kill_switch,
            event_store=self.event_store,
            order_manager=self.order_manager,
            fill_applier=self.fill_applier,
            health=self.health,
        )

        # 9b. Execution-quality telemetry (append-only, independent of strategy logic)
        self.telemetry = ExecutionTelemetry(
            event_store=self.event_store,
            benchmark_id=self.manifest.get("benchmark_id", "UNKNOWN"),
            mode=config.mode,
            strategy_hash=self.manifest.get("strategy", {}).get("source_hash", ""),
            helper_hash=self.manifest.get("strategy", {}).get("helper_hash", ""),
        )

        # 9c. Protective-exit state (levels, linkage, unprotected-interval reporting)
        self.protective = ProtectiveExitManager(
            event_store=self.event_store,
            manifest=self.manifest,
            symbol=config.symbol,
            health=self.health,
            broker=self.broker,
        )

        # 10. Secondary Kline Validator
        self.kline_validator = BinanceKlineValidator(
            symbol=config.symbol,
            event_store=self.event_store,
        )

        # 11. User Data Stream (for LIVE/TESTNET account events)
        self.user_data_stream: Optional[BinanceUserDataStream] = None
        if config.mode in ("LIVE", "TESTNET"):
            self.user_data_stream = BinanceUserDataStream(
                api_key=config.binance_api_key or "",
                testnet=(config.mode == "TESTNET"),
                on_order_trade_update=self.on_user_order_trade_update,
                on_account_update=self.on_user_account_update,
                on_lifecycle_event=self.on_user_stream_lifecycle,
            )

        self._latest_strategy_candle: Optional[Candle] = None
        self._is_initialized = False
        # Guards the recursive replay of bars recovered by history healing.
        self._replaying_history = False
        # The first 1m candle built after connecting is always missing its early trades.
        self._first_1m_pending = True
        # Gap ranges with a recovery attempt in flight, so the per-minute retry sweep never
        # stacks a second attempt on top of one that is still running.
        self._recovering: set = set()

    @property
    def rest_base_url(self) -> str:
        """Futures REST host for this runtime."""
        return "https://testnet.binancefuture.com" if self.config.binance_testnet else "https://fapi.binance.com"

    @property
    def exit_position_side(self) -> Optional[str]:
        """positionSide to send with orders; None in one-way mode where it is implicit."""
        return "LONG" if getattr(self.broker, "position_mode", "ONE_WAY") == "HEDGE" else None

    def validate_position_mode(self) -> str:
        """Detects and validates the exchange position mode during authenticated startup."""
        detect = getattr(self.broker, "detect_position_mode", None)
        if detect is None:
            raise UnsupportedPositionModeError(
                f"UNSUPPORTED POSITION MODE: broker {type(self.broker).__name__} cannot report "
                f"the futures position mode; authenticated trading is refused."
            )
        mode = detect()
        if mode not in ("ONE_WAY", "HEDGE"):
            raise UnsupportedPositionModeError(f"UNSUPPORTED POSITION MODE: {mode}")
        self.event_store.log_event("POSITION_MODE_VALIDATED", {"position_mode": mode})
        logger.info(f"Position mode validated: {mode}")
        return mode

    async def shutdown(self, reason: str = "operator stop") -> None:
        """Stops background work cleanly and flushes durable state.

        Killing the process is not a shutdown: a half-applied fill, an unsaved heartbeat
        or an orphaned task is exactly the state reconciliation then has to untangle.
        """
        logger.info("Graceful shutdown requested: %s", reason)
        for task in list(self._recovery_tasks):
            task.cancel()
        for task in list(self._recovery_tasks):
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        self._recovery_tasks.clear()

        if self.user_data_stream is not None:
            try:
                await self.user_data_stream.stop()
            except Exception as exc:
                logger.warning("User stream shutdown error: %s", exc)
        if getattr(self.kline_validator, "_running", False):
            try:
                await self.kline_validator.stop()
            except Exception as exc:
                logger.warning("Kline validator shutdown error: %s", exc)

        self.health.beat("engine", "STOPPED", reason)
        self.event_store.set_state("last_accepted_agg_trade_id", self.gap_detector.last_agg_trade_id)
        self.event_store.log_event("SYSTEM_SHUTDOWN", {
            "reason": reason,
            "health": self.health.snapshot(),
            "position": self.position_manager.to_dict(),
        })
        logger.info("Shutdown complete; state flushed.")

    def _validate_parity_report(self) -> None:
        """Validates that benchmark parity report exists and comprehensively passes before startup.
        Fail-closed enforcement with full field validation."""
        if self.config.mode not in ("SHADOW", "PAPER"):
            return

        benchmark_id = self.manifest.get("benchmark_id", "UNKNOWN")
        manifest_symbol = self.manifest.get("symbol", "UNKNOWN")
        manifest_strategy_hash = self.manifest.get("strategy", {}).get("source_hash", "UNKNOWN")
        manifest_helper_hash = self.manifest.get("strategy", {}).get("helper_hash", "UNKNOWN")

        report_path = self.base_dir / f"benchmarks/reports/{benchmark_id}_parity_report.json"

        if not report_path.exists():
            raise RuntimeError(
                f"PARITY VALIDATION FAILED: Report not found for {benchmark_id}. Expected: {report_path}."
            )

        try:
            import json
            report = json.loads(report_path.read_text())

            # 1. Check status is PASS
            status = report.get("status", "UNKNOWN")
            if status != "PASS":
                raise RuntimeError(f"Report status is {status}, not PASS")

            # 2. Verify strategy decision parity is 100% (main field)
            decision_pct = float(report.get("strategy_decision_parity_pct", 0.0))
            if decision_pct < 100.0:
                raise RuntimeError(f"strategy_decision_parity_pct={decision_pct:.1f}%, requires 100%")

            # 3. Verify other parity fields if present (backward compatible with older reports)
            optional_100_pct = [
                "candle_timestamp_parity_pct",
                "entry_signal_parity_pct",
                "exit_signal_parity_pct",
                "trade_direction_parity_pct",
            ]
            for field in optional_100_pct:
                if field in report:
                    pct = float(report.get(field, 0.0))
                    if pct < 100.0:
                        raise RuntimeError(f"{field}={pct:.1f}%, requires 100%")

            # 4. Verify trade counts match benchmark
            bm_trades = int(report.get("benchmark_trades_count", 0))
            replay_trades = int(report.get("replay_trades_count", 0))
            matched_trades = int(report.get("matched_trades_count", 0))
            if bm_trades != replay_trades or matched_trades != bm_trades:
                raise RuntimeError(
                    f"Trade count mismatch: benchmark={bm_trades}, replay={replay_trades}, matched={matched_trades}"
                )
            if bm_trades == 0:
                raise RuntimeError("No trades in benchmark (possibly synthetic or corrupted report)")

            # 5. Cross-check benchmark ID
            report_benchmark_id = report.get("benchmark_id", "UNKNOWN")
            if report_benchmark_id != benchmark_id:
                raise RuntimeError(f"Report benchmark_id={report_benchmark_id} != config {benchmark_id}")

            logger.info(
                f"Parity validation PASSED: {benchmark_id} "
                f"(100% decision parity, {bm_trades}/{replay_trades} trades matched, "
                f"return: {report.get('replay_net_return_pct', 0):.2f}%)"
            )
        except RuntimeError:
            raise
        except (json.JSONDecodeError, KeyError, ValueError, TypeError) as e:
            raise RuntimeError(f"Report parsing failed: {e}")

    def initialize(self) -> None:
        """Primes warmup candles, restores state, and performs startup reconciliation."""
        logger.info(f"Initializing LiveEngine in {self.config.mode} mode for {self.config.symbol}...")

        # 0. Validate parity report (fail-closed)
        self._validate_parity_report()

        # 0b. Restore the durable market-data high-water mark so the first live event is
        # compared against stored state instead of being accepted blind, provided the state
        # is from a recent run (<= 300s). Beyond that, the offline interval is bridged
        # by candle warmup and kline healing rather than fetching hundreds of thousands of ticks.
        last_id = self.event_store.get_state("last_accepted_agg_trade_id")
        updated_at = self.event_store.get_state_updated_at("last_accepted_agg_trade_id")
        if last_id is not None and updated_at is not None:
            age_s = (time.time() * 1000 - updated_at) / 1000.0
            if age_s <= MAX_RESTART_GAP_AGE_S:
                self.gap_detector.restore_last_accepted_id(last_id)
            else:
                logger.info(
                    f"Stored aggTrade high-water mark ({last_id}) is {age_s / 3600:.1f}h old "
                    f"(> {MAX_RESTART_GAP_AGE_S:.0f}s); starting stream baseline fresh. "
                    f"Historical continuity is managed via candle backfill."
                )
        elif last_id is not None:
            self.gap_detector.restore_last_accepted_id(last_id)

        # 1. Restore any open orders from persistent store
        saved_orders = self.event_store.get_open_orders()
        for o in saved_orders:
            self.order_manager.upsert_order(o)
        if saved_orders:
            logger.info(f"Restored {len(saved_orders)} unresolved open orders from database.")

        # 2. Load exchange filters
        if self.config.mode in ("SHADOW", "PAPER"):
            # For SHADOW/PAPER, fetch from public Binance endpoint (no authentication)
            try:
                public_filters = fetch_public_exchange_info(self.config.symbol, timeout=10)
                if public_filters:
                    self.filters = public_filters
                    self.risk_guards.filters = public_filters
                    if hasattr(self.broker, "filters"):
                        self.broker.filters = public_filters
                    logger.info(f"Loaded public exchange filters for {self.config.symbol}")
                else:
                    logger.warning(f"Failed to fetch public exchange filters for {self.config.symbol}. Using default filters.")
            except Exception as e:
                logger.warning(f"Exchange filter fetch failed ({e}). Using default filters for {self.config.mode} mode.")
        elif hasattr(self.broker, "get_symbol_filters"):
            # For LIVE/TESTNET, use authenticated broker method
            try:
                live_filters = self.broker.get_symbol_filters(self.config.symbol)
                if live_filters:
                    self.filters = live_filters
                    self.risk_guards.filters = live_filters
                    logger.info(f"Synchronized live exchange filters for {self.config.symbol}")
            except Exception as e:
                logger.warning(f"Could not fetch dynamic exchange filters, using defaults: {e}")

        # 3. Load warmup candles (fail-closed if missing or insufficient)
        data_cfg = self.manifest.get("data", {})
        dataset_path = data_cfg.get("dataset_candle_path", f"{self.config.symbol}_USDM_DATA/candles/{self.config.symbol}_{self.config.timeframe}.parquet")
        ds_path = (self.base_dir / dataset_path).resolve()

        if not ds_path.exists():
            raise RuntimeError(
                f"WARMUP VALIDATION FAILED: Dataset not found at {ds_path}. "
                f"Symbol {self.config.symbol} requires historical candle data."
            )

        try:
            self.warmup_manager.load_from_parquet(str(ds_path))
            warmup_candles = self.warmup_manager.candle_buffer

            if len(warmup_candles) < self.config.warmup_candles:
                raise RuntimeError(
                    f"WARMUP VALIDATION FAILED: {len(warmup_candles)} candles loaded but {self.config.warmup_candles} required. "
                    f"Dataset may be corrupted or insufficient."
                )

            # Check for data gap: compare last historical candle time to current time
            if warmup_candles:
                last_candle_time_ms = warmup_candles[-1].close_time
                from datetime import datetime, timezone
                last_candle_time = datetime.fromtimestamp(last_candle_time_ms / 1000, tz=timezone.utc)
                current_time = datetime.now(tz=timezone.utc)
                time_gap_hours = (current_time - last_candle_time).total_seconds() / 3600

                # The Parquet dataset is static and is normally stale by the time we start.
                # Close the gap with exchange klines so the strategy receives one contiguous
                # history; splicing live candles onto a stale tail corrupts every indicator.
                if time_gap_hours > 0.5:
                    logger.info(
                        f"WARMUP BACKFILL: Parquet tail is {time_gap_hours:.1f}h old. "
                        f"Fetching intervening klines from the exchange."
                    )
                    base_url = self.rest_base_url
                    added = self.warmup_manager.backfill_gap_from_rest(base_url=base_url)
                    warmup_candles = self.warmup_manager.candle_buffer
                    logger.info(
                        f"WARMUP BACKFILL: appended {added} bars; history now contiguous "
                        f"through {warmup_candles[-1].open_datetime_utc}."
                    )

            self.signal_engine.prime_history(warmup_candles)
            self.health.warmup_ready = len(warmup_candles) >= self.config.warmup_candles
            if warmup_candles:
                self._latest_strategy_candle = warmup_candles[-1]
            logger.info(f"SignalEngine primed with {len(warmup_candles)} warmup candles.")
        except (FileNotFoundError, ValueError, RuntimeError) as e:
            if isinstance(e, RuntimeError):
                raise
            raise RuntimeError(f"WARMUP VALIDATION FAILED: Could not load dataset: {e}")

        # 3b. Validate the authenticated futures position mode. One-way uses reduceOnly
        # exits; hedge mode needs an explicit positionSide. An undeterminable mode fails
        # closed here, before READY, rather than at the first exit.
        if self.config.mode in ("LIVE", "TESTNET"):
            self.validate_position_mode()

        # 4. Rebuild position and order state from the durable fill ledger, then reconcile
        # against the exchange. Both steps are idempotent, so a restart repairs state once.
        replayed = self.fill_applier.replay_from_store()
        if replayed:
            logger.info(f"Replayed {replayed} stored fill event(s) to rebuild position state.")

        recon = self.reconciler.reconcile_all(
            symbol=self.config.symbol,
            open_orders=self.order_manager.get_open_orders(),
            since_ms=self.event_store.get_state("last_reconciled_trade_ms"),
        )
        rec_res, bal_res = recon["position"], recon["balance"]
        ord_res = recon["orders"]
        self.event_store.set_state("last_reconciled_trade_ms", int(time.time() * 1000))
        self.equity.restore()
        # Paper mode has no broker mark price; it only arrives on the first candle close.
        # Seed it from the warmed history so the day-start snapshot marks an open position
        # instead of booking it at zero PnL.
        if self._latest_strategy_candle is not None:
            self.equity.update_mark_price(
                Decimal(str(self._latest_strategy_candle.close)),
                self._latest_strategy_candle.close_time,
            )
        self.equity.refresh()
        self.equity.ensure_daily_snapshot()
        if self.slot_book:
            mismatch = self.slot_book.reconcile_quantity(
                self.position_manager.quantity, getattr(self.filters, "step_size", Decimal("0"))
            )
            if mismatch:
                self.health.reconciliation_required = True
                self.event_store.record_incident("SLOT_BOOK_MISMATCH", "HIGH", mismatch)
                logger.error(mismatch)
            # For a multi-slot strategy the slot book *is* the protective state: its per-slot
            # barriers are the stop and the target. Record the same reconciliation evidence the
            # single-slot protective manager records, so the protective-state gate reads this
            # strategy's actual source instead of finding nothing.
            self.event_store.log_event("PROTECTIVE_RECONCILIATION", {
                "symbol": self.config.symbol,
                "source": "slot_book",
                "open_position": not self.position_manager.is_flat,
                "has_protective_state": True,
                "open_slots": self.slot_book.open_count,
                "issues": [mismatch] if mismatch else [],
            })
        else:
            self.protective.reconcile(
                open_position=not self.position_manager.is_flat,
                position_quantity=self.position_manager.quantity,
            )
        logger.info(
            f"Startup reconciliation completed: reconciled={recon['reconciled']}, "
            f"position={rec_res['matched']}, orders={ord_res['open_orders_audited']}, "
            f"unresolved={recon['unresolved_orders']}"
        )

        self.event_store.log_event(
            event_type="SYSTEM_INITIALIZED",
            payload={
                "mode": self.config.mode,
                "symbol": self.config.symbol,
                "timeframe": self.config.timeframe,
                "warmup_count": len(warmup_candles),
                "reconciliation": rec_res,
                "balance": bal_res,
                "open_orders": ord_res,
            },
        )
        self._is_initialized = True

    def process_aggtrade(self, agg_trade: AggTrade) -> List[SignalEvent]:
        """Feeds a single AggTrade into the engine pipeline."""
        if not self._is_initialized:
            raise RuntimeError("Engine not initialized. Call initialize() first.")

        # Persist raw aggTrade to SQLite WAL event store
        self.event_store.store_aggtrade(agg_trade)
        self.health.beat("engine")
        self.health.beat("public_stream")

        # Check for sequence gap
        gap_status = self.gap_detector.process_trade(agg_trade)
        if gap_status.value == "GAP":
            active_gap = self.gap_detector.active_gaps[-1] if self.gap_detector.active_gaps else None
            if active_gap:
                gap_size = active_gap.to_id - active_gap.from_id + 1
                if gap_size > MAX_RECOVERABLE_GAP_SIZE:
                    self._abandon_gap(
                        active_gap.from_id, active_gap.to_id,
                        f"{gap_size} trades exceeds the aggTrade recovery limit "
                        f"({MAX_RECOVERABLE_GAP_SIZE})",
                    )
                else:
                    self.health.data_desynced = True
                    self.event_store.log_event("AGGTRADE_GAP", {"from_id": active_gap.from_id, "to_id": active_gap.to_id})
                    self._schedule_gap_recovery(active_gap.from_id, active_gap.to_id)

        # A next_open action executes at the first event of its execution candle.
        tf_ms = TIMEFRAME_MAP_MS[self.config.timeframe]
        self.process_pending_actions((agg_trade.trade_time // tf_ms) * tf_ms, agg_trade.price)
        signals: List[SignalEvent] = self._process_slot_barriers(agg_trade)

        # Process trade into 1m candle
        closed_1m = self.candle_builder.add_trade(agg_trade)
        if closed_1m:
            # We joined the trade stream part-way through this minute, so its open/high/low/volume
            # are missing every trade before we connected. Mark it so the resampler refuses to
            # treat the strategy bar containing it as whole.
            if self._first_1m_pending:
                closed_1m.is_closed = False
                self._first_1m_pending = False
            logger.info(
                f"[1m CANDLE] {closed_1m.open_datetime_utc} | Open=${closed_1m.open} High=${closed_1m.high} Low=${closed_1m.low} Close=${closed_1m.close} Vol={closed_1m.volume}"
            )
            self.event_store.store_candle(closed_1m)
            self.kline_validator.register_reconstructed_candle(closed_1m)
            self.health.beat("kline_validator")
            # Durable high-water mark so a restart compares the first new stream event
            # against stored state rather than starting blind.
            # ponytail: persisted per closed minute, not per trade. A restart can re-detect
            # at most one minute of already-stored trades; recovery is idempotent so it
            # costs one redundant fetch. Persist per trade only if that ever matters.
            self.event_store.set_state("last_accepted_agg_trade_id", self.gap_detector.last_agg_trade_id)
            self.health.beat("public_stream")
            self._retry_stalled_gaps()
            closed_strategy = self.resampler.add_1m_candle(closed_1m)
            if closed_strategy:
                signals.extend(self._on_strategy_candle_closed(closed_strategy))

        return signals

    def process_candle(self, candle: Candle) -> List[SignalEvent]:
        """Directly processes a completed candle (e.g. from historical data or Kline feed)."""
        if not self._is_initialized:
            raise RuntimeError("Engine not initialized. Call initialize() first.")
        return self._on_strategy_candle_closed(candle)

    def _on_strategy_candle_closed(self, candle: Candle) -> List[SignalEvent]:
        """Executes full strategy and order generation workflow on candle close."""
        self._latest_strategy_candle = candle
        # Persist reconstructed closed candle to database
        self.event_store.store_candle(candle)
        self.event_store.log_event("CANDLE_CLOSED", candle.to_dict())
        slot_exit_signals = self._process_slot_time_exits(candle)

        # A bar we only partially witnessed (engine started or restarted mid-bucket) has a wrong
        # open/high/low/volume. Persist it for the dashboard, but never let it reach the strategy
        # history, where it would corrupt every indicator derived from it.
        if not candle.is_closed:
            logger.warning(
                f"[PARTIAL BAR SKIPPED] {candle.open_datetime_utc} — engine joined mid-bucket; "
                f"excluded from strategy history to protect indicator state."
            )
            self.event_store.record_incident(
                category="PARTIAL_CANDLE_SKIPPED",
                severity="MEDIUM",
                details=f"Partial {candle.timeframe} bar at {candle.open_datetime_utc} excluded from strategy history.",
            )
            return slot_exit_signals

        replayed_signals: List[SignalEvent] = []
        if not self._replaying_history:
            contiguous, filler = self._ensure_contiguous_history(candle)
            if not contiguous:
                # Fail closed: an incomplete history means every indicator on this bar is
                # wrong. Observation and protective exits continue; evaluation does not.
                return []
            if filler:
                self._replaying_history = True
                try:
                    for bar in filler:
                        replayed_signals.extend(self._on_strategy_candle_closed(bar))
                finally:
                    self._replaying_history = False

        # Market data desynced: a *locally reconstructed* bar is built from an incomplete
        # trade sequence and must not reach the strategy. The exchange's own kline for the
        # same window is unaffected by the hole, so it is substituted and evaluated on time
        # rather than dropping the bar and leaving the position unmanaged. A bar that already
        # came from REST (history healing, or a previous substitution) needs no such check.
        if candle.provenance != "rest_kline" and (self.gap_detector.is_desynced or self.health.data_desynced):
            substitute = self._authoritative_bar(candle)
            if substitute is None:
                reason = (
                    f"Strategy evaluation blocked at {candle.open_datetime_utc}: market data "
                    f"DESYNCED ({len(self.gap_detector.active_gaps)} active gap(s)) and the "
                    f"exchange kline for this bar could not be fetched."
                )
                logger.warning(reason)
                self.event_store.record_incident("EVALUATION_BLOCKED_DESYNCED", "HIGH", reason)
                if not self.position_manager.is_flat:
                    # Protective exits are strategy-managed, so a bar that cannot be evaluated
                    # is a bar in which the open position has no working protection anywhere.
                    self.alerter.alert(
                        "UNPROTECTED_INTERVAL", "HIGH",
                        f"Open {self.config.symbol} position of {self.position_manager.quantity} is "
                        f"unprotected while market data is desynced.",
                        {"symbol": self.config.symbol, "protective": self.protective.describe()},
                    )
                return slot_exit_signals + replayed_signals
            candle = substitute
            self._latest_strategy_candle = candle

        # Refresh the authoritative equity and mark price before any decision is acted on,
        # so the guards judge this bar against current data rather than the last one.
        self.equity.update_mark_price(Decimal(str(candle.close)), candle.close_time)
        self.equity.refresh()

        # 1. Generate Signal
        signal = self.signal_engine.on_candle_close(candle)
        if not signal:
            logger.info(
                f"[{self.config.timeframe} EVALUATION] {candle.open_datetime_utc} | Close=${candle.close} | Position={self.position_manager.quantity} {self.config.symbol} | Decision: NO_ACTION (Hold)"
            )
            # Reconcile on each candle close even if no signal
            self.reconciler.reconcile_position()
            return slot_exit_signals + replayed_signals

        if signal.action == SignalAction.ENTER_LONG and self.slot_book and not self.slot_book.can_open:
            self.event_store.log_event("SLOT_ENTRY_BLOCKED", {
                "signal": signal.to_dict(), "slot_book": self.slot_book.describe(),
            })
            logger.info("HYPE entry blocked: slot book is full (%s/%s).", self.slot_book.open_count, self.slot_book.limit)
            self.reconciler.reconcile_position()
            return slot_exit_signals + replayed_signals

        self.event_store.store_signal(signal)
        self.event_store.log_event("SIGNAL_GENERATED", signal.to_dict())
        logger.info(f">>> [DECISION SIGNAL] {signal.action.value} at {candle.open_datetime_utc} | Price=${candle.close} | Reason: {signal.reason}")

        candle_close_px = Decimal(str(candle.close))

        # 2. Route the action through the manifest's declared fill model. A next_open model
        # must not be executed on the signal candle: that is the timing divergence between
        # the benchmark and the runtime this release exists to remove.
        if signal.action == SignalAction.ENTER_LONG:
            if self.entry_fill_model == "signal_close":
                self._submit_entry(signal, candle_close_px, candle.open_time, candle)
            else:
                self._defer_action(signal, candle, candle_close_px)
        elif signal.action == SignalAction.EXIT_LONG:
            reference_px = signal.reference_price if (signal.reference_price and signal.reference_price > Decimal("0")) else candle_close_px
            if self.exit_fill_model in ("signal_reference", "signal_close"):
                # The benchmark's stop/TP level is the *reference*, not the fill. By the
                # time the decision is known (bar close) that level is no longer executable,
                # so the order is submitted at the market price available at that moment and
                # the difference is measured as slippage instead of being assumed away.
                self._submit_exit(signal, self._executable_exit_price(reference_px, candle_close_px),
                                  candle.open_time, benchmark_reference=reference_px)
            else:
                self._defer_action(signal, candle, reference_px)

        # Reconcile after trading decision
        self.reconciler.reconcile_position()
        return slot_exit_signals + replayed_signals + [signal]

    # --- concurrent logical slots -----------------------------------------

    def _process_slot_barriers(self, trade: AggTrade) -> List[SignalEvent]:
        if not self.slot_book:
            return []
        signals = []
        for slot, reason in self.slot_book.barrier_hits(trade.price):
            signal = self._submit_slot_exit(slot, reason, trade.price, trade.trade_time)
            if signal:
                signals.append(signal)
        return signals

    def _process_slot_time_exits(self, candle: Candle) -> List[SignalEvent]:
        if not self.slot_book or not candle.is_closed:
            return []
        signals = []
        for slot in self.slot_book.time_hits(candle.open_time):
            signal = self._submit_slot_exit(slot, "TIME", candle.close, candle.close_time)
            if signal:
                signals.append(signal)
        return signals

    def _submit_slot_exit(
        self, slot: StrategySlot, reason: str, price: Decimal, observed_at: int
    ) -> Optional[SignalEvent]:
        """Close one logical lot through the normal reduce-only order path."""
        assert self.slot_book is not None
        self.slot_book.mark_exit_pending(slot.slot_id)
        tf_ms = TIMEFRAME_MAP_MS[self.config.timeframe]
        signal = SignalEvent(
            signal_id=f"SIG-{self.config.symbol}-{self.config.timeframe}-{slot.entry_signal_open_time}-EXIT_LONG",
            benchmark_id=self.manifest["benchmark_id"],
            strategy_hash=self.manifest["strategy"]["source_hash"],
            symbol=self.config.symbol,
            timeframe=self.config.timeframe,
            candle_open_time=(observed_at // tf_ms) * tf_ms,
            candle_close_time=observed_at,
            generated_at=observed_at,
            action=SignalAction.EXIT_LONG,
            reference_price=Decimal(str(price)),
            reason=reason,
            requested_position_size=slot.quantity,
            indicator_snapshot={"slot_entry_price": float(slot.entry_price), "slot_atr": float(slot.atr)},
        )
        self.event_store.store_signal(signal)
        self.event_store.log_event("SLOT_EXIT_TRIGGERED", {
            "signal": signal.to_dict(), "slot": slot.to_dict(),
        })
        order = self._submit_exit(
            signal, Decimal(str(price)), slot.entry_signal_open_time,
            benchmark_reference=Decimal(str(price)), slot_id=slot.slot_id,
        )
        if self.slot_book.has_slot(slot.slot_id):
            remaining = next(value for value in self.slot_book.slots if value.slot_id == slot.slot_id)
            if remaining.state == "EXIT_PENDING":
                self.slot_book.reopen(slot.slot_id)
        return signal if order and order.status in (OrderStatus.FILLED, OrderStatus.PARTIALLY_FILLED) else None

    # --- position sizing ----------------------------------------------------

    def _stake_for_entry(self) -> Decimal:
        """Stake under the manifest's frozen sizing rule, capped by canary mode."""
        manifest_risk = self.manifest.get("risk", {}) if hasattr(self, "manifest") else {}
        is_compounding = (
            manifest_risk.get("stake_rule") == "full_compounding"
            or getattr(self.config, "stake_rule", "full_compounding") == "full_compounding"
        )
        if is_compounding:
            balances = self.broker.get_account_balance()
            equity = balances.get("USDT", self.config.stake_amount)
            stake = equity if equity > Decimal("0") else self.config.stake_amount
        else:
            stake = self.config.stake_amount

        if getattr(self.config, "canary_mode", False):
            canary_cap = getattr(self.config, "max_canary_allocation_usd", Decimal("100.0"))
            if stake > canary_cap:
                self.event_store.log_event("CANARY_CAP_APPLIED", {"stake": str(canary_cap), "cap": str(canary_cap)})
                stake = canary_cap
        return stake

    # --- deferred (next_open) execution -------------------------------------

    def _defer_action(self, signal: SignalEvent, candle: Candle, reference_px: Decimal) -> None:
        """Persists a next_open action so it survives a restart and executes exactly once."""
        tf_ms = TIMEFRAME_MAP_MS[self.config.timeframe]
        expected_open = candle.open_time + tf_ms
        self.event_store.save_pending_action({
            "signal_id": signal.signal_id,
            "symbol": self.config.symbol,
            "timeframe": self.config.timeframe,
            "action": signal.action.value,
            "signal_candle_open_time": candle.open_time,
            "expected_execution_open_time": expected_open,
            "benchmark_reference_price": reference_px,
            "indicator_snapshot": signal.indicator_snapshot,
            "quantity": None,
            "state": "PENDING",
            "created_at": int(time.time() * 1000),
        })
        self.event_store.log_event("PENDING_ACTION_CREATED", {
            "signal_id": signal.signal_id,
            "action": signal.action.value,
            "signal_candle_open_time": candle.open_time,
            "expected_execution_open_time": expected_open,
            "benchmark_reference_price": str(reference_px),
        })
        logger.info(
            f"[PENDING {signal.action.value}] signal at {candle.open_datetime_utc} will execute at the "
            f"open of the next {self.config.timeframe} candle ({expected_open})."
        )

    def process_pending_actions(self, bucket_open_ms: int, price: Decimal) -> List[str]:
        """Executes or expires persisted next_open actions at a strategy-bucket boundary.

        Called with the open time of the bucket the current market event belongs to and
        that event's price. An action fires at most once, only in its own bucket, and only
        while market data is synchronised.
        """
        resolutions: List[str] = []
        for row in self.event_store.get_pending_actions():
            if row["symbol"] != self.config.symbol or row["timeframe"] != self.config.timeframe:
                continue
            expected = int(row["expected_execution_open_time"])
            if bucket_open_ms < expected:
                continue

            if bucket_open_ms > expected:
                # The execution candle came and went unobserved. Approximating the fill on a
                # later bar would invent a price the benchmark never used.
                self.event_store.resolve_pending_action(row["signal_id"], "EXPIRED_MISSED_EXECUTION_WINDOW")
                self.event_store.record_incident(
                    "PENDING_ACTION_EXPIRED", "HIGH",
                    f"{row['action']} for signal {row['signal_id']} expected execution at {expected} "
                    f"but the first observed bucket was {bucket_open_ms}; expired without a fill.",
                )
                resolutions.append("EXPIRED")
                continue

            if self.gap_detector.is_desynced or self.health.data_desynced or self.health.history_gap_open:
                # Fail closed: the next open cannot be observed reliably, so no fill is made up.
                self.event_store.record_incident(
                    "PENDING_ACTION_BLOCKED_DESYNCED", "HIGH",
                    f"{row['action']} for signal {row['signal_id']} could not execute at {expected}: "
                    f"market data is not synchronised.",
                )
                resolutions.append("BLOCKED")
                continue

            self._execute_pending(row, price)
            resolutions.append("EXECUTED")
        return resolutions

    def _execute_pending(self, row: Dict[str, Any], price: Decimal) -> None:
        """Submits one restored pending action, preserving its original signal identity."""
        signal = SignalEvent(
            signal_id=row["signal_id"],
            benchmark_id=self.manifest.get("benchmark_id", "UNKNOWN"),
            strategy_hash=self.manifest.get("strategy", {}).get("source_hash", ""),
            symbol=row["symbol"],
            timeframe=row["timeframe"],
            candle_open_time=int(row["signal_candle_open_time"]),
            candle_close_time=int(row["expected_execution_open_time"]) - 1,
            generated_at=int(row["created_at"]),
            action=SignalAction(row["action"]),
            reference_price=Decimal(str(row["benchmark_reference_price"])),
            reason="next_open execution of persisted pending action",
            requested_position_size=Decimal(row["quantity"]) if row.get("quantity") else None,
            indicator_snapshot=json.loads(row["indicator_snapshot_json"]) if row.get("indicator_snapshot_json") else None,
        )
        # The client order ID keys on the signal candle, so a restart cannot submit twice.
        cid_key = int(row["signal_candle_open_time"])
        self.event_store.resolve_pending_action(row["signal_id"], "EXECUTED")
        if signal.action == SignalAction.ENTER_LONG:
            self._submit_entry(signal, price, cid_key, self._latest_strategy_candle)
        else:
            self._submit_exit(signal, price, cid_key)

    # --- order submission ---------------------------------------------------

    def _submit_entry(self, signal: SignalEvent, price: Decimal, cid_candle_open_ms: int,
                      latest_candle: Optional[Candle]) -> Optional[Order]:
        """Submits a long entry at the given execution price."""
        allowed, health_reason = self.health.can_open_new_risk()
        if not allowed:
            logger.warning(f"Entry blocked by runtime health: {health_reason}")
            self.event_store.log_event("ORDER_BLOCKED", {"reason": health_reason, "signal": signal.to_dict()})
            return None

        fee_rate = Decimal(str(getattr(self.broker, "taker_fee", None) or self.manifest.get("fees", {}).get("taker", Decimal("0.0005"))))
        if self.slot_book:
            atr_value = (signal.indicator_snapshot or {}).get("atr")
            if atr_value is None or Decimal(str(atr_value)) <= 0:
                reason = f"Slot entry {signal.signal_id} has no valid ATR snapshot"
                self.event_store.record_incident("SLOT_ENTRY_UNPROTECTED", "HIGH", reason)
                logger.error(reason)
                return None
            # Reserve the entry fee outside SHADOW so all 12 slots fit available margin.
            slot_notional = self.slot_book.entry_notional(
                fee_rate if self.config.mode != "SHADOW" else Decimal("0")
            )
            raw_qty = slot_notional / price
        else:
            stake = self._stake_for_entry()
            allocated_stake = stake / (Decimal("1") + fee_rate) if fee_rate > Decimal("0") else stake
            raw_qty = allocated_stake / price
        target_qty = self.filters.round_quantity(raw_qty)

        guard_res = self.risk_guards.evaluate_entry(
            symbol=self.config.symbol,
            side=OrderSide.BUY,
            quantity=target_qty,
            price=price,
            position_manager=self.position_manager,
            latest_candle=latest_candle,
            has_active_gaps=self.gap_detector.is_desynced or self.kline_validator.has_active_mismatch,
            open_positions_count=self.slot_book.open_count if self.slot_book else None,
        )
        if not guard_res.passed:
            logger.warning(f"Entry order blocked by risk guard: {guard_res.reason}")
            self.event_store.log_event("ORDER_BLOCKED", {"reason": guard_res.reason, "signal": signal.to_dict()})
            return None

        cid = generate_client_order_id(
            symbol=self.config.symbol, timeframe=self.config.timeframe,
            candle_timestamp_ms=cid_candle_open_ms, action="BUY",
        )
        existing = self.order_manager.get_order_by_client_id(cid)
        if existing and existing.status in (OrderStatus.SUBMITTING, OrderStatus.NEW,
                                            OrderStatus.PARTIALLY_FILLED, OrderStatus.FILLED):
            logger.warning(f"Order {cid} already tracked with status {existing.status.value}. Skipping duplicate submission.")
            return existing

        telemetry = self.telemetry.begin(
            signal=signal, requested_qty=target_qty, reference_price=price,
            client_order_id=cid, expected_execution_time_ms=cid_candle_open_ms,
            side=OrderSide.BUY, reduce_only=False, position_side=self.exit_position_side,
        )

        now_ms = int(time.time() * 1000)
        intent_order = Order(
            client_order_id=cid, symbol=self.config.symbol, side=OrderSide.BUY,
            order_type=OrderType.MARKET, quantity=target_qty, price=price,
            status=OrderStatus.SUBMITTING, signal_id=signal.signal_id,
            created_at=now_ms, submitted_at=now_ms, position_side=self.exit_position_side,
        )
        self.order_manager.upsert_order(intent_order)
        self.event_store.save_order(intent_order)

        try:
            self.telemetry.mark(telemetry, "T1")
            order = self.broker.place_order(
                symbol=self.config.symbol, side=OrderSide.BUY, order_type=OrderType.MARKET,
                quantity=target_qty, price=price, client_order_id=cid,
                position_side=self.exit_position_side,
            )
            self.telemetry.mark(telemetry, "T2")
            self.order_manager.upsert_order(order)
            self.event_store.save_order(order)
        except BinanceUnknownOrderError as err:
            logger.error(f"Order submission returned UNKNOWN: {err}. Running bounded absence proof...")
            order = self._handle_unknown_submission(cid, err)
            if order is None:
                self.telemetry.finish(telemetry, order=None, block_reason="UNKNOWN_SUBMISSION")
                return None
        except Exception as err:
            logger.error(f"Order placement failed: {err}")
            self.order_manager.update_order_status(cid, OrderStatus.REJECTED, rejection_reason=str(err))
            self.event_store.record_incident("ORDER_REJECTED", "MEDIUM", str(err))
            self.telemetry.finish(telemetry, order=None, block_reason=f"REJECTED: {err}")
            return None

        self.event_store.log_event("ORDER_SUBMITTED", order.to_dict())

        if order.status in (OrderStatus.FILLED, OrderStatus.PARTIALLY_FILLED) and order.filled_quantity > Decimal("0"):
            fill_px = order.average_fill_price or price
            applied = self.fill_applier.apply_order_response(order, source="REST_RESPONSE")
            self.telemetry.mark(telemetry, "T3")
            if order.status == OrderStatus.FILLED:
                self.telemetry.mark(telemetry, "T4")
            status_str = "EXECUTED" if order.status == OrderStatus.FILLED else "PARTIALLY EXECUTED"
            logger.info(
                f">>> [BUY {status_str}] Mode={self.config.mode} | Bought {applied} {self.config.symbol} @ ${fill_px} | Total Position: {self.position_manager.quantity} {self.config.symbol} | CID={cid}"
            )
            snapshot = signal.indicator_snapshot or {}
            if self.slot_book:
                entry_fee = order.accumulated_fees
                if self.config.mode == "SHADOW":
                    entry_fee = applied * fill_px * fee_rate
                self.slot_book.add_fill(
                    signal, applied, fill_px,
                    signal.candle_open_time + TIMEFRAME_MAP_MS[self.config.timeframe],
                    entry_fee,
                )
            else:
                self.protective.on_entry_filled(order, fill_px, snapshot.get("atr"), signal.signal_id)
        self.telemetry.finish(telemetry, order=order)
        return order

    def _submit_exit(self, signal: SignalEvent, price: Decimal, cid_candle_open_ms: int,
                     benchmark_reference: Optional[Decimal] = None,
                     slot_id: Optional[str] = None) -> Optional[Order]:
        """Submits a reduce-only long exit at the given execution price."""
        requested_qty = signal.requested_position_size or self.position_manager.quantity
        exit_qty, exit_block = self._authoritative_exit_quantity(requested_qty)
        if exit_block:
            logger.warning(exit_block)
            self.event_store.log_event("EXIT_BLOCKED", {"reason": exit_block, "signal": signal.to_dict()})
            return None

        guard_res = self.risk_guards.evaluate_exit(
            symbol=self.config.symbol, side=OrderSide.SELL,
            quantity=exit_qty, position_manager=self.position_manager,
        )
        if not guard_res.passed:
            logger.warning(f"Exit order blocked: {guard_res.reason}")
            self.event_store.log_event("EXIT_BLOCKED", {"reason": guard_res.reason})
            return None

        cid = generate_client_order_id(
            symbol=self.config.symbol, timeframe=self.config.timeframe,
            candle_timestamp_ms=cid_candle_open_ms, action="SELL",
        )
        existing = self.order_manager.get_order_by_client_id(cid)
        if existing and existing.status in (OrderStatus.SUBMITTING, OrderStatus.NEW,
                                            OrderStatus.PARTIALLY_FILLED, OrderStatus.FILLED):
            logger.warning(f"Exit order {cid} already tracked. Skipping duplicate submission.")
            return existing

        telemetry = self.telemetry.begin(
            signal=signal, requested_qty=exit_qty,
            reference_price=benchmark_reference if benchmark_reference is not None else price,
            client_order_id=cid, expected_execution_time_ms=cid_candle_open_ms,
            side=OrderSide.SELL, reduce_only=True, position_side=self.exit_position_side,
        )

        now_ms = int(time.time() * 1000)
        intent_order = Order(
            client_order_id=cid, symbol=self.config.symbol, side=OrderSide.SELL,
            order_type=OrderType.MARKET, quantity=exit_qty, price=price,
            status=OrderStatus.SUBMITTING, signal_id=signal.signal_id,
            created_at=now_ms, submitted_at=now_ms,
            reduce_only=True, position_side=self.exit_position_side,
        )
        self.order_manager.upsert_order(intent_order)
        self.event_store.save_order(intent_order)

        try:
            self.telemetry.mark(telemetry, "T1")
            order = self.broker.place_order(
                symbol=self.config.symbol, side=OrderSide.SELL, order_type=OrderType.MARKET,
                quantity=exit_qty, price=price, client_order_id=cid,
                reduce_only=True, position_side=self.exit_position_side,
            )
            self.telemetry.mark(telemetry, "T2")
            self.order_manager.upsert_order(order)
            self.event_store.save_order(order)
        except BinanceUnknownOrderError as err:
            logger.error(f"Exit order returned UNKNOWN: {err}. Running bounded absence proof...")
            order = self._handle_unknown_submission(cid, err)
            if order is None:
                self.telemetry.finish(telemetry, order=None, block_reason="UNKNOWN_SUBMISSION")
                return None
        except Exception as err:
            logger.error(f"Exit order placement failed: {err}")
            self.order_manager.update_order_status(cid, OrderStatus.REJECTED, rejection_reason=str(err))
            self.event_store.record_incident("EXIT_ORDER_REJECTED", "MEDIUM", str(err))
            self.telemetry.finish(telemetry, order=None, block_reason=f"REJECTED: {err}")
            return None

        self.event_store.log_event("ORDER_SUBMITTED", order.to_dict())

        if order.status in (OrderStatus.FILLED, OrderStatus.PARTIALLY_FILLED) and order.filled_quantity > Decimal("0"):
            fill_px = order.average_fill_price or price
            order.reduce_only = True
            applied = self.fill_applier.apply_order_response(order, source="REST_RESPONSE")
            self.telemetry.mark(telemetry, "T3")
            if order.status == OrderStatus.FILLED:
                self.telemetry.mark(telemetry, "T4")
            status_str = "EXECUTED" if order.status == OrderStatus.FILLED else "PARTIALLY EXECUTED"
            logger.info(
                f">>> [SELL {status_str}] Mode={self.config.mode} | Sold {applied} {self.config.symbol} @ ${fill_px} | Total Position: {self.position_manager.quantity} {self.config.symbol} | Realized PnL: ${self.position_manager.realized_pnl} | CID={cid}"
            )
            if slot_id and self.slot_book:
                exit_fee = order.accumulated_fees
                if self.config.mode == "SHADOW":
                    exit_fee = applied * fill_px * Decimal(str(self.manifest["fees"]["taker"]))
                self.slot_book.close_fill(
                    slot_id, applied, fill_px, exit_fee, signal.reason
                )
            elif self.position_manager.is_flat:
                self.protective.on_exit_filled(order, signal.reason)
        self.telemetry.finish(telemetry, order=order, realized_pnl=self.position_manager.realized_pnl,
                              exit_reason=signal.reason)
        return order

    def _handle_unknown_submission(self, cid: str, err: Exception) -> Optional[Order]:
        """Resolves an ambiguous submission outcome without ever blindly resubmitting.

        Marks the order UNKNOWN (which blocks new entries for this symbol through the
        health state), then queries by client order ID with bounded backoff. Only the
        documented absence proof in AccountReconciler may conclude the order does not
        exist; anything short of that leaves entries blocked.
        """
        self.order_manager.update_order_status(cid, OrderStatus.UNKNOWN, rejection_reason=str(err))
        self.health.unknown_orders.add(cid)
        stored = self.order_manager.get_order_by_client_id(cid)
        if stored is not None:
            self.event_store.save_order(stored)

        outcome = self.reconciler.resolve_unknown_order(self.config.symbol, cid)
        if outcome["outcome"] == "FOUND":
            return self.order_manager.get_order_by_client_id(cid)

        severity = "HIGH"
        self.event_store.record_incident(
            "ORDER_UNKNOWN", severity,
            f"Order {cid} outcome {outcome['outcome']} after {outcome['queries']} queries; "
            f"new entries blocked until reconciled.",
        )
        self.alerter.alert(
            "ORDER_UNKNOWN", severity,
            f"Unresolved order {cid} ({outcome['outcome']}); new entries blocked.",
            outcome,
        )
        return None

    def _executable_exit_price(self, benchmark_reference: Decimal, market_price: Decimal) -> Decimal:
        """Price an exit can actually be filled at once the decision is known.

        SHADOW exists to prove decision parity against the benchmark, so it keeps the
        benchmark reference. Every mode that simulates or performs execution uses the
        market price available at decision time; a retrospective stop level would be a
        fill that was no longer obtainable.
        """
        if self.config.mode == "SHADOW":
            return benchmark_reference
        return market_price if market_price > Decimal("0") else benchmark_reference

    def _authoritative_exit_quantity(self, requested_qty: Decimal) -> tuple[Decimal, Optional[str]]:
        """Clamps an exit to what the exchange actually holds, right before submission.

        Local position state can be stale after a disconnect or a missed fill. An exit
        sized from stale state can exceed the real position, and without this clamp the
        surplus opens a reverse position. Returns (quantity, block_reason).
        """
        try:
            exch_side, exch_qty, _ = self.broker.get_position(self.config.symbol)
        except Exception as exc:
            return Decimal("0"), f"Exit blocked: exchange position unreadable ({exc})"

        if exch_qty <= Decimal("0") or exch_side is None:
            return Decimal("0"), "Exit blocked: exchange reports a flat position"
        if exch_side != OrderSide.BUY:
            return Decimal("0"), (
                f"Exit blocked: exchange position side {exch_side.value} does not match the "
                f"long exit this strategy emits"
            )

        qty = min(requested_qty, exch_qty)
        stepped = self.filters.round_quantity(qty)
        if stepped <= Decimal("0"):
            return Decimal("0"), (
                f"Exit blocked: authoritative quantity {qty} rounds to zero at step size "
                f"{self.filters.step_size}"
            )
        if stepped < requested_qty:
            logger.warning(
                f"Exit quantity clamped from {requested_qty} to {stepped} using authoritative "
                f"exchange position {exch_qty} (step {self.filters.step_size})."
            )
            self.event_store.record_incident(
                "EXIT_QUANTITY_CLAMPED", "MEDIUM",
                f"Requested exit {requested_qty} exceeded exchange position {exch_qty}; clamped to {stepped}.",
            )
        return stepped, None

    def _ensure_contiguous_history(self, candle: Candle) -> tuple[bool, List[Candle]]:
        """Closes any hole between the strategy history tail and the incoming bar.

        Indicators computed across a hole are wrong, so the incoming bar is not evaluated
        until the hole is provably closed: the refetched bars must be exactly the expected
        count at exactly the expected open times. Anything else stays fail-closed.

        Returns (history_is_contiguous, bars_that_must_be_replayed_in_order).
        """
        history = self.signal_engine.candle_history
        if not history:
            return True, []

        tf_ms = TIMEFRAME_MAP_MS[self.config.timeframe]
        expected_open = history[-1].open_time + tf_ms
        if candle.open_time <= expected_open:
            self.health.history_gap_open = False
            return True, []

        expected_opens = list(range(expected_open, candle.open_time, tf_ms))
        missing = len(expected_opens)

        try:
            filler = fetch_closed_klines(
                self.config.symbol, self.config.timeframe,
                start_ms=expected_open, end_ms=candle.open_time,
                base_url=self.rest_base_url,
            )
        except Exception as e:
            filler = []
            logger.error(f"HISTORY GAP HEAL FAILED for {missing} bar(s): {e}")

        actual_opens = [b.open_time for b in filler]
        healed = actual_opens == expected_opens

        if not healed:
            self.health.history_gap_open = True
            detail = (
                f"{missing} missing {self.config.timeframe} bar(s) before "
                f"{candle.open_datetime_utc}; refetched {len(filler)} "
                f"(expected opens {expected_opens[:3]}..., got {actual_opens[:3]}...)."
            )
            logger.error(f"[HISTORY GAP UNHEALED] {detail} Strategy evaluation blocked.")
            self.event_store.record_incident("HISTORY_GAP_UNHEALED", "HIGH", detail)
            self.alerter.alert("HISTORY_GAP", "HIGH", detail, {"symbol": self.config.symbol})
            return False, []

        # Provenance: these bars are REST klines, not aggTrade reconstructions. Tagging them
        # is what lets the replay below reach the strategy even while the trade stream has an
        # open hole — an exchange kline cannot be corrupted by a gap in our own feed. Without
        # the tag the desync gate discards every healed bar and the hole never closes.
        filler = [replace(bar, provenance="rest_kline") for bar in filler]
        for bar in filler:
            self.event_store.store_candle(bar)
            self.event_store.log_event("CANDLE_PROVENANCE", bar.to_dict())
        self._verify_filler_equivalence(filler)

        self.health.history_gap_open = False
        logger.warning(
            f"[HISTORY GAP] {missing} bar(s) missing before {candle.open_datetime_utc}; "
            f"refetched {len(filler)} from the exchange (healed); replaying their decisions."
        )
        self.event_store.record_incident(
            "HISTORY_GAP_HEALED", "MEDIUM",
            f"{missing} missing {self.config.timeframe} bar(s) before {candle.open_datetime_utc}; "
            f"refetched {len(filler)} REST klines (provenance recorded).",
        )
        return True, filler

    def _verify_filler_equivalence(self, filler: List[Candle]) -> None:
        """Compares REST filler bars against locally reconstructed candles where both exist.

        Mixing canonical aggTrade-derived candles with REST klines is only acceptable when
        the two agree; a disagreement is recorded rather than silently absorbed.

        The reconstruction buffer holds *1m* candles, so a strategy-timeframe filler bar is
        compared against the aggregate of the minutes it spans, and only when every one of
        those minutes was witnessed whole. Comparing a 15m kline against the single minute
        that shares its open time (as this once did) disagrees by construction and says
        nothing about provenance. Price tolerance is the validator's, so one reconstruction
        cannot be a match per-minute and a mismatch in aggregate.
        """
        recon = getattr(self.kline_validator, "reconstructed_candles", {})
        if not recon:
            return
        tol = getattr(self.kline_validator, "max_price_tolerance", Decimal("0"))
        minute_ms = TIMEFRAME_MAP_MS["1m"]
        for bar in filler:
            span = bar.close_time + 1 - bar.open_time
            minutes = [recon.get(bar.open_time + off) for off in range(0, span, minute_ms)]
            if any(m is None or not m.is_closed for m in minutes):
                # Partial local coverage cannot prove agreement or disagreement either way.
                continue
            local = (
                minutes[0].open,
                max(m.high for m in minutes),
                min(m.low for m in minutes),
                minutes[-1].close,
            )
            diffs = [abs(l - r) for l, r in zip(local, (bar.open, bar.high, bar.low, bar.close))]
            if max(diffs) > tol:
                self.event_store.record_incident(
                    "CANDLE_PROVENANCE_MISMATCH", "HIGH",
                    f"REST kline at {bar.open_datetime_utc} differs from the aggTrade "
                    f"reconstruction aggregated over its {len(minutes)} minute(s) "
                    f"(tolerance {tol}): rest=(O={bar.open} H={bar.high} L={bar.low} "
                    f"C={bar.close}) local=(O={local[0]} H={local[1]} L={local[2]} "
                    f"C={local[3]}) diffs={[str(d) for d in diffs]}",
                )

    def _authoritative_bar(self, candle: Candle) -> Optional[Candle]:
        """Refetches one strategy bar from the exchange when the local reconstruction is suspect.

        A trade-stream gap corrupts the bar Escanor aggregated itself, but not the exchange's
        own kline for the same window. Substituting it keeps the strategy evaluating on its
        real bar boundary instead of dropping the bar — which is what left a strategy-managed
        position with no working protection for as long as the gap stayed open.

        Returns None when the exchange bar cannot be obtained exactly, so the caller still
        fails closed rather than evaluating a bar nobody can vouch for.
        """
        tf_ms = TIMEFRAME_MAP_MS[self.config.timeframe]
        try:
            # ponytail: blocking REST call on the socket callback path, same tradeoff
            # _ensure_contiguous_history already makes — the decision for this bar cannot be
            # deferred off-thread and still be this bar's decision. Once per strategy bar, 20s
            # timeout. Prefetch klines on a timer if bar-close latency ever matters.
            bars = fetch_closed_klines(
                self.config.symbol, self.config.timeframe,
                start_ms=candle.open_time, end_ms=candle.open_time + tf_ms,
                base_url=self.rest_base_url,
            )
        except Exception as e:
            logger.error(f"Authoritative bar refetch failed for {candle.open_datetime_utc}: {e}")
            return None
        if len(bars) != 1 or bars[0].open_time != candle.open_time:
            logger.error(
                f"Authoritative bar refetch for {candle.open_datetime_utc} returned "
                f"{len(bars)} bar(s); evaluation stays blocked."
            )
            return None

        bar = replace(bars[0], provenance="rest_kline")
        # INSERT OR REPLACE: the corrupted local bar is overwritten, so the dashboard and any
        # later history read see the bar the strategy actually decided on.
        self.event_store.store_candle(bar)
        self.event_store.log_event("CANDLE_PROVENANCE", bar.to_dict())
        self.event_store.record_incident(
            "DESYNCED_BAR_SUBSTITUTED", "MEDIUM",
            f"Local reconstruction of {candle.open_datetime_utc} was desynced "
            f"({len(self.gap_detector.active_gaps)} active gap(s)); evaluated the exchange "
            f"kline instead (O={bar.open} H={bar.high} L={bar.low} C={bar.close}).",
        )
        return bar

    # --- market-data gap recovery ------------------------------------------

    def _schedule_gap_recovery(self, from_id: int, to_id: int) -> None:
        """Runs recovery off the websocket hot path when an event loop is available.

        Network waiting and bulk persistence inside the socket callback starve heartbeat
        processing, which is how a gap repair used to cause a second disconnect.
        """
        self._recovering.add((from_id, to_id))
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            self.recover_gap(from_id, to_id)
            return
        task = loop.create_task(self._recover_gap_async(from_id, to_id))
        self._recovery_tasks.add(task)

        def _on_recovery_done(t: asyncio.Task) -> None:
            self._recovery_tasks.discard(t)
            self._recovering.discard((from_id, to_id))
            if not t.cancelled() and t.exception() is not None:
                logger.error(
                    f"Gap recovery background task failed for IDs {from_id}-{to_id}: {t.exception()}",
                    exc_info=t.exception(),
                )
        task.add_done_callback(_on_recovery_done)

    async def _recover_gap_async(self, from_id: int, to_id: int) -> RecoveryResult:
        """Awaits recovery in a worker thread so the event loop keeps serving the socket."""
        return await asyncio.to_thread(self.recover_gap, from_id, to_id)

    def _abandon_gap(self, from_id: int, to_id: int, reason: str) -> None:
        """Clears a gap recovery cannot close, so the runtime stops being DATA_DESYNCED for it.

        The missing trades are unobtainable, so the alternative is not a correct engine: it is
        an engine that never evaluates another bar and never manages the position it is already
        holding. Candle integrity for the affected window comes from the exchange's own klines
        instead, via ``_authoritative_bar`` and history healing, plus 1m kline validation.
        """
        if not self.gap_detector.abandon_gap(from_id, to_id):
            return
        self.health.data_desynced = self.gap_detector.is_desynced
        detail = (
            f"aggTrade gap {from_id}-{to_id} abandoned: {reason}. Affected bars are evaluated "
            f"from exchange klines instead; aggTrade reconstruction for that window stays incomplete."
        )
        logger.warning(f"[GAP ABANDONED] {detail}")
        self.event_store.log_event("GAP_ABANDONED", {"from_id": from_id, "to_id": to_id, "reason": reason})
        self.event_store.record_incident("GAP_ABANDONED_TO_KLINES", "HIGH", detail)
        self.alerter.alert(
            "MARKET_DATA_GAP", "HIGH", detail,
            {"symbol": self.config.symbol, "from_id": from_id, "to_id": to_id},
        )
        if not self.gap_detector.active_gaps:
            self.alerter.recover("MARKET_DATA_GAP", f"No active gaps remain after abandoning {from_id}-{to_id}")

    def _retry_stalled_gaps(self) -> None:
        """Re-attempts every active gap that has no attempt in flight.

        Nothing else ever re-runs recovery for an already-active gap, so without this sweep a
        single failed attempt (one dropped connection) leaves the runtime DATA_DESYNCED until
        the process is restarted. Called once per closed minute, which also spaces the retries.
        """
        for gap in list(self.gap_detector.active_gaps):
            key = (gap.from_id, gap.to_id)
            if key in self._recovering:
                continue
            logger.info(
                f"Retrying recovery for active gap {gap.from_id}-{gap.to_id} "
                f"(attempt {gap.attempts + 1} of {MAX_GAP_RECOVERY_ATTEMPTS})."
            )
            self._schedule_gap_recovery(gap.from_id, gap.to_id)

    def recover_gap(self, from_id: int, to_id: int) -> RecoveryResult:
        """Recovers a gap exactly, or stays fail-closed with the remaining interval recorded.

        Idempotent: IDs already durably stored are skipped, and re-running the same range
        neither refetches them nor re-applies them to candles.
        """
        self._recovering.add((from_id, to_id))
        try:
            return self._recover_gap_inner(from_id, to_id)
        finally:
            self._recovering.discard((from_id, to_id))

    def _recover_gap_inner(self, from_id: int, to_id: int) -> RecoveryResult:
        already = self.event_store.get_aggtrade_ids(from_id, to_id)
        result = self.gap_recovery.recover(from_id, to_id, already_have=already)

        if result.trades:
            self.event_store.store_aggtrades_batch(result.trades)
            self._feed_recovered_trades(result.trades)

        if result.complete:
            self.gap_detector.mark_gap_recovered(from_id, to_id)
            self.event_store.log_event("GAP_RECOVERED", result.to_dict())
            logger.info(
                f"Gap recovery COMPLETE for IDs {from_id}-{to_id}: "
                f"{len(result.trades)} trade(s) over {result.pages} page(s)."
            )
            if not self.gap_detector.active_gaps:
                self.health.data_desynced = False
                self.alerter.recover("MARKET_DATA_GAP", f"Gap {from_id}-{to_id} recovered exactly")
        else:
            # Fail closed *for a bounded number of attempts*: the gap stays active and the
            # runtime stays DATA_DESYNCED while recovery still has a chance. Once the attempts
            # are spent the trades are gone for good, and keeping the flag latched would only
            # guarantee the engine never evaluates another bar.
            attempts = self.gap_detector.record_attempt(from_id, to_id)
            self.event_store.log_event("GAP_RECOVERY_INCOMPLETE", result.to_dict())
            self.event_store.record_incident(
                "GAP_RECOVERY_INCOMPLETE", "HIGH",
                f"Gap {from_id}-{to_id} not fully recovered on attempt {attempts} of "
                f"{MAX_GAP_RECOVERY_ATTEMPTS}; remaining={result.remaining}; "
                f"reason={result.reason}; rejected={result.rejected}",
            )
            if attempts >= MAX_GAP_RECOVERY_ATTEMPTS:
                self._abandon_gap(
                    from_id, to_id,
                    f"unrecoverable after {attempts} attempts ({result.reason})",
                )
            else:
                self.gap_detector.narrow_gap(from_id, to_id, result.remaining)
                self.health.data_desynced = True
                self.alerter.alert(
                    "MARKET_DATA_GAP", "HIGH",
                    f"Market data gap {from_id}-{to_id} unresolved; remaining {result.remaining}.",
                    result.to_dict(),
                )
        return result

    def _feed_recovered_trades(self, trades: List[AggTrade]) -> None:
        """Feeds recovered trades through candle construction in exact event order."""
        for trade in sorted(trades, key=lambda t: (t.trade_time, t.agg_trade_id)):
            self._process_slot_barriers(trade)
            closed_1m = self.candle_builder.add_trade(trade)
            if not closed_1m:
                continue
            self.event_store.store_candle(closed_1m)
            self.kline_validator.register_reconstructed_candle(closed_1m)
            closed_strategy = self.resampler.add_1m_candle(closed_1m)
            if closed_strategy:
                self._on_strategy_candle_closed(closed_strategy)

    def on_user_order_trade_update(self, data: Dict[str, Any]) -> None:
        """Handles ORDER_TRADE_UPDATE from the Binance user data stream.

        Delegates to the shared delta-based path: `z` is a cumulative quantity, so only the
        difference against what was already applied moves the position. Duplicate, stale and
        out-of-order events change nothing.
        """
        self.health.beat("private_stream")
        self.fill_applier.apply_stream_event(data)

    def on_user_stream_lifecycle(self, kind: str, payload: Dict[str, Any]) -> None:
        """Records private-stream lifecycle transitions and keeps health freshness current.

        The payload never carries an API key or listen key: only the event kind, host and
        counters reach the log and the audit trail.
        """
        self.event_store.log_event(f"USER_STREAM_{kind}", payload)
        if kind in ("CONNECTED", "LISTEN_KEY_CREATED", "LISTEN_KEY_RENEWED"):
            self.health.beat("private_stream", "OK", kind)
            self.alerter.recover("PRIVATE_STREAM_DOWN", f"Private stream {kind.lower()}")
        elif kind in ("DISCONNECTED", "RECONNECTING", "LISTEN_KEY_EXPIRED",
                      "LISTEN_KEY_RENEWAL_FAILED", "TERMINAL_FAILURE"):
            self.health.beat("private_stream", "DEGRADED", kind)
            self.alerter.alert(
                "PRIVATE_STREAM_DOWN", "HIGH",
                f"Private execution stream {kind}; account events may be missing.",
                payload,
            )

    def on_user_account_update(self, data: Dict[str, Any]) -> None:
        """Handles ACCOUNT_UPDATE from Binance User Data Stream."""
        self.event_store.log_event("USER_STREAM_ACCOUNT_UPDATE", data)
        self.reconciler.reconcile_position()
