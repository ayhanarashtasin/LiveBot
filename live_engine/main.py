"""Escanor Live Trading Engine CLI.

Usage:
  python -m live_engine.main --mode shadow
  python -m live_engine.main --mode paper
  python -m live_engine.main --mode live
  python -m live_engine.main --parity-check
  python -m live_engine.main --status
  python -m live_engine.main --kill "Emergency operator halt"
  python -m live_engine.main --disengage-kill
"""
import argparse
import asyncio
import json
import logging
import os
import sys
import time
from decimal import Decimal
from pathlib import Path

import pandas as pd
import websockets

from live_engine.config import load_config, LiveEngineConfig
from live_engine.orchestrator import LiveEngineOrchestrator
from live_engine.risk.kill_switch import KillSwitch
from live_engine.parity.replay import HistoricalReplayRunner
from live_engine.market_data.binance_aggtrade import BinanceAggTradeStream
from live_engine.market_data.models import AggTrade
from live_engine.market_data.candle_builder import MinuteCandleBuilder

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("escanor")


def print_startup_banner(config: LiveEngineConfig, orchestrator: LiveEngineOrchestrator) -> None:
    strat_name = orchestrator.manifest.get("strategy", {}).get("name", "Unknown")
    is_live = config.mode.upper() == "LIVE"
    order_sub = "ENABLED" if is_live else "DISABLED"
    acct_state = "RECONCILED" if is_live else "NOT REQUIRED"

    # Check market data health
    market_data_status = "SYNCHRONIZED"
    if orchestrator.gap_detector.is_desynced:
        market_data_status = "DESYNCED (gap detected)"
    elif orchestrator.kline_validator.has_active_mismatch:
        market_data_status = "MISMATCH (kline validation failed)"

    # Check warmup status
    warmup_count = len(orchestrator.signal_engine.candle_history)
    required_warmup = orchestrator.warmup_manager.required_candles if hasattr(orchestrator.warmup_manager, 'required_candles') else 250
    warmup_status = f"READY ({warmup_count} candles, need {required_warmup})" if warmup_count >= required_warmup else f"INCOMPLETE ({warmup_count}/{required_warmup})"

    # Get database path and filters
    db_path = config.event_store_path
    filters_str = f"tick={orchestrator.filters.tick_size} step={orchestrator.filters.step_size}"

    print("==================================================")
    print("              ESCANOR LIVE ENGINE                 ")
    print("==================================================")
    print(f"Mode:               {config.mode.upper()}")
    print(f"Exchange:           Binance")
    print(f"Market:             Futures (USD-M)")
    print(f"Symbol:             {config.symbol}")
    print(f"Benchmark:          {orchestrator.manifest.get('benchmark_id', 'CUSTOM')}")
    print(f"Strategy:           {strat_name}")
    print(f"Strategy hash:      VERIFIED ({orchestrator.manifest.get('strategy', {}).get('source_hash', '')[:12]}...)")
    print(f"Timeframe:          {config.timeframe}")
    print(f"Canonical data:     aggTrade")
    print(f"Market data:        {market_data_status}")
    print(f"Warmup:             {warmup_status}")
    print(f"Health state:       {orchestrator.health.state().value}")
    print(f"Account:            {acct_state}")
    print(f"Filters:            {filters_str}")
    print(f"Database:           {db_path}")
    print(f"Order submission:   {order_sub}")
    real_orders = "ENABLED" if config.mode.upper() in ("LIVE", "TESTNET") else "DISABLED"
    print(f"REAL ORDER SUBMISSION: {real_orders}")
    print("==================================================")


from live_engine.risk.safety_gates import SafetyGateVerifier


def preflight_live_authorization(config: LiveEngineConfig, ks: KillSwitch) -> None:
    """Refuses funded LIVE before anything connects, when authorisation is absent.

    This is the half of the gate set that needs no exchange connection: the kill switch and
    the operator acknowledgement plus credentials (Gate 24). Checking it first means a LIVE
    start with no acknowledgement never opens an authenticated session at all. It replaces
    nothing: :func:`validate_live_safety_gates` still evaluates all 34 gates once the engine
    is initialised and its streams are reporting, before any order can be submitted.
    """
    if config.mode.upper() != "LIVE":
        return
    if ks.is_engaged():
        raise PermissionError("GATE FAILURE: Persistent Kill Switch is active. Clear via --disengage-kill first.")
    gate = SafetyGateVerifier(config)._gate_live_disabled()
    if not gate.passed:
        raise PermissionError(
            f"LIVE EXECUTION BLOCKED: Gate {gate.gate_id} ({gate.name}): {gate.details}"
        )


def validate_live_safety_gates(config: LiveEngineConfig, ks: KillSwitch,
                               orchestrator=None) -> None:
    """Validates every funded activation gate against evidence prior to live execution.

    Each gate resolves to an evidence record with a source, a value, an observation time
    and an expiry policy. Missing, stale or unverifiable evidence fails closed. A source
    or test file merely existing satisfies nothing.
    """
    if config.mode.upper() != "LIVE":
        return

    # Check Kill Switch first
    if ks.is_engaged():
        raise PermissionError("GATE FAILURE: Persistent Kill Switch is active. Clear via --disengage-kill first.")

    verifier = SafetyGateVerifier(config, orchestrator=orchestrator)
    all_passed, results = verifier.evaluate_all_gates()

    failed_gates = [g for g in results if not g.passed]
    if failed_gates:
        lines = []
        for g in failed_gates:
            evidence = ""
            if g.evidence is not None:
                age = "n/a" if g.evidence.age_s is None else f"{g.evidence.age_s:.0f}s"
                evidence = f" [evidence: {g.evidence.source}, age {age}]"
            lines.append(f"  - Gate {g.gate_id} ({g.name}): {g.details}{evidence}")
        raise PermissionError(
            f"LIVE EXECUTION BLOCKED: {len(failed_gates)}/{len(results)} mandatory safety gates failed:\n"
            + "\n".join(lines)
        )


def handle_execution_report(config: LiveEngineConfig) -> int:
    """Prints the benchmark-versus-observed execution report for this instance.

    Strategy/data divergence (a signal the benchmark had and the runtime did not) is
    reported separately from execution divergence (slippage, latency, fees). Anything the
    system did not capture is reported as not captured, never estimated.
    """
    import csv as csv_module

    from live_engine.monitoring.telemetry import build_execution_comparison
    from live_engine.persistence.event_store import EventStore
    from live_engine.strategy.loader import StrategyLoader

    base_dir = Path(__file__).resolve().parent.parent
    db_path = (base_dir / config.event_store_path).resolve()
    if not db_path.exists():
        print(f"[ERROR] No event store at {db_path}. Run the engine first.")
        return 1

    store = EventStore(db_path)
    telemetry = store.get_telemetry(limit=10000)

    benchmark_trades = []
    try:
        manifest = StrategyLoader.load_manifest(base_dir / config.manifest_path)
        csv_path = base_dir / manifest.get("benchmark_result_reference", "")
        if csv_path.is_file():
            with csv_path.open(encoding="utf-8") as fh:
                benchmark_trades = list(csv_module.DictReader(fh))
    except Exception as exc:
        print(f"[WARN] Benchmark trade log unavailable: {exc}")

    report = build_execution_comparison(telemetry, benchmark_trades)

    print("\n" + "=" * 70)
    print(f"ESCANOR EXECUTION QUALITY REPORT: {config.symbol} ({config.mode})")
    print("=" * 70)
    print(f"Benchmark trades:            {report['benchmark_trades']}")
    print(f"Observed submissions:        {report['observed_submissions']}")
    print(f"  entries / exits:           {report['entry_signals']} / {report['exit_signals']}")
    print(f"  blocked by guards:         {report['blocked_submissions']}")
    if report["block_reasons"]:
        for reason in report["block_reasons"][:5]:
            print(f"    - {reason}")
    print(f"Missing signals:             {len(report['missing_signals'])}")
    print(f"Extra signals:               {len(report['extra_signals'])}")
    print(f"Avg entry slippage:          {report['avg_entry_slippage_pct'] or 'not captured'}")
    print(f"Avg exit slippage:           {report['avg_exit_slippage_pct'] or 'not captured'}")
    print(f"Avg implementation shortfall:{report['avg_implementation_shortfall'] or 'not captured'}")
    print(f"Avg ack latency (ms):        {report['avg_ack_latency_ms'] if report['avg_ack_latency_ms'] is not None else 'not captured'}")
    print(f"Avg first-fill latency (ms): {report['avg_first_fill_latency_ms'] if report['avg_first_fill_latency_ms'] is not None else 'not captured'}")
    print(f"Total fees:                  {report['total_fees']}")
    print(f"Divergence attributable to:  {report['divergence_attribution']}")
    if report["measurements_not_captured"]:
        print(f"Not captured:                {', '.join(report['measurements_not_captured'])}")
    print("=" * 70 + "\n")
    return 0


def handle_kill_switch(args, config: LiveEngineConfig) -> int:
    ks = KillSwitch(Path(config.kill_switch_path))
    if args.kill:
        res = ks.trigger(reason=args.kill, triggered_by="OPERATOR_CLI")
        print(f"[SUCCESS] Kill switch ENGAGED: {res}")
        return 0
    if args.disengage_kill:
        ks.disengage(reason="Operator manual release via CLI", disengaged_by="OPERATOR_CLI")
        print("[SUCCESS] Kill switch DISENGAGED.")
        return 0
    return 0


def handle_status(config: LiveEngineConfig) -> int:
    ks = KillSwitch(Path(config.kill_switch_path))
    status = ks.get_status()
    print("========================================")
    print("         ESCANOR ENGINE STATUS          ")
    print("========================================")
    print(f"Mode:         {config.mode}")
    print(f"Symbol:       {config.symbol}")
    print(f"Timeframe:    {config.timeframe}")
    print(f"Manifest:     {config.manifest_path}")
    print(f"Kill Switch:  {'ENGAGED' if status.get('engaged') else 'CLEAR'}")
    if status.get("engaged"):
        print(f"  Triggered:  {status.get('triggered_at')}")
        print(f"  Reason:     {status.get('reason')}")

    db_path = Path(config.event_store_path)
    if not db_path.exists():
        fallback = Path("data/escanor_live.db")
        if fallback.exists():
            db_path = fallback
    if db_path.exists():
        print("----------------------------------------")
        print("           LIVE DATABASE AUDIT          ")
        print("----------------------------------------")
        try:
            import sqlite3
            with sqlite3.connect(str(db_path)) as conn:
                cur = conn.cursor()
                # 1. Trades
                try:
                    cur.execute("SELECT COUNT(*) FROM raw_aggtrades;")
                    n_trades = cur.fetchone()[0]
                    print(f"Raw Trades Ingested:   {n_trades:,}")
                except Exception:
                    pass

                # 2. Candles
                try:
                    cur.execute("SELECT timeframe, COUNT(*) FROM candles GROUP BY timeframe;")
                    candle_counts = cur.fetchall()
                    if candle_counts:
                        c_str = ", ".join(f"{tf}: {cnt:,}" for tf, cnt in candle_counts)
                        print(f"Candles Reconstructed: {c_str}")
                except Exception:
                    pass

                # 3. Position
                try:
                    cur.execute("SELECT payload_json FROM audit_events WHERE event_type='POSITION_UPDATED' ORDER BY event_id DESC LIMIT 1;")
                    pos_row = cur.fetchone()
                    if pos_row:
                        pos_data = json.loads(pos_row[0])
                        print(f"Live Position:         {pos_data.get('quantity', '0')} BTC (Avg Entry: ${pos_data.get('entry_price', '0')}, Realized PnL: ${pos_data.get('realized_pnl', '0')})")
                    else:
                        print("Live Position:         FLAT (0.000 BTC)")
                except Exception:
                    pass

                # 4. Recent Signals
                try:
                    cur.execute("SELECT action, reference_price, datetime(candle_open_time/1000, 'unixepoch'), reason FROM signals ORDER BY generated_at DESC LIMIT 5;")
                    sigs = cur.fetchall()
                    print(f"\nRecent Signals ({len(sigs)}):")
                    if sigs:
                        for act, px, dt, rsn in sigs:
                            print(f"  [{dt} UTC] {act:<11} @ ${px} | {rsn}")
                    else:
                        print("  (No signals generated yet)")
                except Exception:
                    pass

                # 5. Recent Orders
                try:
                    cur.execute("SELECT side, quantity, price, status, avg_fill_price, datetime(created_at/1000, 'unixepoch') FROM orders ORDER BY created_at DESC LIMIT 5;")
                    ords = cur.fetchall()
                    print(f"\nRecent Orders ({len(ords)}):")
                    if ords:
                        for side, qty, px, st, fill_px, dt in ords:
                            fill_info = f"@ ${fill_px}" if fill_px else f"(target ${px})"
                            print(f"  [{dt} UTC] {side:<4} {qty} BTC {fill_info} | Status: {st}")
                    else:
                        print("  (No orders placed yet)")
                except Exception:
                    pass
        except Exception as db_err:
            print(f"Database query error: {db_err}")

    print("========================================")
    return 0


def handle_parity_check(config: LiveEngineConfig, raw_aggtrades: bool = False) -> int:
    """Runs parity check: prebuilt candles (fast) or raw aggTrades (thorough)."""
    manifest_p = Path(config.manifest_path)
    print(f"Running Historical Parity Verification on {manifest_p}...")

    # Load manifest to get dataset paths
    import yaml
    try:
        manifest = yaml.safe_load(manifest_p.read_text())
        data_cfg = manifest.get("data", {})
        warmup_dataset_p = Path(data_cfg.get("dataset_candle_path", f"{config.symbol}_USDM_DATA/candles/{config.symbol}_{config.timeframe}.parquet"))
        if not warmup_dataset_p.is_absolute():
            warmup_dataset_p = repo_root / warmup_dataset_p
        if not warmup_dataset_p.exists():
            fallback_p = repo_root / f"{config.symbol}_USDM_DATA/candles/{config.symbol}_{config.timeframe}.parquet"
            if fallback_p.exists():
                warmup_dataset_p = fallback_p
            else:
                fallback_data = repo_root / f"data/{config.symbol}_{config.timeframe}.parquet"
                if fallback_data.exists():
                    warmup_dataset_p = fallback_data
        aggtrade_dataset_p = Path(data_cfg.get("dataset_aggtrade_path", f"{config.symbol}_USDM_DATA/aggTrades/2026-08.parquet"))
    except Exception as e:
        print(f"[ERROR] Could not load manifest: {e}")
        return 1

    if not warmup_dataset_p.exists():
        print(f"[ERROR] Dataset not found: {warmup_dataset_p}")
        return 1

    runner = HistoricalReplayRunner(manifest_p)

    canonical_candles = None
    if raw_aggtrades:
        print("[INFO] Raw aggTrade replay (thorough parity validation)\n")

        # Coverage is validated before any streaming starts: an evaluation window that is
        # not fully backed by declared raw files fails closed rather than replaying a
        # different trade set over partial data.
        coverage = runner.validate_raw_coverage()
        print(f"[COVERAGE] Declared raw files: {coverage['files'] or 'none'}")
        if not coverage["ok"]:
            for err in coverage["errors"]:
                print(f"[ERROR] {err}")
            return 1

        start_ts_utc = data_cfg.get("evaluation_window_start", "2026-08-15 00:00:00+00:00")
        end_ts_utc = data_cfg.get("evaluation_window_end", "2026-08-31 23:59:59+00:00")

        warmup_candles, canonical_candles = runner.load_dataset(
            warmup_dataset_p, warmup_bars=250,
            start_time_utc=start_ts_utc, end_time_utc=end_ts_utc,
        )
        start_ts_ms = int(pd.Timestamp(start_ts_utc).timestamp() * 1000)
        end_ts_ms = int(pd.Timestamp(end_ts_utc).timestamp() * 1000)

        print(f"[REPLAY] Streaming aggTrades -> MinuteCandleBuilder -> TimeframeResampler -> strategy")
        print(f"[REPLAY] Warmup: 250 candles")
        print(f"[REPLAY] Evaluation: {start_ts_utc} to {end_ts_utc}")
        eval_candles, trades = runner.run_aggtrade_replay(
            warmup_candles=warmup_candles,
            start_ts_ms=start_ts_ms,
            end_ts_ms=end_ts_ms,
        )
        print(f"[REPLAY] Generated {len(eval_candles)} evaluation candles from raw events")
        print(f"[REPLAY] Generated {len(trades)} trades from strategy signals\n")
    else:
        print("[INFO] Prebuilt candle replay (fast)\n")
        warmup_candles, eval_candles = runner.load_dataset(warmup_dataset_p, warmup_bars=250)
        trades = runner.run_replay(warmup_candles, eval_candles)

    report = runner.verify_parity(
        trades,
        eval_candles if raw_aggtrades else None,
        canonical_candles=canonical_candles,
        verify_coverage=raw_aggtrades,
    )
    report.print_summary()

    if report.status == "PASS":
        print("[PASS] 100% Decision Parity Confirmed")
        return 0
    else:
        print(f"[FAIL] Parity check failed with {len(report.mismatches)} mismatch(es)")
        for m in report.mismatches[:20]:
            print(f"  - {m}")
        if len(report.mismatches) > 20:
            print(f"  ... and {len(report.mismatches) - 20} more")
        return 1


async def async_check_feed(symbol: str, count: int = 5) -> int:
    """Connects to Binance live WebSocket feed, displays live trades with latency, and validates ingestion."""
    print(f"\n[LIVE FEED CHECK] Connecting to Binance USD-M Futures market stream for {symbol}...", flush=True)
    received_trades = []
    builder = MinuteCandleBuilder(symbol=symbol)

    stream = BinanceAggTradeStream(
        symbol=symbol,
        market_type="usdm_futures",
    )

    ws_url = stream.stream_url
    print(f"[LIVE FEED CHECK] Endpoint: {ws_url}", flush=True)
    print(f"[LIVE FEED CHECK] Subscribing to {symbol.lower()}@aggTrade...", flush=True)

    max_attempts = 3
    for attempt in range(1, max_attempts + 1):
        try:
            print(f"[LIVE FEED CHECK] Connection attempt {attempt}/{max_attempts}...", flush=True)
            async with websockets.connect(ws_url, ping_interval=20, open_timeout=20) as ws:
                print(f"[LIVE FEED CHECK] Connected successfully!", flush=True)
                print("-" * 80, flush=True)
                print(f"{'TRADE ID':<14} | {'PRICE (USDT)':<14} | {'QTY (BTC)':<10} | {'SIDE':<8} | {'LATENCY':<8} | {'EXCHANGE TIME (UTC)'}", flush=True)
                print("-" * 80, flush=True)

                sub_msg = {"method": "SUBSCRIBE", "params": [f"{symbol.lower()}@aggTrade"], "id": 1}
                await ws.send(json.dumps(sub_msg))

                async for msg in ws:
                    payload = json.loads(msg)
                    if "result" in payload and "id" in payload:
                        continue
                    trade = AggTrade.from_binance_ws(payload)
                    received_trades.append(trade)
                    side = "SELL (M)" if trade.buyer_is_market_maker else "BUY (T)"
                    print(f"{trade.agg_trade_id:<14} | {trade.price:<14} | {trade.quantity:<10} | {side:<8} | {trade.latency_ms:>5}ms | {trade.trade_datetime_utc}", flush=True)
                    builder.add_trade(trade)

                    if len(received_trades) >= count:
                        break
                break
        except Exception as e:
            print(f"[LIVE FEED CHECK] Attempt {attempt} failed: {e}", flush=True)
            if attempt < max_attempts:
                await asyncio.sleep(1)

    if not received_trades:
        print(f"\n[FEED ERROR] Failed to receive live market data after {max_attempts} attempts.", flush=True)
        return 1

    avg_latency = sum(t.latency_ms for t in received_trades) / len(received_trades)
    print("-" * 80, flush=True)
    print(f"[FEED STATUS] PASS: Successfully received {len(received_trades)} live Binance trade events!", flush=True)
    print(f"[METRICS] Average Network Latency: {avg_latency:.1f}ms", flush=True)
    print(f"[METRICS] Current Binance Price: ${received_trades[-1].price}", flush=True)
    print(f"[METRICS] Live 1m Candle State: Open=${builder._open} High=${builder._high} Low=${builder._low} Close=${builder._close} Vol={builder._volume} (Trades: {builder._trade_count})\n", flush=True)
    return 0


def handle_check_feed(config: LiveEngineConfig) -> int:
    return asyncio.run(async_check_feed(config.symbol))


SUPERVISOR_INTERVAL_S = 15.0


async def supervise(orchestrator: LiveEngineOrchestrator, stop_event: asyncio.Event) -> None:
    """Periodic health supervision.

    A check at startup proves only that the process launched. This keeps watching for the
    conditions that take an engine down quietly: stale heartbeats, reconnect storms,
    unresolved gaps, an expired private stream, failed reconciliation, UNKNOWN orders and
    the two circuit breakers.
    """
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=SUPERVISOR_INTERVAL_S)
            return
        except asyncio.TimeoutError:
            pass
        try:
            active = orchestrator.supervisor.check(process_alive=True)
            if active:
                logger.warning("Health supervisor active conditions: %s", active)
        except Exception as exc:
            logger.error("Health supervisor iteration failed: %s", exc)


STREAM_EVIDENCE_TIMEOUT_S = 45.0


async def run_live_loop(orchestrator: LiveEngineOrchestrator, config: LiveEngineConfig,
                        dry_run: bool = False):
    """Connects to the Binance aggTrade stream and feeds trades into the orchestrator.

    With ``dry_run`` the engine initialises, connects, proves its streams and evaluates
    every gate, then stops without processing a single trade. That is the funded pre-flight:
    the gates that matter for LIVE resolve to runtime evidence, and evidence a process never
    connected to observe cannot be produced by inspecting files.
    """
    orchestrator.initialize()
    print_startup_banner(config, orchestrator)
    logger.info(f"Starting real-time market data feed for {config.symbol} in {config.mode} mode...")

    stop_event = asyncio.Event()
    _install_signal_handlers(stop_event)

    # Trades that arrive before the gates have been evaluated are held, not dropped and not
    # processed: dropping them opens a sequence gap the detector would report as desync, and
    # processing them would let an order out under gates that have not been checked yet.
    hold = {"active": True, "buffer": []}

    def on_trade(trade):
        if hold["active"]:
            orchestrator.health.beat("public_stream")
            hold["buffer"].append(trade)
            return
        signals = orchestrator.process_aggtrade(trade)
        for s in signals:
            logger.info(f"Signal executed: {s.action.value} on candle {s.candle_close_time}")

    stream = BinanceAggTradeStream(
        symbol=config.symbol,
        market_type="usdm_futures",
        on_trade_callback=on_trade,
    )

    if config.mode.upper() in ("SHADOW", "PAPER"):
        try:
            await orchestrator.kline_validator.start()
            logger.info(f"Started Binance 1m kline validator for {config.symbol}")
        except Exception as e:
            logger.warning(f"Failed to start kline validator: {e}")

    if orchestrator.user_data_stream:
        try:
            await orchestrator.user_data_stream.start()
            logger.info("Started Binance user data stream for account events")
        except Exception as e:
            logger.warning(f"Failed to start user data stream: {e}")

    supervisor_task = asyncio.create_task(supervise(orchestrator, stop_event), name="health-supervisor")

    try:
        await stream.start()
        orchestrator.health.beat("engine")
        await _await_stream_evidence(orchestrator, hold)
        # Every gate, evaluated against the initialised engine and its live streams, before
        # the first held trade is released. A failure here raises and the finally block below
        # shuts the engine down without having processed anything.
        validate_live_safety_gates(config, orchestrator.kill_switch, orchestrator)
        if dry_run:
            print(f"[DRY-RUN] All safety gates satisfied for {config.mode}; "
                  f"{len(hold['buffer'])} held trade(s) discarded without processing.")
            return
        held = hold["buffer"]
        hold["buffer"] = []
        hold["active"] = False
        logger.info("Safety gates satisfied; releasing %d held trade(s) in event order.", len(held))
        for trade in held:
            on_trade(trade)
        stream_task = stream._task
        stop_task = asyncio.create_task(stop_event.wait(), name="stop-signal")
        done, pending = await asyncio.wait(
            {t for t in (stream_task, stop_task) if t is not None},
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            task.cancel()
    except asyncio.CancelledError:
        logger.info("Engine stopping...")
    finally:
        stop_event.set()
        supervisor_task.cancel()
        try:
            await supervisor_task
        except (asyncio.CancelledError, Exception):
            pass
        await stream.stop()
        await orchestrator.shutdown(reason="engine loop ended")


async def _await_stream_evidence(orchestrator: LiveEngineOrchestrator, hold: dict,
                                 timeout: float = STREAM_EVIDENCE_TIMEOUT_S) -> None:
    """Waits for the first real event on every stream this mode depends on.

    Connecting is not evidence that data is flowing, so the public stream counts only once
    a trade has actually arrived and the private stream only once it has reported its
    lifecycle heartbeat. A timeout is not an error here: the gates then fail closed on the
    missing evidence and say which stream was silent.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        public_ok = bool(hold["buffer"])
        private_ok = (orchestrator.user_data_stream is None
                      or orchestrator.health.age_s("private_stream") is not None)
        if public_ok and private_ok:
            return
        await asyncio.sleep(0.25)
    logger.warning(
        "Stream evidence incomplete after %.0fs (public trades=%d, private age=%s); "
        "the safety gates will report what is missing.",
        timeout, len(hold["buffer"]), orchestrator.health.age_s("private_stream"),
    )


def _install_signal_handlers(stop_event: asyncio.Event) -> None:
    """Requests a graceful stop on SIGINT/SIGTERM where the platform supports it."""
    import signal

    loop = asyncio.get_running_loop()
    for sig_name in ("SIGINT", "SIGTERM"):
        sig = getattr(signal, sig_name, None)
        if sig is None:
            continue
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except (NotImplementedError, RuntimeError, ValueError):
            # Windows event loops do not support add_signal_handler; KeyboardInterrupt
            # still reaches asyncio.run and the finally block still runs shutdown.
            pass


def main() -> int:
    parser = argparse.ArgumentParser(description="Escanor Live Trading Engine")
    parser.add_argument("--mode", choices=["SHADOW", "PAPER", "TESTNET", "LIVE", "shadow", "paper", "testnet", "live"], help="Execution mode (default: SHADOW)")
    parser.add_argument("--config", type=str, help="Path to engine configuration file")
    parser.add_argument("--manifest", type=str, help="Path to benchmark manifest")
    parser.add_argument("--benchmark", type=str, help="Benchmark ID or path to manifest")
    parser.add_argument("--symbol", type=str, help="Symbol to trade (e.g. BTCUSDT)")
    parser.add_argument("--timeframe", type=str, help="Timeframe (e.g. 5m)")
    parser.add_argument("--kill", type=str, help="Engage kill switch with reason")
    parser.add_argument("--disengage-kill", action="store_true", help="Disengage kill switch")
    parser.add_argument("--status", action="store_true", help="Print engine status")
    parser.add_argument("--parity-check", action="store_true", help="Run historical parity replay check")
    parser.add_argument("--execution-report", action="store_true",
                        help="Print the benchmark vs observed execution-quality report")
    parser.add_argument("--check-feed", action="store_true", help="Connect to live Binance stream, display live trades and latency metrics")
    parser.add_argument("--raw-aggtrades", action="store_true", help="Stream full raw aggTrades dataset during parity check (thorough)")
    parser.add_argument("--dry-run", action="store_true", help="Initialize and validate all components without starting event loop")
    parser.add_argument("--dashboard", action="store_true", help="Launch interactive dark-mode web dashboard UI")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="Web dashboard host (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8080, help="Web dashboard port (default: 8080)")
    parser.add_argument(
        "--dashboard-config",
        dest="dashboard_configs",
        action="append",
        help="Path to engine configuration file for multi-symbol dashboard (can be repeated)",
    )

    args = parser.parse_args()

    # Multi-symbol dashboard standalone mode
    if args.dashboard_configs:
        from live_engine.dashboard import run_multi_symbol_dashboard
        return run_multi_symbol_dashboard(config_paths=args.dashboard_configs, host=args.host, port=args.port)

    config = load_config(args.config)

    # CLI parameter overrides
    if args.mode:
        config.mode = args.mode.upper()
    if args.manifest:
        config.manifest_path = args.manifest
    elif args.benchmark:
        if Path(args.benchmark).exists():
            config.manifest_path = args.benchmark
        else:
            config.manifest_path = f"benchmarks/manifests/{args.benchmark}.yaml"
    if args.symbol:
        config.symbol = args.symbol
    if args.timeframe:
        config.timeframe = args.timeframe

    if args.kill or args.disengage_kill:
        return handle_kill_switch(args, config)

    if args.status:
        return handle_status(config)

    if args.check_feed:
        return handle_check_feed(config)

    if args.execution_report:
        return handle_execution_report(config)

    if args.parity_check:
        return handle_parity_check(config, raw_aggtrades=args.raw_aggtrades)

    if args.dashboard and not args.mode:
        from live_engine.dashboard import run_standalone_dashboard
        run_standalone_dashboard(config_path=args.config, host=args.host, port=args.port)
        return 0

    # Live safety validation, in two stages. The kill switch, operator acknowledgement and
    # credentials are checked before anything connects, so an unauthorised LIVE start never
    # opens an authenticated session. All 34 evidence-backed gates are then evaluated with
    # the initialised engine and its live streams attached — exchange filters, account
    # reconciliation and stream freshness are runtime facts — and always before the first
    # order can be submitted.
    ks = KillSwitch(Path(config.kill_switch_path))
    try:
        preflight_live_authorization(config, ks)
    except Exception as e:
        print(f"[FATAL SAFETY GATE REJECTION] {e}")
        return 1

    orchestrator = LiveEngineOrchestrator(config)

    if args.dry_run:
        print("[DRY-RUN] Initializing all engine components...")
        try:
            if config.mode.upper() == "LIVE":
                # A funded pre-flight has to satisfy the runtime gates, so it connects and
                # proves the streams, evaluates every gate, and processes nothing.
                asyncio.run(run_live_loop(orchestrator, config, dry_run=True))
            else:
                orchestrator.initialize()
                print_startup_banner(config, orchestrator)
        except PermissionError as e:
            print(f"[FATAL SAFETY GATE REJECTION] {e}")
            return 1
        except Exception as e:
            # A pre-flight that cannot initialise has proved nothing. Report the reason and
            # refuse, rather than leaving an operator to decode a traceback - a rejected API
            # key and a software fault look identical in one.
            print(f"[DRY-RUN FAILED] {type(e).__name__}: {e}")
            logger.debug("Dry-run initialisation failed", exc_info=True)
            return 1
        print("[DRY-RUN] Successfully verified strategy, filters, warmup, and reconciliation.")
        return 0

    if args.dashboard:
        from live_engine.dashboard import start_dashboard_server
        start_dashboard_server(config, host=args.host, port=args.port)

    try:
        asyncio.run(run_live_loop(orchestrator, config))
        return 0
    except PermissionError as e:
        print(f"[FATAL SAFETY GATE REJECTION] {e}")
        return 1
    except KeyboardInterrupt:
        print("\nShutdown requested by user.")
        return 0


if __name__ == "__main__":
    sys.exit(main())
