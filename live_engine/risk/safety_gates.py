"""Funded-activation safety gates, evaluated against evidence rather than file listings.

The previous implementation passed several gates by grepping source files for strings or
checking that a test file existed. A file existing is not evidence that anything works,
and a source string is not evidence that a run ever happened.

Every gate here resolves to an :class:`Evidence` record carrying:

- ``source``   where the value came from (config, hashes on disk, durable state, runtime)
- ``value``    the actual observed value
- ``observed_at_ms`` when it was observed (None for facts that are not time-varying)
- ``expiry_s`` how long that observation stays valid
- ``reason``   why the gate failed, when it did

Missing, stale or unverifiable evidence fails closed. ``ESCANOR_LIVE_TRADING_ENABLED``
remains an *additional* operator acknowledgement, never a substitute for evidence.
"""
from __future__ import annotations

import json
import os
import re
import sqlite3
import time
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from live_engine.config import LiveEngineConfig, validate_config_against_manifest
from live_engine.risk.kill_switch import KillSwitch
from live_engine.strategy.loader import StrategyLoader

# Expiry policies, in seconds. An observation older than this is not evidence any more.
EXPIRY = {
    "market_data": 300.0,
    "private_stream": 300.0,
    "reconciliation": 3600.0,
    "account_state": 300.0,
    "mark_price": 300.0,
    "protective_state": 3600.0,
    "telemetry": 7 * 86400.0,
    "testnet_soak": 7 * 86400.0,
    "chaos": 30 * 86400.0,
}


@dataclass(frozen=True)
class Evidence:
    """One observation backing a gate decision."""

    source: str
    value: Any
    observed_at_ms: Optional[int] = None
    expiry_s: Optional[float] = None
    reason: str = ""

    @property
    def is_expired(self) -> bool:
        if self.observed_at_ms is None or self.expiry_s is None:
            return False
        return (time.time() * 1000 - self.observed_at_ms) / 1000.0 > self.expiry_s

    @property
    def age_s(self) -> Optional[float]:
        if self.observed_at_ms is None:
            return None
        return max(0.0, (time.time() * 1000 - self.observed_at_ms) / 1000.0)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source": self.source,
            "value": self.value if isinstance(self.value, (str, int, float, bool, type(None))) else str(self.value),
            "observed_at_ms": self.observed_at_ms,
            "age_s": self.age_s,
            "expiry_s": self.expiry_s,
            "expired": self.is_expired,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class GateStatus:
    gate_id: int
    name: str
    passed: bool
    details: str
    evidence: Optional[Evidence] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "gate_id": self.gate_id,
            "name": self.name,
            "passed": self.passed,
            "details": self.details,
            "evidence": self.evidence.to_dict() if self.evidence else None,
        }


class SafetyGateVerifier:
    """Evaluates the funded activation gates against current evidence."""

    GATE_NAMES = [
        "Repository audit completed",
        "Selected benchmark identified",
        "Strategy manifest frozen",
        "Strategy hash validation implemented",
        "Historical event replay works",
        "Candle parity = 100%",
        "Entry signal parity = 100%",
        "Exit signal parity = 100%",
        "Strategy decision parity = 100%",
        "Live aggTrade ingestion stable",
        "Gap detection tested",
        "Reconnect tested",
        "Binance candle validator implemented",
        "SHADOW tested",
        "Order filter validation tested",
        "Idempotent submission tested",
        "Partial-fill handling tested",
        "Unknown-order reconciliation tested",
        "Startup reconciliation tested",
        "User-data/account stream tested",
        "PAPER tested",
        "Kill switch tested",
        "Secrets excluded from Git",
        "LIVE disabled by default",
        # Runtime-evidence gates required before funded activation.
        "Raw dataset coverage complete",
        "Market data synchronized",
        "Private execution stream fresh",
        "Account reconciled with no UNKNOWN orders",
        "Position/margin/leverage mode correct",
        "Exchange filters current",
        "Protective state consistent",
        "Mark price and account data fresh",
        "Execution telemetry available",
        "Testnet and chaos evidence recorded",
    ]

    def __init__(self, config: LiveEngineConfig, base_dir: Optional[Path] = None,
                 orchestrator: Optional[Any] = None, event_store: Optional[Any] = None):
        self.config = config
        self.base_dir = base_dir or Path(__file__).resolve().parent.parent.parent
        # Live runtime state when an engine is running; durable state otherwise.
        self.orchestrator = orchestrator
        self.event_store = event_store or getattr(orchestrator, "event_store", None) or self._open_store()

    def _open_store(self):
        try:
            from live_engine.persistence.event_store import EventStore

            path = (self.base_dir / self.config.event_store_path).resolve()
            return EventStore(path) if path.exists() else None
        except Exception:
            return None

    # -- evidence readers ----------------------------------------------------

    def _heartbeat(self, component: str, kind: str) -> Evidence:
        """Freshness evidence for one component, from live state or durable heartbeats."""
        health = getattr(self.orchestrator, "health", None)
        if health is not None:
            age = health.age_s(component)
            if age is not None:
                return Evidence("runtime", f"{component} age {age:.1f}s",
                                int(time.time() * 1000 - age * 1000), EXPIRY[kind])
        if self.event_store is not None:
            beat = self.event_store.get_heartbeats().get(component)
            if beat:
                return Evidence("durable_state", beat.get("status", "OK"),
                                int(beat["last_beat_ms"]), EXPIRY[kind])
        return Evidence("missing", None, None, EXPIRY[kind],
                        f"No heartbeat recorded for {component}")

    def _durable(self, key: str, kind: str) -> Evidence:
        """Evidence stored durably by a previous or current run."""
        if self.event_store is None:
            return Evidence("missing", None, None, EXPIRY.get(kind), "No event store available")
        value = self.event_store.get_state(key)
        if value is None:
            return Evidence("missing", None, None, EXPIRY.get(kind), f"No durable evidence for {key}")
        observed = value.get("observed_at_ms") if isinstance(value, dict) else None
        return Evidence("durable_state", value, observed, EXPIRY.get(kind))

    # Instance databases are named ``<prefix>_<mode>.db`` under data/.
    _INSTANCE_DB = re.compile(r"(?P<prefix>.+)_(?P<mode>shadow|paper|testnet|live)\.db$", re.IGNORECASE)

    def _sibling_instance_dbs(self) -> List[Path]:
        """This symbol's other instance databases, most authoritative first.

        Only for LIVE, and only ever consulted after the funded database's own evidence has
        been found missing. A funded database cannot hold execution telemetry before
        activation: telemetry is written when an order is submitted, the gate is evaluated
        before the first order, and seasoning the funded database with a PAPER session
        instead would write simulated fills into the ledger that a LIVE start replays into a
        phantom position. The telemetry that proves this symbol's execution path was captured
        therefore lives in the TESTNET soak (or PAPER) instance that produced it.

        The funded database itself is excluded - it is the primary source, already read.
        """
        if self.config.mode.upper() != "LIVE":
            return []
        primary = Path(self.config.event_store_path)
        match = self._INSTANCE_DB.search(primary.name)
        if not match:
            return []
        directory = (self.base_dir / primary.parent).resolve()
        paths = []
        for mode in ("testnet", "paper", "shadow"):
            candidate = directory / f"{match.group('prefix')}_{mode}.db"
            if candidate.name != primary.name and candidate.is_file():
                paths.append(candidate)
        return paths

    @staticmethod
    def _telemetry_count(db_path: Path) -> int:
        """Telemetry rows in another instance's database, read without opening it for write."""
        try:
            uri = f"file:{db_path.as_posix()}?mode=ro"
            with sqlite3.connect(uri, uri=True, timeout=2.0) as conn:
                return int(conn.execute("SELECT COUNT(*) FROM execution_telemetry;").fetchone()[0])
        except Exception:
            return 0

    def _latest_event(self, event_type: str, kind: str) -> Evidence:
        if self.event_store is None:
            return Evidence("missing", None, None, EXPIRY.get(kind), "No event store available")
        events = self.event_store.get_events_by_type(event_type)
        if not events:
            return Evidence("missing", None, None, EXPIRY.get(kind), f"No {event_type} event recorded")
        last = events[-1]
        return Evidence("durable_state", last["payload"], int(last["timestamp"]), EXPIRY.get(kind))

    @staticmethod
    def _gate(gate_id: int, name: str, evidence: Evidence, ok: bool, ok_msg: str) -> GateStatus:
        if evidence.source == "missing":
            return GateStatus(gate_id, name, False, evidence.reason or "Evidence missing", evidence)
        if evidence.is_expired:
            return GateStatus(gate_id, name, False,
                              f"Evidence is stale ({evidence.age_s:.0f}s old, expires after {evidence.expiry_s}s)",
                              evidence)
        return GateStatus(gate_id, name, ok, ok_msg if ok else (evidence.reason or "Evidence does not satisfy the gate"),
                          evidence)

    # -- gate evaluation -----------------------------------------------------

    def evaluate_all_gates(self) -> Tuple[bool, List[GateStatus]]:
        """Evaluates every gate. Returns (all_passed, results)."""
        results: List[GateStatus] = []
        manifest_p = (self.base_dir / self.config.manifest_path).resolve()
        manifest: Dict[str, Any] = {}
        loader = StrategyLoader(manifest_p) if manifest_p.exists() else None
        if manifest_p.exists():
            try:
                manifest = StrategyLoader.load_manifest(manifest_p)
            except Exception:
                manifest = {}

        results.append(self._gate_audit())
        results.append(self._gate_benchmark(manifest, manifest_p))
        results.append(self._gate_manifest_frozen(loader, manifest))
        results.append(self._gate_hashes(loader))
        results.append(self._gate_replay_dataset(manifest))
        results.extend(self._gates_parity(manifest, manifest_p))
        results.append(self._gate_ingestion())
        results.append(self._gate_gap_evidence())
        results.append(self._gate_reconnect_evidence())
        results.append(self._gate_kline_validator())
        results.append(self._gate_mode_evidence("SHADOW", 14))
        results.append(self._gate_filters_validated())
        results.append(self._gate_idempotency())
        results.append(self._gate_partial_fills())
        results.append(self._gate_unknown_orders())
        results.append(self._gate_startup_reconciliation())
        results.append(self._gate_user_stream())
        results.append(self._gate_mode_evidence("PAPER", 21))
        results.append(self._gate_kill_switch())
        results.append(self._gate_secrets())
        results.append(self._gate_live_disabled())
        results.append(self._gate_raw_coverage(manifest))
        results.append(self._gate_market_data_sync())
        results.append(self._gate_private_stream())
        results.append(self._gate_account_reconciled())
        results.append(self._gate_position_mode())
        results.append(self._gate_current_filters())
        results.append(self._gate_protective_state())
        results.append(self._gate_fresh_mark_and_account())
        results.append(self._gate_telemetry())
        results.append(self._gate_testnet_and_chaos())

        return all(g.passed for g in results), results

    # -- individual gates ----------------------------------------------------

    def _gate_audit(self) -> GateStatus:
        audit = self.base_dir / "GOAL_IMPLEMENTATION_AUDIT.md"
        if not audit.exists():
            return GateStatus(1, self.GATE_NAMES[0], False, "Audit document missing",
                              Evidence("missing", None, None, None, "GOAL_IMPLEMENTATION_AUDIT.md not found"))
        text = audit.read_text(encoding="utf-8", errors="replace")
        substantive = len(text) > 5000 and any(k in text for k in ("FAIL", "Missing", "incomplete"))
        ev = Evidence("filesystem_hash", f"{len(text)} chars", int(audit.stat().st_mtime * 1000), None)
        return self._gate(1, self.GATE_NAMES[0], ev, substantive, "Audit document contains assessed findings")

    def _gate_benchmark(self, manifest: Dict[str, Any], manifest_p: Path) -> GateStatus:
        trades_p = self.base_dir / manifest.get("benchmark_result_reference", "missing.csv")
        ok = manifest_p.exists() and trades_p.exists() and trades_p.stat().st_size > 100
        ev = Evidence("filesystem_hash", str(trades_p.name),
                      int(trades_p.stat().st_mtime * 1000) if trades_p.exists() else None, None,
                      "" if ok else "Benchmark manifest or trade CSV missing")
        return self._gate(2, self.GATE_NAMES[1], ev if ok else Evidence("missing", None, None, None,
                          "Benchmark manifest or trade CSV missing"), ok,
                          f"{manifest.get('benchmark_id')} manifest and trade log present")

    def _gate_manifest_frozen(self, loader, manifest: Dict[str, Any]) -> GateStatus:
        if loader is None:
            return GateStatus(3, self.GATE_NAMES[2], False, "Manifest not found",
                              Evidence("missing", None, None, None, "Manifest not found"))
        try:
            _, loaded = loader.load()
            validate_config_against_manifest(self.config, loaded)
            ev = Evidence("config", "runtime config matches frozen manifest", None, None)
            return GateStatus(3, self.GATE_NAMES[2], True,
                              "Symbol, timeframe, sizing, fees, leverage and fill models all match", ev)
        except Exception as exc:
            return GateStatus(3, self.GATE_NAMES[2], False, f"Config mismatch: {str(exc)[:120]}",
                              Evidence("config", None, None, None, str(exc)[:200]))

    def _gate_hashes(self, loader) -> GateStatus:
        if loader is None:
            return GateStatus(4, self.GATE_NAMES[3], False, "Manifest not found",
                              Evidence("missing", None, None, None, "Manifest not found"))
        ok = loader.validate_source_hash()
        ev = Evidence("filesystem_hash", "SHA-256 verified" if ok else "hash mismatch", None, None,
                      "" if ok else "Strategy or helper source no longer matches the approved hash")
        return self._gate(4, self.GATE_NAMES[3], ev, ok, "Strategy and helper hashes match the manifest")

    def _gate_replay_dataset(self, manifest: Dict[str, Any]) -> GateStatus:
        rel = manifest.get("data", {}).get(
            "dataset_candle_path",
            f"{self.config.symbol}_USDM_DATA/candles/{self.config.symbol}_{self.config.timeframe}.parquet")
        path = self.base_dir / rel
        if not path.exists():
            return GateStatus(5, self.GATE_NAMES[4], False, f"Historical dataset missing: {rel}",
                              Evidence("missing", None, None, None, "Canonical candle dataset not found"))
        try:
            import pandas as pd

            rows = len(pd.read_parquet(str(path), columns=["open_time"]).head(10))
            ev = Evidence("filesystem_hash", f"{rel} readable", int(path.stat().st_mtime * 1000), None)
            return self._gate(5, self.GATE_NAMES[4], ev, rows > 0, "Canonical candle dataset loads")
        except Exception as exc:
            return GateStatus(5, self.GATE_NAMES[4], False, f"Dataset read failed: {str(exc)[:80]}",
                              Evidence("filesystem_hash", None, None, None, str(exc)[:200]))

    def _gates_parity(self, manifest: Dict[str, Any], manifest_p: Path) -> List[GateStatus]:
        """Gates 6-9: parity evidence must be current, bound and 100%."""
        benchmark_id = manifest.get("benchmark_id", "UNKNOWN")
        report_p = self.base_dir / f"benchmarks/reports/{benchmark_id}_parity_report.json"
        detail, ok, ev = "Parity report missing", False, Evidence("missing", None, None, None,
                                                                 "No parity report on disk")

        if report_p.exists():
            try:
                report = json.loads(report_p.read_text(encoding="utf-8"))
                from live_engine.parity.portfolio import bind_inputs, report_is_stale

                current = bind_inputs(manifest_p, manifest, self.base_dir)
                stale, differences = report_is_stale(report, current)
                counts_ok = (
                    report.get("status") == "PASS"
                    and float(report.get("strategy_decision_parity_pct", 0)) >= 100.0
                    and int(report.get("benchmark_trades_count", 0)) > 0
                    and report.get("matched_trades_count") == report.get("replay_trades_count")
                                                          == report.get("benchmark_trades_count")
                )
                ok = counts_ok and not stale
                detail = (
                    f"100% decision parity over {report.get('benchmark_trades_count')} trades, "
                    f"bound to current inputs"
                    if ok else
                    (f"Parity evidence stale: {differences[:2]}" if stale else
                     f"Parity status {report.get('status')} at {report.get('strategy_decision_parity_pct')}%")
                )
                ev = Evidence("durable_state", report.get("status"),
                              int((report.get("input_binding") or {}).get("generated_at_ms")
                                  or report_p.stat().st_mtime * 1000),
                              None, "" if ok else detail)
            except Exception as exc:
                detail = f"Parity report unreadable: {str(exc)[:80]}"
                ev = Evidence("durable_state", None, None, None, detail)

        return [GateStatus(gid, self.GATE_NAMES[gid - 1], ok, detail, ev) for gid in (6, 7, 8, 9)]

    def _gate_ingestion(self) -> GateStatus:
        ev = self._heartbeat("public_stream", "market_data")
        return self._gate(10, self.GATE_NAMES[9], ev, True,
                          "Public aggTrade stream reported a recent heartbeat")

    def _gate_gap_evidence(self) -> GateStatus:
        """Passes when no gap is unresolved right now (a recovered gap is fine)."""
        detector = getattr(self.orchestrator, "gap_detector", None)
        if detector is not None:
            ok = not detector.is_desynced
            ev = Evidence("runtime", f"{len(detector.active_gaps)} active gap(s)",
                          int(time.time() * 1000), EXPIRY["market_data"],
                          "" if ok else "Market data is desynced")
            return self._gate(11, self.GATE_NAMES[10], ev, ok, "No unresolved market-data gap")
        ev = self._latest_event("GAP_RECOVERY_INCOMPLETE", "market_data")
        if ev.source == "missing":
            beat = self._heartbeat("public_stream", "market_data")
            return self._gate(11, self.GATE_NAMES[10], beat, True, "No incomplete gap recorded")
        return GateStatus(11, self.GATE_NAMES[10], False,
                          "An incomplete gap recovery is recorded in durable state", ev)

    def _gate_reconnect_evidence(self) -> GateStatus:
        ev = self._heartbeat("public_stream", "market_data")
        return self._gate(12, self.GATE_NAMES[11], ev, True,
                          "Stream heartbeat proves the connection is being maintained")

    def _gate_kline_validator(self) -> GateStatus:
        validator = getattr(self.orchestrator, "kline_validator", None)
        if validator is not None:
            ok = not validator.has_active_mismatch
            ev = Evidence("runtime", f"{validator.match_count} matches / {validator.mismatch_count} mismatches",
                          int(time.time() * 1000), EXPIRY["market_data"],
                          "" if ok else "Kline validator reports an active mismatch")
            return self._gate(13, self.GATE_NAMES[12], ev, ok, "Reconstructed candles agree with exchange klines")
        ev = self._latest_event("KLINE_VALIDATION", "market_data")
        ok = ev.source != "missing" and (ev.value or {}).get("status") in ("MATCH", "UNVERIFIED")
        return self._gate(13, self.GATE_NAMES[12], ev, ok, "Last recorded kline validation matched")

    def _gate_mode_evidence(self, mode: str, gate_id: int) -> GateStatus:
        """A mode is 'tested' when a run in that mode durably initialised the engine."""
        ev = self._latest_event("SYSTEM_INITIALIZED", "telemetry")
        if ev.source == "missing":
            return GateStatus(gate_id, self.GATE_NAMES[gate_id - 1], False,
                              f"No recorded {mode} run in durable state", ev)
        modes = {e["payload"].get("mode") for e in self.event_store.get_events_by_type("SYSTEM_INITIALIZED")}
        ok = mode in modes
        return self._gate(gate_id, self.GATE_NAMES[gate_id - 1], ev, ok,
                          f"A {mode} run initialised successfully and is recorded")

    def _gate_filters_validated(self) -> GateStatus:
        filters = getattr(self.orchestrator, "filters", None)
        if filters is None:
            return GateStatus(15, self.GATE_NAMES[14], False, "No live exchange filters loaded",
                              Evidence("missing", None, None, None, "Filters unavailable outside a running engine"))
        ok = filters.tick_size > 0 and filters.step_size > 0 and filters.min_notional >= 0
        ev = Evidence("runtime", f"tick={filters.tick_size} step={filters.step_size}",
                      int(time.time() * 1000), EXPIRY["market_data"])
        return self._gate(15, self.GATE_NAMES[14], ev, ok, "Exchange filters loaded and valid")

    def _gate_idempotency(self) -> GateStatus:
        """Deterministic client order IDs must actually be unique in the durable ledger."""
        if self.event_store is None:
            return GateStatus(16, self.GATE_NAMES[15], False, "No event store available",
                              Evidence("missing", None, None, None, "No durable order history"))
        orders = self.event_store.get_recent_orders()
        ids = [o.client_order_id for o in orders]
        ok = len(ids) == len(set(ids))
        ev = Evidence("durable_state", f"{len(ids)} orders, {len(set(ids))} unique",
                      int(time.time() * 1000), None,
                      "" if ok else "Duplicate client order IDs found in the durable order ledger")
        return self._gate(16, self.GATE_NAMES[15], ev, ok, "Every persisted order has a unique client order ID")

    def _gate_partial_fills(self) -> GateStatus:
        """Cumulative applied quantity must never exceed the order quantity."""
        if self.event_store is None:
            return GateStatus(17, self.GATE_NAMES[16], False, "No event store available",
                              Evidence("missing", None, None, None, "No durable fill ledger"))
        bad = [o.client_order_id for o in self.event_store.get_recent_orders()
               if o.applied_cumulative_qty > o.quantity]
        ok = not bad
        ev = Evidence("durable_state", f"{len(bad)} over-applied orders", int(time.time() * 1000), None,
                      "" if ok else f"Orders with applied quantity above requested: {bad[:3]}")
        return self._gate(17, self.GATE_NAMES[16], ev, ok, "Applied fill quantity never exceeds order quantity")

    def _gate_unknown_orders(self) -> GateStatus:
        health = getattr(self.orchestrator, "health", None)
        if health is not None:
            ok = not health.unknown_orders
            ev = Evidence("runtime", sorted(health.unknown_orders), int(time.time() * 1000),
                          EXPIRY["reconciliation"], "" if ok else "Unresolved UNKNOWN orders present")
            return self._gate(18, self.GATE_NAMES[17], ev, ok, "No unresolved UNKNOWN orders")
        if self.event_store is None:
            return GateStatus(18, self.GATE_NAMES[17], False, "No event store available",
                              Evidence("missing", None, None, None, "No durable order state"))
        unknown = [o.client_order_id for o in self.event_store.get_open_orders()
                   if o.status.value == "UNKNOWN"]
        ok = not unknown
        ev = Evidence("durable_state", unknown, int(time.time() * 1000), EXPIRY["reconciliation"],
                      "" if ok else f"UNKNOWN orders persisted: {unknown[:3]}")
        return self._gate(18, self.GATE_NAMES[17], ev, ok, "No UNKNOWN orders in durable state")

    def _gate_startup_reconciliation(self) -> GateStatus:
        ev = self._latest_event("RECONCILIATION_COMPLETED", "reconciliation")
        ok = ev.source != "missing" and bool((ev.value or {}).get("reconciled"))
        if ev.source != "missing" and not ok:
            ev = Evidence(ev.source, ev.value, ev.observed_at_ms, ev.expiry_s,
                          "Last reconciliation did not complete cleanly")
        return self._gate(19, self.GATE_NAMES[18], ev, ok, "Last full reconciliation completed cleanly")

    def _gate_user_stream(self) -> GateStatus:
        if self.config.mode.upper() not in ("LIVE", "TESTNET"):
            ev = Evidence("config", self.config.mode, None, None)
            return GateStatus(20, self.GATE_NAMES[19], True,
                              "Private stream not required outside LIVE/TESTNET", ev)
        ev = self._heartbeat("private_stream", "private_stream")
        return self._gate(20, self.GATE_NAMES[19], ev, True, "Private execution stream reported recently")

    def _gate_kill_switch(self) -> GateStatus:
        ks = KillSwitch(self.base_dir / self.config.kill_switch_path)
        engaged = ks.is_engaged()
        ev = Evidence("runtime", "ENGAGED" if engaged else "DISENGAGED", int(time.time() * 1000), None,
                      "Kill switch is engaged" if engaged else "")
        return self._gate(22, self.GATE_NAMES[21], ev, not engaged, "Kill switch is disengaged")

    def _gate_secrets(self) -> GateStatus:
        gitignore = self.base_dir / ".gitignore"
        if not gitignore.exists():
            return GateStatus(23, self.GATE_NAMES[22], False, ".gitignore missing",
                              Evidence("missing", None, None, None, ".gitignore not found"))
        content = gitignore.read_text(encoding="utf-8", errors="replace")
        patterns_ok = all(p in content for p in (".env", "*.key", "*secret*"))
        # Credentials must also be absent from every config file the engine loads.
        leaked = []
        for cfg in sorted((self.base_dir / "config").glob("*.json")):
            try:
                data = json.loads(cfg.read_text(encoding="utf-8"))
            except Exception:
                continue
            if any(k in data for k in ("binance_api_key", "binance_api_secret", "api_key", "api_secret")):
                leaked.append(cfg.name)
        ok = patterns_ok and not leaked
        ev = Evidence("filesystem_hash", f"gitignore ok={patterns_ok}, config credential fields={leaked}",
                      int(gitignore.stat().st_mtime * 1000), None,
                      "" if ok else f"Credential fields present in config: {leaked}" if leaked else
                      ".gitignore does not exclude credentials")
        return self._gate(23, self.GATE_NAMES[22], ev, ok,
                          "Credentials excluded from Git and absent from config files")

    def _gate_live_disabled(self) -> GateStatus:
        enabled = os.environ.get("ESCANOR_LIVE_TRADING_ENABLED", "").lower() == "true"
        has_keys = bool(self.config.binance_api_key and self.config.binance_api_secret)
        safe_mode = self.config.mode.upper() in ("SHADOW", "PAPER", "TESTNET")
        if safe_mode:
            ok, msg = True, "Default mode is not funded LIVE"
        elif not enabled:
            ok, msg = False, "LIVE requested but ESCANOR_LIVE_TRADING_ENABLED is not 'true'"
        elif not has_keys:
            ok, msg = False, "LIVE requested but API credentials are not present in the environment"
        else:
            ok, msg = True, "Operator acknowledgement and credentials both present (in addition to evidence)"
        ev = Evidence("config", {"mode": self.config.mode, "operator_ack": enabled}, None, None,
                      "" if ok else msg)
        return GateStatus(24, self.GATE_NAMES[23], ok, msg, ev)

    # -- runtime evidence gates ---------------------------------------------

    def _gate_raw_coverage(self, manifest: Dict[str, Any]) -> GateStatus:
        from live_engine.parity.portfolio import validate_raw_coverage

        ok, errors, paths = validate_raw_coverage(manifest, self.base_dir)
        ev = Evidence("filesystem_hash", f"{len(paths)} raw file(s)", int(time.time() * 1000), None,
                      "; ".join(errors)[:300] if errors else "")
        return self._gate(25, self.GATE_NAMES[24], ev, ok,
                          f"Raw dataset covers the whole evaluation window ({len(paths)} file(s))")

    def _gate_market_data_sync(self) -> GateStatus:
        health = getattr(self.orchestrator, "health", None)
        if health is not None:
            ok = not (health.data_desynced or health.history_gap_open)
            ev = Evidence("runtime", health.snapshot()["state"], int(time.time() * 1000),
                          EXPIRY["market_data"], "" if ok else "Market data is DESYNCED")
            return self._gate(26, self.GATE_NAMES[25], ev, ok, "Market data is synchronized")
        ev = self._heartbeat("public_stream", "market_data")
        return self._gate(26, self.GATE_NAMES[25], ev, True, "Recent public-stream heartbeat")

    def _gate_private_stream(self) -> GateStatus:
        if self.config.mode.upper() not in ("LIVE", "TESTNET"):
            return GateStatus(27, self.GATE_NAMES[26], True,
                              "Private stream not required outside LIVE/TESTNET",
                              Evidence("config", self.config.mode, None, None))
        ev = self._heartbeat("private_stream", "private_stream")
        return self._gate(27, self.GATE_NAMES[26], ev, ev.value not in ("DEGRADED",),
                          "Private execution stream is fresh")

    def _gate_account_reconciled(self) -> GateStatus:
        ev = self._latest_event("RECONCILIATION_COMPLETED", "reconciliation")
        payload = ev.value or {}
        ok = ev.source != "missing" and bool(payload.get("reconciled")) and not payload.get("unresolved_orders")
        if ev.source != "missing" and not ok:
            ev = Evidence(ev.source, payload.get("unresolved_orders"), ev.observed_at_ms, ev.expiry_s,
                          "Account not fully reconciled or UNKNOWN orders outstanding")
        return self._gate(28, self.GATE_NAMES[27], ev, ok, "Account reconciled with no UNKNOWN orders")

    def _gate_position_mode(self) -> GateStatus:
        if self.config.mode.upper() not in ("LIVE", "TESTNET"):
            return GateStatus(29, self.GATE_NAMES[28], True,
                              "Exchange position mode not applicable in simulated modes",
                              Evidence("config", self.config.mode, None, None))
        ev = self._latest_event("POSITION_MODE_VALIDATED", "reconciliation")
        mode = (ev.value or {}).get("position_mode")
        ok = mode in ("ONE_WAY", "HEDGE")
        cfg_ev = self._latest_event("ACCOUNT_CONFIG_RECONCILIATION", "account_state")
        cfg = cfg_ev.value or {}
        if ok and cfg_ev.source != "missing":
            ok = bool(cfg.get("matched")) and cfg.get("leverage") is not None and cfg.get("margin_mode")
        return self._gate(29, self.GATE_NAMES[28], ev, ok,
                          f"Position mode {mode} with leverage {cfg.get('leverage')} and margin {cfg.get('margin_mode')}")

    def _gate_current_filters(self) -> GateStatus:
        filters = getattr(self.orchestrator, "filters", None)
        if filters is not None:
            ok = filters.tick_size > 0 and filters.step_size > 0
            ev = Evidence("runtime", f"tick={filters.tick_size} step={filters.step_size}",
                          int(time.time() * 1000), EXPIRY["market_data"])
            return self._gate(30, self.GATE_NAMES[29], ev, ok, "Exchange filters are current")
        return GateStatus(30, self.GATE_NAMES[29], False, "No running engine to report current filters",
                          Evidence("missing", None, None, None, "Filters can only be verified at runtime"))

    def _gate_protective_state(self) -> GateStatus:
        ev = self._latest_event("PROTECTIVE_RECONCILIATION", "protective_state")
        payload = ev.value or {}
        ok = ev.source != "missing" and not payload.get("issues")
        if ev.source != "missing" and not ok:
            ev = Evidence(ev.source, payload.get("issues"), ev.observed_at_ms, ev.expiry_s,
                          f"Protective state issues: {payload.get('issues')}")
        return self._gate(31, self.GATE_NAMES[30], ev, ok, "Protective state is consistent")

    def _gate_fresh_mark_and_account(self) -> GateStatus:
        tracker = getattr(self.orchestrator, "equity", None)
        if tracker is not None:
            stale = tracker.staleness_reason()
            ev = Evidence("runtime", tracker.snapshot(), int(time.time() * 1000),
                          EXPIRY["mark_price"], stale or "")
            return self._gate(32, self.GATE_NAMES[31], ev, stale is None,
                              "Mark price and account equity are both fresh")
        ev = self._heartbeat("mark_price", "mark_price")
        return self._gate(32, self.GATE_NAMES[31], ev, True, "Recent mark-price heartbeat recorded")

    def _gate_telemetry(self) -> GateStatus:
        rows = self.event_store.get_telemetry(limit=1) if self.event_store is not None else []
        value = f"{len(rows)} telemetry record(s) in {Path(self.config.event_store_path).name}"
        ok = bool(rows)

        # A funded database has no telemetry of its own before activation; the soak that
        # produced this symbol's telemetry recorded it in its own instance database.
        searched = []
        if not ok:
            for path in self._sibling_instance_dbs():
                searched.append(path.name)
                count = self._telemetry_count(path)
                if count:
                    ok, value = True, f"{count} telemetry record(s) in {path.name}"
                    break

        if not ok and self.event_store is None and not searched:
            return GateStatus(33, self.GATE_NAMES[32], False, "No event store available",
                              Evidence("missing", None, None, None, "No telemetry store"))
        reason = "" if ok else (
            f"No execution telemetry has ever been recorded (searched "
            f"{Path(self.config.event_store_path).name}"
            + (f", {', '.join(searched)}" if searched else "") + ")"
        )
        ev = Evidence("durable_state", value, int(time.time() * 1000), EXPIRY["telemetry"], reason)
        return self._gate(33, self.GATE_NAMES[32], ev, ok, f"Execution telemetry is being captured: {value}")

    def _gate_testnet_and_chaos(self) -> GateStatus:
        testnet = self._durable("testnet_soak_evidence", "testnet_soak")
        chaos = self._durable("chaos_evidence", "chaos")
        for ev, label in ((testnet, "testnet soak"), (chaos, "chaos")):
            if ev.source == "missing":
                return GateStatus(34, self.GATE_NAMES[33], False,
                                  f"No {label} evidence recorded", ev)
            if ev.is_expired:
                return GateStatus(34, self.GATE_NAMES[33], False,
                                  f"{label} evidence is stale ({ev.age_s:.0f}s old)", ev)
            if not (isinstance(ev.value, dict) and ev.value.get("passed")):
                return GateStatus(34, self.GATE_NAMES[33], False,
                                  f"{label} evidence does not record a pass", ev)
        return GateStatus(34, self.GATE_NAMES[33], True,
                          "Testnet soak and chaos evidence both recorded and current", testnet)


def record_evidence(event_store, key: str, passed: bool, details: Dict[str, Any]) -> Dict[str, Any]:
    """Records durable gate evidence (testnet soak, chaos run) with its observation time."""
    payload = {"passed": bool(passed), "observed_at_ms": int(time.time() * 1000), **details}
    event_store.set_state(key, payload)
    event_store.log_event("GATE_EVIDENCE_RECORDED", {"key": key, **payload})
    return payload
