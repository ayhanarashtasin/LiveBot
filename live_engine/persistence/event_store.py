"""Persistent local SQLite storage for raw market events, orders, fills, and metrics in WAL mode."""
from __future__ import annotations

import json
import sqlite3
import time
from contextlib import contextmanager
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Iterator

from live_engine.execution.models import Order, OrderSide, OrderStatus, OrderType, SignalAction, SignalEvent
from live_engine.market_data.models import AggTrade, Candle

# Current persistence schema revision. Bump when a forward migration is added below.
SCHEMA_VERSION = 4
BUSY_TIMEOUT_S = 30.0


class EventStore:
    """Thread-safe, durable SQLite database in WAL mode storing market data and trading actions."""

    def __init__(self, db_path: str | Path = "data/escanor_live.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    @contextmanager
    def _get_connection(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(str(self.db_path), timeout=BUSY_TIMEOUT_S)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.execute(f"PRAGMA busy_timeout={int(BUSY_TIMEOUT_S * 1000)};")
        conn.row_factory = sqlite3.Row
        try:
            with conn:
                yield conn
        finally:
            conn.close()


    def _init_db(self) -> None:
        with self._get_connection() as conn:
            # 1. Raw aggTrades table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS raw_aggtrades (
                    symbol TEXT NOT NULL,
                    agg_trade_id INTEGER PRIMARY KEY,
                    trade_time INTEGER NOT NULL,
                    event_time INTEGER NOT NULL,
                    received_at INTEGER NOT NULL,
                    price TEXT NOT NULL,
                    quantity TEXT NOT NULL,
                    first_trade_id INTEGER NOT NULL,
                    last_trade_id INTEGER NOT NULL,
                    buyer_is_market_maker INTEGER NOT NULL
                );
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_agg_time ON raw_aggtrades(symbol, trade_time);")

            # 2. Reconstructed candles table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS candles (
                    symbol TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    open_time INTEGER NOT NULL,
                    close_time INTEGER NOT NULL,
                    open TEXT NOT NULL,
                    high TEXT NOT NULL,
                    low TEXT NOT NULL,
                    close TEXT NOT NULL,
                    volume TEXT NOT NULL,
                    taker_buy_base_volume TEXT NOT NULL DEFAULT '0',
                    trade_count INTEGER NOT NULL,
                    is_closed INTEGER NOT NULL,
                    PRIMARY KEY (symbol, timeframe, open_time)
                );
            """)

            # 3. Strategy signals table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS signals (
                    signal_id TEXT PRIMARY KEY,
                    benchmark_id TEXT NOT NULL,
                    strategy_hash TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    candle_open_time INTEGER NOT NULL,
                    candle_close_time INTEGER NOT NULL,
                    generated_at INTEGER NOT NULL,
                    action TEXT NOT NULL,
                    reference_price TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    payload_json TEXT
                );
            """)

            # 4. Orders table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS orders (
                    client_order_id TEXT PRIMARY KEY,
                    exchange_order_id TEXT,
                    signal_id TEXT,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    order_type TEXT NOT NULL,
                    quantity TEXT NOT NULL,
                    price TEXT,
                    stop_price TEXT,
                    status TEXT NOT NULL,
                    filled_quantity TEXT NOT NULL,
                    accumulated_fees TEXT NOT NULL,
                    avg_fill_price TEXT,
                    rejection_reason TEXT,
                    created_at INTEGER NOT NULL,
                    submitted_at INTEGER,
                    acknowledged_at INTEGER,
                    filled_at INTEGER
                );
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_orders_signal ON orders(signal_id);")

            # 5. Operational incidents and audit table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS incidents (
                    incident_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp INTEGER NOT NULL,
                    category TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    details TEXT NOT NULL
                );
            """)

            # 6. Structured audit events
            conn.execute("""
                CREATE TABLE IF NOT EXISTS audit_events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp INTEGER NOT NULL,
                    event_type TEXT NOT NULL,
                    payload_json TEXT NOT NULL
                );
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_audit_event_type ON audit_events(event_type);")

            # 7. Append-only raw execution/fill events. dedup_key is the exchange trade ID when
            # the venue supplies one, otherwise a cumulative-quantity marker, so replaying the
            # same execution is harmless.
            conn.execute("""
                CREATE TABLE IF NOT EXISTS fills (
                    fill_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    client_order_id TEXT NOT NULL,
                    exchange_order_id TEXT,
                    dedup_key TEXT NOT NULL,
                    trade_id TEXT,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    last_qty TEXT NOT NULL,
                    cumulative_qty TEXT NOT NULL,
                    last_price TEXT,
                    avg_price TEXT,
                    commission TEXT NOT NULL,
                    commission_asset TEXT,
                    order_status TEXT,
                    reduce_only INTEGER NOT NULL DEFAULT 0,
                    position_side TEXT,
                    event_time INTEGER,
                    transaction_time INTEGER,
                    received_at INTEGER NOT NULL,
                    source TEXT NOT NULL,
                    UNIQUE (client_order_id, dedup_key)
                );
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_fills_order ON fills(client_order_id, fill_id);")

            # 8. Persisted pending next-open actions (survive restart between signal and fill).
            conn.execute("""
                CREATE TABLE IF NOT EXISTS pending_actions (
                    signal_id TEXT PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    action TEXT NOT NULL,
                    signal_candle_open_time INTEGER NOT NULL,
                    expected_execution_open_time INTEGER NOT NULL,
                    benchmark_reference_price TEXT NOT NULL,
                    indicator_snapshot_json TEXT,
                    quantity TEXT,
                    state TEXT NOT NULL,
                    created_at INTEGER NOT NULL,
                    resolved_at INTEGER,
                    resolution TEXT
                );
            """)

            # 9. Execution-quality telemetry (immutable, one row per submission attempt).
            conn.execute("""
                CREATE TABLE IF NOT EXISTS execution_telemetry (
                    telemetry_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    client_order_id TEXT,
                    signal_id TEXT,
                    benchmark_id TEXT,
                    payload_json TEXT NOT NULL,
                    created_at INTEGER NOT NULL
                );
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_telemetry_signal ON execution_telemetry(signal_id);")

            # 10. Component heartbeats / freshness for the health supervisor.
            conn.execute("""
                CREATE TABLE IF NOT EXISTS heartbeats (
                    component TEXT PRIMARY KEY,
                    last_beat_ms INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    details TEXT
                );
            """)

            # 11. Durable engine key/value state (last accepted aggTrade ID, daily equity
            # snapshot, protective-order linkage, reconciliation cursors).
            conn.execute("""
                CREATE TABLE IF NOT EXISTS engine_state (
                    key TEXT PRIMARY KEY,
                    value_json TEXT NOT NULL,
                    updated_at INTEGER NOT NULL
                );
            """)

            conn.execute("CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL);")
            conn.commit()
            self._migrate(conn)

    # --- schema versioning -------------------------------------------------

    # Columns added after the first release. Existing databases are migrated forward
    # in place; no table is ever dropped or rewritten.
    _FORWARD_COLUMNS = {
        "orders": (
            ("applied_cumulative_qty", "TEXT NOT NULL DEFAULT '0'"),
            ("reduce_only", "INTEGER NOT NULL DEFAULT 0"),
            ("position_side", "TEXT"),
        ),
        "candles": (("taker_buy_base_volume", "TEXT NOT NULL DEFAULT '0'"),),
        "pending_actions": (("indicator_snapshot_json", "TEXT"),),
    }

    def _migrate(self, conn: sqlite3.Connection) -> None:
        """Applies forward-only migrations, preserving all existing rows."""
        row = conn.execute("SELECT version FROM schema_version LIMIT 1;").fetchone()
        current = int(row["version"]) if row else 0

        for table, columns in self._FORWARD_COLUMNS.items():
            existing = {r["name"] for r in conn.execute(f"PRAGMA table_info({table});")}
            for name, ddl in columns:
                if name not in existing:
                    conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {ddl};")

        if row is None:
            conn.execute("INSERT INTO schema_version (version) VALUES (?);", (SCHEMA_VERSION,))
        elif current != SCHEMA_VERSION:
            conn.execute("UPDATE schema_version SET version = ?;", (SCHEMA_VERSION,))
        conn.commit()

    def store_aggtrade(self, trade: AggTrade) -> bool:
        """Stores aggTrade event idempotently. Returns True if newly inserted, False if duplicate."""
        with self._get_connection() as conn:
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO raw_aggtrades (
                    symbol, agg_trade_id, trade_time, event_time, received_at,
                    price, quantity, first_trade_id, last_trade_id, buyer_is_market_maker
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                (
                    trade.symbol,
                    trade.agg_trade_id,
                    trade.trade_time,
                    trade.event_time,
                    trade.received_at,
                    str(trade.price),
                    str(trade.quantity),
                    trade.first_trade_id,
                    trade.last_trade_id,
                    int(trade.buyer_is_market_maker),
                ),
            )
            conn.commit()
            return cur.rowcount > 0

    def store_candle(self, candle: Candle) -> None:
        """Stores or replaces a closed or active candle."""
        with self._get_connection() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO candles (
                    symbol, timeframe, open_time, close_time, open, high, low, close,
                    volume, taker_buy_base_volume, trade_count, is_closed
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                (
                    candle.symbol,
                    candle.timeframe,
                    candle.open_time,
                    candle.close_time,
                    str(candle.open),
                    str(candle.high),
                    str(candle.low),
                    str(candle.close),
                    str(candle.volume),
                    str(candle.taker_buy_base_volume),
                    candle.trade_count,
                    int(candle.is_closed),
                ),
            )
            conn.commit()

    def store_signal(self, signal: SignalEvent) -> bool:
        """Stores a SignalEvent. Returns True if newly inserted."""
        with self._get_connection() as conn:
            payload = json.dumps(signal.indicator_snapshot) if signal.indicator_snapshot else None
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO signals (
                    signal_id, benchmark_id, strategy_hash, symbol, timeframe,
                    candle_open_time, candle_close_time, generated_at, action,
                    reference_price, reason, payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                (
                    signal.signal_id,
                    signal.benchmark_id,
                    signal.strategy_hash,
                    signal.symbol,
                    signal.timeframe,
                    signal.candle_open_time,
                    signal.candle_close_time,
                    signal.generated_at,
                    signal.action.value,
                    str(signal.reference_price),
                    signal.reason,
                    payload,
                ),
            )
            conn.commit()
            return cur.rowcount > 0

    def get_signal(self, signal_id: str) -> Optional[SignalEvent]:
        """Fetch a stored signal by its signal_id."""
        with self._get_connection() as conn:
            row = conn.execute("SELECT * FROM signals WHERE signal_id = ?;", (signal_id,)).fetchone()
            if not row:
                return None
            payload = json.loads(row["payload_json"]) if row["payload_json"] else {}
            return SignalEvent(
                signal_id=row["signal_id"],
                benchmark_id=row["benchmark_id"],
                strategy_hash=row["strategy_hash"],
                symbol=row["symbol"],
                timeframe=row["timeframe"],
                candle_open_time=row["candle_open_time"],
                candle_close_time=row["candle_close_time"],
                generated_at=row["generated_at"],
                action=SignalAction(row["action"]),
                reference_price=Decimal(str(row["reference_price"])),
                reason=row["reason"],
                indicator_snapshot=payload,
            )

    def save_order(self, order: Order) -> None:
        """Insert or update order in local database."""
        with self._get_connection() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO orders (
                    client_order_id, exchange_order_id, signal_id, symbol, side,
                    order_type, quantity, price, stop_price, status, filled_quantity,
                    accumulated_fees, avg_fill_price, rejection_reason, created_at,
                    submitted_at, acknowledged_at, filled_at,
                    applied_cumulative_qty, reduce_only, position_side
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                (
                    order.client_order_id,
                    order.exchange_order_id,
                    order.signal_id,
                    order.symbol,
                    order.side.value,
                    order.order_type.value,
                    str(order.quantity),
                    str(order.price) if order.price is not None else None,
                    str(order.stop_price) if order.stop_price is not None else None,
                    order.status.value,
                    str(order.filled_quantity),
                    str(order.accumulated_fees),
                    str(order.avg_fill_price) if order.avg_fill_price is not None else None,
                    order.rejection_reason,
                    order.created_at,
                    order.submitted_at,
                    order.acknowledged_at,
                    order.filled_at,
                    str(order.applied_cumulative_qty),
                    int(order.reduce_only),
                    order.position_side,
                ),
            )
            conn.commit()

    @staticmethod
    def _row_to_order(row: sqlite3.Row) -> Order:
        """Single row -> Order mapping shared by every order query."""
        keys = row.keys()
        return Order(
            client_order_id=row["client_order_id"],
            symbol=row["symbol"],
            side=OrderSide(row["side"]),
            order_type=OrderType(row["order_type"]),
            quantity=Decimal(row["quantity"]),
            price=Decimal(row["price"]) if row["price"] is not None else None,
            stop_price=Decimal(row["stop_price"]) if row["stop_price"] is not None else None,
            status=OrderStatus(row["status"]),
            exchange_order_id=row["exchange_order_id"],
            signal_id=row["signal_id"],
            created_at=row["created_at"],
            submitted_at=row["submitted_at"],
            acknowledged_at=row["acknowledged_at"],
            filled_at=row["filled_at"],
            filled_quantity=Decimal(row["filled_quantity"]),
            accumulated_fees=Decimal(row["accumulated_fees"]),
            avg_fill_price=Decimal(row["avg_fill_price"]) if row["avg_fill_price"] is not None else None,
            rejection_reason=row["rejection_reason"],
            applied_cumulative_qty=Decimal(row["applied_cumulative_qty"]) if "applied_cumulative_qty" in keys and row["applied_cumulative_qty"] is not None else Decimal("0"),
            reduce_only=bool(row["reduce_only"]) if "reduce_only" in keys and row["reduce_only"] is not None else False,
            position_side=row["position_side"] if "position_side" in keys else None,
        )

    def get_order_by_client_id(self, client_order_id: str) -> Optional[Order]:
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM orders WHERE client_order_id = ?;", (client_order_id,)
            ).fetchone()
            return self._row_to_order(row) if row else None

    def get_open_orders(self) -> List[Order]:
        """Retrieves active/unresolved orders across restarts."""
        with self._get_connection() as conn:
            cur = conn.execute(
                "SELECT * FROM orders WHERE status IN ('CREATED', 'SUBMITTING', 'NEW', 'PARTIALLY_FILLED', 'UNKNOWN');"
            )
            return [self._row_to_order(r) for r in cur.fetchall()]

    def get_recent_orders(self, since_ms: int = 0) -> List[Order]:
        """Every Escanor-owned order created at or after since_ms, oldest first."""
        with self._get_connection() as conn:
            cur = conn.execute(
                "SELECT * FROM orders WHERE created_at >= ? ORDER BY created_at ASC;", (since_ms,)
            )
            return [self._row_to_order(r) for r in cur.fetchall()]

    def record_incident(self, category: str, severity: str, details: str) -> None:
        now_ms = int(time.time() * 1000)
        with self._get_connection() as conn:
            conn.execute(
                "INSERT INTO incidents (timestamp, category, severity, details) VALUES (?, ?, ?, ?);",
                (now_ms, category, severity, details),
            )
            conn.commit()

    def log_event(self, event_type: str, payload: Dict[str, Any]) -> None:
        """Logs arbitrary structured audit event."""
        now_ms = int(time.time() * 1000)
        with self._get_connection() as conn:
            conn.execute(
                "INSERT INTO audit_events (timestamp, event_type, payload_json) VALUES (?, ?, ?);",
                (now_ms, event_type, json.dumps(payload)),
            )
            conn.commit()

    def get_events_by_type(self, event_type: str) -> List[Dict[str, Any]]:
        """Retrieves structured audit events by event_type ordered chronologically."""
        with self._get_connection() as conn:
            cur = conn.execute(
                "SELECT * FROM audit_events WHERE event_type = ? ORDER BY event_id ASC;",
                (event_type,),
            )
            return [
                {
                    "event_id": row["event_id"],
                    "timestamp": row["timestamp"],
                    "event_type": row["event_type"],
                    "payload": json.loads(row["payload_json"]),
                }
                for row in cur.fetchall()
            ]

    # --- append-only fill ledger -------------------------------------------

    def record_fill(self, fill: Dict[str, Any]) -> bool:
        """Appends a raw execution event. Returns False if this execution was already stored.

        The UNIQUE(client_order_id, dedup_key) constraint makes replaying the same
        execution harmless: the second insert is ignored and the caller applies nothing.
        """
        with self._get_connection() as conn:
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO fills (
                    client_order_id, exchange_order_id, dedup_key, trade_id, symbol, side,
                    last_qty, cumulative_qty, last_price, avg_price, commission,
                    commission_asset, order_status, reduce_only, position_side,
                    event_time, transaction_time, received_at, source
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                (
                    fill["client_order_id"],
                    fill.get("exchange_order_id"),
                    fill["dedup_key"],
                    fill.get("trade_id"),
                    fill["symbol"],
                    fill["side"],
                    str(fill.get("last_qty", "0")),
                    str(fill.get("cumulative_qty", "0")),
                    str(fill["last_price"]) if fill.get("last_price") is not None else None,
                    str(fill["avg_price"]) if fill.get("avg_price") is not None else None,
                    str(fill.get("commission", "0")),
                    fill.get("commission_asset"),
                    fill.get("order_status"),
                    int(bool(fill.get("reduce_only", False))),
                    fill.get("position_side"),
                    fill.get("event_time"),
                    fill.get("transaction_time"),
                    int(fill.get("received_at") or time.time() * 1000),
                    fill.get("source", "UNKNOWN"),
                ),
            )
            conn.commit()
            return cur.rowcount > 0

    def get_fills(self, client_order_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Returns stored fill events in insertion order (replay order)."""
        sql = "SELECT * FROM fills"
        params: tuple = ()
        if client_order_id is not None:
            sql += " WHERE client_order_id = ?"
            params = (client_order_id,)
        sql += " ORDER BY fill_id ASC;"
        with self._get_connection() as conn:
            return [dict(r) for r in conn.execute(sql, params).fetchall()]

    # --- pending next-open actions ----------------------------------------

    def save_pending_action(self, action: Dict[str, Any]) -> None:
        with self._get_connection() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO pending_actions (
                    signal_id, symbol, timeframe, action, signal_candle_open_time,
                    expected_execution_open_time, benchmark_reference_price,
                    indicator_snapshot_json, quantity, state, created_at, resolved_at, resolution
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                (
                    action["signal_id"], action["symbol"], action["timeframe"], action["action"],
                    int(action["signal_candle_open_time"]), int(action["expected_execution_open_time"]),
                    str(action["benchmark_reference_price"]),
                    json.dumps(action.get("indicator_snapshot")) if action.get("indicator_snapshot") else None,
                    str(action["quantity"]) if action.get("quantity") is not None else None,
                    action.get("state", "PENDING"), int(action.get("created_at") or time.time() * 1000),
                    action.get("resolved_at"), action.get("resolution"),
                ),
            )
            conn.commit()

    def get_pending_actions(self) -> List[Dict[str, Any]]:
        with self._get_connection() as conn:
            cur = conn.execute(
                "SELECT * FROM pending_actions WHERE state = 'PENDING' ORDER BY expected_execution_open_time ASC;"
            )
            return [dict(r) for r in cur.fetchall()]

    def resolve_pending_action(self, signal_id: str, resolution: str) -> None:
        with self._get_connection() as conn:
            conn.execute(
                "UPDATE pending_actions SET state = 'RESOLVED', resolved_at = ?, resolution = ? "
                "WHERE signal_id = ? AND state = 'PENDING';",
                (int(time.time() * 1000), resolution, signal_id),
            )
            conn.commit()

    # --- execution telemetry ----------------------------------------------

    def record_telemetry(self, payload: Dict[str, Any]) -> None:
        with self._get_connection() as conn:
            conn.execute(
                "INSERT INTO execution_telemetry (client_order_id, signal_id, benchmark_id, payload_json, created_at) "
                "VALUES (?, ?, ?, ?, ?);",
                (
                    payload.get("client_order_id"), payload.get("signal_id"),
                    payload.get("benchmark_id"), json.dumps(payload, default=str),
                    int(time.time() * 1000),
                ),
            )
            conn.commit()

    def get_telemetry(self, limit: int = 500) -> List[Dict[str, Any]]:
        with self._get_connection() as conn:
            cur = conn.execute(
                "SELECT payload_json FROM execution_telemetry ORDER BY telemetry_id ASC LIMIT ?;", (limit,)
            )
            return [json.loads(r["payload_json"]) for r in cur.fetchall()]

    # --- heartbeats and durable engine state -------------------------------

    def record_heartbeat(self, component: str, status: str = "OK", details: str = "") -> None:
        with self._get_connection() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO heartbeats (component, last_beat_ms, status, details) VALUES (?, ?, ?, ?);",
                (component, int(time.time() * 1000), status, details),
            )
            conn.commit()

    def get_heartbeats(self) -> Dict[str, Dict[str, Any]]:
        with self._get_connection() as conn:
            cur = conn.execute("SELECT * FROM heartbeats;")
            return {r["component"]: dict(r) for r in cur.fetchall()}

    def set_state(self, key: str, value: Any) -> None:
        with self._get_connection() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO engine_state (key, value_json, updated_at) VALUES (?, ?, ?);",
                (key, json.dumps(value, default=str), int(time.time() * 1000)),
            )
            conn.commit()

    def get_state(self, key: str, default: Any = None) -> Any:
        with self._get_connection() as conn:
            row = conn.execute("SELECT value_json FROM engine_state WHERE key = ?;", (key,)).fetchone()
            return json.loads(row["value_json"]) if row else default

    def get_state_updated_at(self, key: str) -> Optional[int]:
        with self._get_connection() as conn:
            row = conn.execute("SELECT updated_at FROM engine_state WHERE key = ?;", (key,)).fetchone()
            return int(row["updated_at"]) if row else None

    # --- bulk market-data persistence --------------------------------------

    def store_aggtrades_batch(self, trades: List[AggTrade]) -> int:
        """Stores many aggTrades in one transaction. Returns the number newly inserted.

        Gap recovery pulls thousands of trades at a time; one connection and one commit
        per trade turns recovery into a multi-minute stall.
        """
        if not trades:
            return 0
        rows = [
            (
                t.symbol, t.agg_trade_id, t.trade_time, t.event_time, t.received_at,
                str(t.price), str(t.quantity), t.first_trade_id, t.last_trade_id,
                int(t.buyer_is_market_maker),
            )
            for t in trades
        ]
        with self._get_connection() as conn:
            cur = conn.executemany(
                """
                INSERT OR IGNORE INTO raw_aggtrades (
                    symbol, agg_trade_id, trade_time, event_time, received_at,
                    price, quantity, first_trade_id, last_trade_id, buyer_is_market_maker
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                rows,
            )
            conn.commit()
            return cur.rowcount

    def get_aggtrade_ids(self, from_id: int, to_id: int) -> "set[int]":
        """Returns which aggTrade IDs in [from_id, to_id] are already durably stored."""
        with self._get_connection() as conn:
            cur = conn.execute(
                "SELECT agg_trade_id FROM raw_aggtrades WHERE agg_trade_id BETWEEN ? AND ?;",
                (from_id, to_id),
            )
            return {int(r["agg_trade_id"]) for r in cur.fetchall()}
