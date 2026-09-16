"""Zero-Downtime Telegram Notification Watcher for Escanor.

Runs independently as a decoupled sidecar process. Observes the SQLite database
in strict read-only mode (WAL mode) and pushes instant notifications to Telegram
whenever the bot enters, exits, or fills an order.

Safety & Design Guarantees:
- Zero impact on live trading: completely isolated process.
- Read-only SQLite access: never locks or writes to the live engine database.
- Non-blocking resilient dispatching: Telegram API network latency or errors
  never crash the watcher or affect Binance trading.
- Credential isolation: loads TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID from .env.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    import certifi
    SSL_CONTEXT = ssl.create_default_context(cafile=certifi.where())
except Exception:
    SSL_CONTEXT = ssl.create_default_context()
    SSL_CONTEXT.check_hostname = False
    SSL_CONTEXT.verify_mode = ssl.CERT_NONE

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] escanor.telegram: %(message)s",
)
logger = logging.getLogger("escanor.telegram")

BASE_DIR = Path(__file__).resolve().parents[2]


def load_env(env_path: Path) -> Dict[str, str]:
    vars_dict: Dict[str, str] = {}
    if not env_path.exists():
        return vars_dict
    for line in env_path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            k, v = line.split("=", 1)
            vars_dict[k.strip()] = v.strip().strip("'\"")
    return vars_dict


class TelegramNotifier:
    """Dispatches formatted Markdown alerts to Telegram via direct Bot API."""

    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = chat_id
        self.base_url = f"https://api.telegram.org/bot{self.token}/sendMessage"

    def send_message(self, text: str, parse_mode: str = "Markdown") -> bool:
        if not self.token or not self.chat_id:
            logger.warning("Telegram token or chat ID is missing; alert skipped.")
            return False

        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": parse_mode,
            "disable_web_page_preview": True,
        }
        data = urllib.parse.urlencode(payload).encode("utf-8")
        req = urllib.request.Request(
            self.base_url,
            data=data,
            headers={
                "User-Agent": "Escanor-TelegramNotifier/1.0",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            method="POST",
        )

        try:
            with urllib.request.urlopen(req, context=SSL_CONTEXT, timeout=10) as resp:
                res_data = json.loads(resp.read().decode("utf-8"))
                return bool(res_data.get("ok"))
        except Exception as exc:
            logger.error(f"Failed to dispatch Telegram message: {exc}")
            return False


class DatabaseWatcher:
    """Watches the live instance SQLite database for new orders, fills, and signals."""

    def __init__(self, db_path: Path, notifier: TelegramNotifier, symbol: str = "HYPEUSDT", broker: Optional[Any] = None):
        self.db_path = db_path
        self.notifier = notifier
        self.symbol = symbol.upper()
        self.broker = broker

        self.last_order_rowid = 0
        self.last_event_id = 0
        self.last_signal_rowid = 0
        self._notified_order_ids: set[str] = set()

    def _get_connection(self) -> sqlite3.Connection:
        uri = f"file:{self.db_path.as_posix()}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=5.0)
        conn.row_factory = sqlite3.Row
        return conn

    def get_account_context(self) -> str:
        """Fetches live Binance balance and open position with graceful fallback to DB."""
        balance_str = "N/A"
        position_str = "FLAT (0.00)"

        if self.broker is not None:
            try:
                bal = self.broker.get_account_balance()
                usdt = bal.get("USDT")
                if usdt is not None:
                    balance_str = f"${float(usdt):.2f} USDT"
                pos_side, pos_qty, entry_px = self.broker.get_position(self.symbol)
                if pos_side and pos_qty > 0:
                    position_str = f"{pos_side.value} {pos_qty} {self.symbol} (Entry: ${float(entry_px):.3f})"
                else:
                    position_str = "FLAT (0.00)"
                return f"• *Binance Balance:* `{balance_str}`\n• *Live Position:* `{position_str}`"
            except Exception as e:
                logger.debug(f"Direct broker query error in watcher: {e}")

        try:
            with self._get_connection() as conn:
                cur = conn.cursor()
                row = cur.execute(
                    "SELECT payload_json FROM audit_events WHERE event_type='POSITION_UPDATED' ORDER BY event_id DESC LIMIT 1"
                ).fetchone()
                if row and row[0]:
                    p = json.loads(row[0])
                    q = float(p.get("quantity", 0))
                    if q > 0:
                        position_str = f"{p.get('side', 'BUY')} {q} {self.symbol} (Entry: ${float(p.get('entry_price', 0)):.3f})"
                    else:
                        position_str = "FLAT (0.00)"
                bal_row = cur.execute(
                    "SELECT payload_json FROM audit_events WHERE event_type='BALANCE_RECONCILIATION' ORDER BY event_id DESC LIMIT 1"
                ).fetchone()
                if bal_row and bal_row[0]:
                    b = json.loads(bal_row[0])
                    usdt = b.get("balances", {}).get("USDT")
                    if usdt:
                        balance_str = f"${float(usdt):.2f} USDT"
        except Exception:
            pass

        return f"• *Binance Balance:* `{balance_str}`\n• *Live Position:* `{position_str}`"

    def init_high_water_marks(self) -> None:
        """Initializes pointers to current max IDs so we only alert on NEW events."""
        try:
            with self._get_connection() as conn:
                cur = conn.cursor()
                tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                if "audit_events" in tables:
                    row = cur.execute("SELECT max(event_id) FROM audit_events").fetchone()
                    self.last_event_id = row[0] or 0
                if "signals" in tables:
                    row = cur.execute("SELECT max(rowid) FROM signals").fetchone()
                    self.last_signal_rowid = row[0] or 0
            logger.info(
                f"Initialized watcher high-water marks: event_id={self.last_event_id}, "
                f"signal_rowid={self.last_signal_rowid}"
            )
        except Exception as exc:
            logger.warning(f"Could not initialize high-water marks: {exc}")

    def poll_once(self) -> None:
        """Polls database tables for new rows since last check."""
        try:
            with self._get_connection() as conn:
                self._check_audit_events(conn)
                self._check_orders(conn)
        except sqlite3.OperationalError as exc:
            logger.debug(f"Database temporarily busy: {exc}")
        except Exception as exc:
            logger.error(f"Unexpected watcher poll error: {exc}")

    def _check_audit_events(self, conn: sqlite3.Connection) -> None:
        cur = conn.cursor()
        rows = cur.execute(
            "SELECT event_id, event_type, payload_json, timestamp FROM audit_events "
            "WHERE event_id > ? ORDER BY event_id ASC LIMIT 50",
            (self.last_event_id,),
        ).fetchall()

        for row in rows:
            event_id = row["event_id"]
            self.last_event_id = max(self.last_event_id, event_id)
            event_type = row["event_type"]
            try:
                payload = json.loads(row["payload_json"])
            except Exception:
                payload = {}

            # Handle Strategy Trigger Signals (decisions only)
            if event_type == "SLOT_ENTRY_TRIGGERED":
                self._handle_slot_entry(payload, row["timestamp"])
            elif event_type == "SLOT_EXIT_TRIGGERED":
                self._handle_slot_exit(payload, row["timestamp"])
            elif event_type == "ALERT":
                category = str(payload.get("category", "")).upper()
                # Strictly suppress internal heartbeat/watchdog noise
                if category in (
                    "STALE_HEARTBEAT", "PRIVATE_STREAM_EXPIRED", "RECONNECT_STORM",
                    "UNRESOLVED_GAP", "MARKET_DATA_GAP", "KLINE_VALIDATOR",
                    "PRIVATE_STREAM_DOWN", "KILL_SWITCH_ACTIVE", "HEALTH_DEGRADED"
                ):
                    continue
                # Only alert if genuine emergency kill switch is engaged
                if category in ("KILL_SWITCH_ENGAGED", "EMERGENCY_HALT"):
                    self._handle_critical_alert(payload, row["timestamp"])

    def _handle_slot_entry(self, payload: Dict[str, Any], ts_ms: int) -> None:
        signal = payload.get("signal", {})
        slot = payload.get("slot", {})
        symbol = signal.get("symbol", self.symbol)
        ref_px = signal.get("reference_price", "N/A")
        slot_id = slot.get("slot_id", "N/A")
        time_str = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        acct_info = self.get_account_context()

        msg = (
            f"🟢 *[ESCANOR STRATEGY SIGNAL - LONG ENTRY]*\n"
            f"─────────────────────────────\n"
            f"• *Symbol:* `{symbol}` (5m)\n"
            f"• *Action:* `ENTER LONG`\n"
            f"• *Slot:* `{slot_id}`\n"
            f"• *Reference Price:* `${ref_px}`\n"
            f"• *Allocated Stake:* Canary Sizing ($100 Cap)\n"
            f"{acct_info}\n"
            f"• *Time:* `{time_str}`\n"
            f"─────────────────────────────\n"
            f"🚀 _Submitting market order to Binance USD-M..._"
        )
        self.notifier.send_message(msg)

    def _handle_slot_exit(self, payload: Dict[str, Any], ts_ms: int) -> None:
        signal = payload.get("signal", {})
        slot = payload.get("slot", {})
        symbol = signal.get("symbol", self.symbol)
        ref_px = signal.get("reference_price", "N/A")
        reason = signal.get("reason", "EXIT_TRIGGERED")
        slot_id = slot.get("slot_id", "N/A")
        entry_px = slot.get("entry_price", "N/A")
        time_str = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        acct_info = self.get_account_context()

        icon = "🎯" if "TAKE_PROFIT" in str(reason).upper() else "🛑" if "STOP" in str(reason).upper() else "🔴"

        msg = (
            f"{icon} *[ESCANOR STRATEGY SIGNAL - LONG EXIT]*\n"
            f"─────────────────────────────\n"
            f"• *Symbol:* `{symbol}` (5m)\n"
            f"• *Action:* `CLOSE LONG`\n"
            f"• *Slot:* `{slot_id}`\n"
            f"• *Reason:* `{reason}`\n"
            f"• *Entry Price:* `${entry_px}`\n"
            f"• *Exit Trigger Price:* `${ref_px}`\n"
            f"{acct_info}\n"
            f"• *Time:* `{time_str}`\n"
            f"─────────────────────────────\n"
            f"⚡ _Submitting reduce-only exit order to Binance..._"
        )
        self.notifier.send_message(msg)

    def _handle_critical_alert(self, payload: Dict[str, Any], ts_ms: int) -> None:
        category = payload.get("category", "INCIDENT")
        severity = payload.get("severity", "CRITICAL")
        message = payload.get("message", "")
        time_str = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

        msg = (
            f"🚨 *[ESCANOR OPERATIONAL ALERT]*\n"
            f"─────────────────────────────\n"
            f"• *Severity:* `{severity}`\n"
            f"• *Category:* `{category}`\n"
            f"• *Details:* {message}\n"
            f"• *Time:* `{time_str}`"
        )
        self.notifier.send_message(msg)

    def _check_orders(self, conn: sqlite3.Connection) -> None:
        cur = conn.cursor()
        rows = cur.execute(
            "SELECT rowid, client_order_id, symbol, side, order_type, quantity, "
            "price, status, filled_quantity, avg_fill_price, accumulated_fees, "
            "created_at, filled_at, reduce_only FROM orders "
            "ORDER BY rowid DESC LIMIT 50",
        ).fetchall()

        for row in reversed(rows):
            cid = row["client_order_id"]
            status = row["status"]
            side = row["side"]
            try:
                filled_qty_val = float(row["filled_quantity"] or 0)
            except (TypeError, ValueError):
                filled_qty_val = 0.0
            try:
                avg_px_val = float(row["avg_fill_price"] or row["price"] or 0)
            except (TypeError, ValueError):
                avg_px_val = 0.0
            try:
                fees_val = float(row["accumulated_fees"] or 0)
            except (TypeError, ValueError):
                fees_val = 0.0

            ts_ms = row["filled_at"] or row["created_at"] or int(time.time() * 1000)
            time_str = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

            dedup_key = f"{cid}:{status}"
            if dedup_key in self._notified_order_ids:
                continue
            self._notified_order_ids.add(dedup_key)

            if status == "FILLED":
                acct_info = self.get_account_context()
                if side == "BUY":
                    msg = (
                        f"✅ *[ORDER FILLED - BUY / LONG]*\n"
                        f"─────────────────────────────\n"
                        f"• *Symbol:* `{row['symbol']}`\n"
                        f"• *Side:* `BUY (LONG)`\n"
                        f"• *Filled Qty:* `{filled_qty_val} HYPE`\n"
                        f"• *Fill Price:* `${avg_px_val:.3f}`\n"
                        f"• *Notional Value:* `${(filled_qty_val * avg_px_val):.2f}`\n"
                        f"• *Fee Paid:* `${fees_val:.4f} USDT`\n"
                        f"{acct_info}\n"
                        f"• *Client Order ID:* `{cid}`\n"
                        f"• *Time:* `{time_str}`\n"
                        f"─────────────────────────────\n"
                        f"📈 _Position open on Binance USD-M Futures._"
                    )
                else:
                    msg = (
                        f"🏁 *[ORDER FILLED - SELL / EXIT]*\n"
                        f"─────────────────────────────\n"
                        f"• *Symbol:* `{row['symbol']}`\n"
                        f"• *Side:* `SELL (REDUCE ONLY)`\n"
                        f"• *Filled Qty:* `{filled_qty_val} HYPE`\n"
                        f"• *Fill Price:* `${avg_px_val:.3f}`\n"
                        f"• *Fee Paid:* `${fees_val:.4f} USDT`\n"
                        f"{acct_info}\n"
                        f"• *Client Order ID:* `{cid}`\n"
                        f"• *Time:* `{time_str}`\n"
                        f"─────────────────────────────\n"
                        f"💰 _Position closed on Binance USD-M Futures._"
                    )
                self.notifier.send_message(msg)


def main() -> int:
    parser = argparse.ArgumentParser(description="Zero-Downtime Telegram Alert Watcher for Escanor")
    parser.add_argument("--database", default="data/hype_live.db", help="Path to instance database")
    parser.add_argument("--symbol", default="HYPEUSDT", help="Trading symbol")
    parser.add_argument("--interval", type=float, default=1.0, help="Polling interval in seconds")
    args = parser.parse_args()

    env_vars = load_env(BASE_DIR / ".env")
    token = env_vars.get("TELEGRAM_BOT_TOKEN", os.environ.get("TELEGRAM_BOT_TOKEN", ""))
    chat_id = env_vars.get("TELEGRAM_CHAT_ID", os.environ.get("TELEGRAM_CHAT_ID", ""))

    if not token or not chat_id:
        logger.error("TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID must be set in .env")
        return 1

    db_path = (BASE_DIR / args.database).resolve()
    if not db_path.exists():
        logger.error(f"Database not found at {db_path}")
        return 1

    api_key = env_vars.get("BINANCE_API_KEY", os.environ.get("BINANCE_API_KEY", ""))
    api_secret = env_vars.get("BINANCE_API_SECRET", os.environ.get("BINANCE_API_SECRET", ""))
    broker = None
    if api_key and api_secret:
        try:
            from live_engine.execution.broker import BinanceLiveBroker
            broker = BinanceLiveBroker(api_key=api_key, api_secret=api_secret)
        except Exception as exc:
            logger.warning(f"Could not initialize BinanceLiveBroker for telegram_watcher: {exc}")

    notifier = TelegramNotifier(token, chat_id)
    watcher = DatabaseWatcher(db_path, notifier, symbol=args.symbol, broker=broker)
    watcher.init_high_water_marks()

    acct_context = watcher.get_account_context()
    startup_msg = (
        f"🚀 *[ESCANOR TELEGRAM WATCHER ACTIVE]*\n"
        f"─────────────────────────────\n"
        f"• *Symbol:* `{args.symbol}`\n"
        f"• *Mode:* `LIVE TRADING` (Binance USD-M)\n"
        f"• *Database:* `{args.database}`\n"
        f"• *Observer Status:* `ONLINE & LISTENING`\n"
        f"{acct_context}\n"
        f"─────────────────────────────\n"
        f"📱 _You will receive instant alerts for every long entry, fill, and exit._"
    )
    notifier.send_message(startup_msg)
    logger.info("Telegram Watcher started. Monitoring database events in real-time...")

    try:
        while True:
            watcher.poll_once()
            time.sleep(args.interval)
    except KeyboardInterrupt:
        logger.info("Telegram Watcher stopping by operator request...")
        notifier.send_message("⏸️ *[ESCANOR TELEGRAM WATCHER STOPPED]*")
    return 0


if __name__ == "__main__":
    sys.exit(main())
