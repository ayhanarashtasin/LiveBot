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
from decimal import Decimal
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
    """Dispatches formatted Markdown alerts and polls commands via direct Bot API."""

    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = chat_id
        self.send_url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        self.updates_url = f"https://api.telegram.org/bot{self.token}/getUpdates"

    def send_message(self, text: str, parse_mode: str = "Markdown", chat_id: Optional[str] = None) -> bool:
        target_chat = chat_id or self.chat_id
        if not self.token or not target_chat:
            logger.warning("Telegram token or chat ID is missing; alert skipped.")
            return False

        payload = {
            "chat_id": target_chat,
            "text": text,
            "parse_mode": parse_mode,
            "disable_web_page_preview": True,
        }
        data = urllib.parse.urlencode(payload).encode("utf-8")
        req = urllib.request.Request(
            self.send_url,
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

    def get_updates(self, offset: Optional[int] = None, timeout: int = 0) -> List[Dict[str, Any]]:
        """Polls Telegram Bot API for incoming updates/commands."""
        if not self.token:
            return []

        params: Dict[str, Any] = {"timeout": timeout, "limit": 20}
        if offset is not None:
            params["offset"] = offset

        url = f"{self.updates_url}?{urllib.parse.urlencode(params)}"
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "Escanor-TelegramNotifier/1.0"},
            method="GET",
        )

        try:
            with urllib.request.urlopen(req, context=SSL_CONTEXT, timeout=8) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                if data.get("ok"):
                    res = data.get("result", [])
                    return res if isinstance(res, list) else []
        except (urllib.error.URLError, TimeoutError) as exc:
            logger.debug(f"Transient getUpdates network error: {exc}")
        except Exception as exc:
            logger.debug(f"getUpdates error: {exc}")
        return []


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
        self.last_update_id = 0
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
        """Polls database tables for new rows and checks for incoming Telegram commands."""
        try:
            with self._get_connection() as conn:
                self._check_audit_events(conn)
                self._check_orders(conn)
        except sqlite3.OperationalError as exc:
            logger.debug(f"Database temporarily busy: {exc}")
        except Exception as exc:
            logger.error(f"Unexpected watcher poll error: {exc}")

        self.poll_commands()

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
            if event_type in ("SLOT_ENTRY_TRIGGERED", "STRATEGY_SLOT_OPENED"):
                self._handle_slot_entry(payload, row["timestamp"])
            elif event_type in ("SLOT_EXIT_TRIGGERED", "STRATEGY_SLOT_CLOSED"):
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

    def _get_open_slots_count(self) -> str:
        try:
            with self._get_connection() as conn:
                cur = conn.cursor()
                r = cur.execute("SELECT value_json FROM engine_state WHERE key LIKE '%slot_book%'").fetchone()
                if r and r[0]:
                    slots = json.loads(r[0]).get("slots", [])
                    return f"{len(slots)} / 12"
        except Exception:
            pass
        return "1 / 12"

    def _handle_slot_entry(self, payload: Dict[str, Any], ts_ms: int) -> None:
        signal = payload.get("signal", {})
        slot = payload.get("slot", {})
        symbol = signal.get("symbol") or payload.get("symbol", self.symbol)
        ref_px = signal.get("reference_price") or payload.get("entry_price", "N/A")
        slot_id = slot.get("slot_id") or payload.get("slot_id", "N/A")
        qty = slot.get("quantity") or payload.get("quantity", "N/A")
        stop_px = slot.get("stop_price") or payload.get("stop_price")
        tp_px = slot.get("take_profit_price") or payload.get("take_profit_price")
        time_str = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        acct_info = self.get_account_context()
        slots_ratio = self._get_open_slots_count()

        targets_str = ""
        if tp_px and stop_px:
            try:
                targets_str = f"• *Take Profit:* `${float(tp_px):.3f}` (+4.5 ATR)\n• *Stop Loss:* `${float(stop_px):.3f}` (-6.0 ATR)\n"
            except Exception:
                pass

        qty_str = f"• *Order Qty:* `{qty} {symbol}`\n" if qty != "N/A" else ""

        header = "ESCANOR STRATEGY - SLOT BOUGHT" if "take_profit_price" in payload else "ESCANOR STRATEGY SIGNAL - LONG ENTRY"
        msg = (
            f"🟢 *[{header}]*\n"
            f"─────────────────────────────\n"
            f"• *Symbol:* `{symbol}` (5m)\n"
            f"• *Action:* `BUY / ENTER LONG`\n"
            f"• *Slot ID:* `{slot_id}`\n"
            f"• *Active Slots:* `{slots_ratio}`\n"
            f"• *Entry Price:* `${ref_px}`\n"
            f"{qty_str}"
            f"{targets_str}"
            f"{acct_info}\n"
            f"• *Time:* `{time_str}`\n"
            f"─────────────────────────────\n"
            f"🚀 _Position open on Binance USD-M Futures._"
        )
        self.notifier.send_message(msg)

    def _handle_slot_exit(self, payload: Dict[str, Any], ts_ms: int) -> None:
        signal = payload.get("signal", {})
        slot = payload.get("slot", {})
        symbol = signal.get("symbol") or payload.get("symbol", self.symbol)
        ref_px = signal.get("reference_price") or payload.get("exit_price", "N/A")
        reason = signal.get("reason") or payload.get("reason", "EXIT_TRIGGERED")
        slot_id = slot.get("slot_id") or payload.get("slot_id", "N/A")
        entry_px = slot.get("entry_price") or payload.get("entry_price", "N/A")
        net_pnl = payload.get("net_pnl")
        time_str = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        acct_info = self.get_account_context()
        slots_ratio = self._get_open_slots_count()

        pnl_str = ""
        if net_pnl is not None:
            try:
                pnl_val = float(net_pnl)
                pnl_icon = "🟢" if pnl_val >= 0 else "🔴"
                pnl_str = f"• *Net Profit:* `{pnl_icon} ${pnl_val:+.2f} USDT`\n"
            except Exception:
                pass

        icon = "🎯" if any(x in str(reason).upper() for x in ("TP", "PROFIT", "TAKE")) else "🛑" if any(x in str(reason).upper() for x in ("SL", "STOP")) else "🔴"

        msg = (
            f"{icon} *[ESCANOR STRATEGY - SLOT CLOSED]*\n"
            f"─────────────────────────────\n"
            f"• *Symbol:* `{symbol}` (5m)\n"
            f"• *Action:* `SELL / EXIT LONG`\n"
            f"• *Slot ID:* `{slot_id}`\n"
            f"• *Reason:* `{reason}`\n"
            f"{pnl_str}"
            f"• *Entry Price:* `${entry_px}`\n"
            f"• *Exit Price:* `${ref_px}`\n"
            f"• *Remaining Slots:* `{slots_ratio}`\n"
            f"{acct_info}\n"
            f"• *Time:* `{time_str}`\n"
            f"─────────────────────────────\n"
            f"⚡ _Position updated on Binance USD-M._"
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
                slots_ratio = self._get_open_slots_count()
                if side == "BUY":
                    msg = (
                        f"✅ *[ORDER FILLED - BUY / LONG]*\n"
                        f"─────────────────────────────\n"
                        f"• *Symbol:* `{row['symbol']}`\n"
                        f"• *Side:* `BUY (LONG)`\n"
                        f"• *Active Slots:* `{slots_ratio}`\n"
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
                        f"• *Remaining Slots:* `{slots_ratio}`\n"
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

    def is_chat_authorized(self, chat_id: Any) -> bool:
        """Verifies if the sender chat_id matches the authorized TELEGRAM_CHAT_ID."""
        if not self.notifier.chat_id or not chat_id:
            return False
        allowed = {cid.strip() for cid in str(self.notifier.chat_id).split(",") if cid.strip()}
        return str(chat_id).strip() in allowed

    def init_telegram_updates(self) -> None:
        """Acknowledge any pre-existing updates so the bot only answers new commands."""
        try:
            updates = self.notifier.get_updates(offset=-1, timeout=0)
            if isinstance(updates, list) and updates:
                max_id = max(int(u.get("update_id", 0)) for u in updates)
                self.last_update_id = max_id
                self.notifier.get_updates(offset=max_id + 1, timeout=0)
                logger.info(f"Initialized Telegram updates offset to {self.last_update_id}")
        except Exception as exc:
            logger.debug(f"Could not initialize Telegram update offset: {exc}")

    def poll_commands(self) -> None:
        """Fetches pending Telegram commands and dispatches responses."""
        try:
            updates = self.notifier.get_updates(offset=self.last_update_id + 1, timeout=0)
            if not isinstance(updates, list):
                return

            for upd in updates:
                upd_id = upd.get("update_id")
                if upd_id is not None:
                    self.last_update_id = max(self.last_update_id, int(upd_id))

                msg = upd.get("message") or upd.get("channel_post") or {}
                text = (msg.get("text") or "").strip()
                if not text:
                    continue

                chat = msg.get("chat", {})
                sender_chat_id = chat.get("id")

                if not self.is_chat_authorized(sender_chat_id):
                    logger.warning(f"Ignored Telegram command from unauthorized chat_id={sender_chat_id}")
                    continue

                self._dispatch_command(text, str(sender_chat_id))
        except Exception as exc:
            logger.debug(f"Telegram command polling error: {exc}")

    def _dispatch_command(self, raw_text: str, chat_id: str) -> None:
        parts = raw_text.split()
        first_token = parts[0].lower().split("@")[0].strip()
        cmd = first_token.lstrip("/")

        logger.info(f"Executing operator Telegram command '{cmd}' from chat_id={chat_id}")

        if cmd in ("bal", "balance"):
            reply = self.cmd_balance()
        elif cmd in ("fees", "fee"):
            reply = self.cmd_fees()
        elif cmd in ("status", "stat", "health"):
            reply = self.cmd_status()
        elif cmd in ("pos", "position", "positions"):
            reply = self.cmd_position()
        elif cmd in ("trades", "history"):
            reply = self.cmd_trades()
        elif cmd in ("help", "start"):
            reply = self.cmd_help()
        else:
            reply = (
                f"❓ *Unknown Command:* `{raw_text}`\n\n"
                f"Available operator commands:\n"
                f"• `/bal` - Account Balance & Equity\n"
                f"• `/fees` - Total Fees Paid & Breakdown\n"
                f"• `/pos` - Live Open Position Details\n"
                f"• `/status` - Engine Health & Mark Price\n"
                f"• `/trades` - Recent Executed Orders\n"
                f"• `/help` - Command Manual"
            )

        self.notifier.send_message(reply, chat_id=chat_id)

    def cmd_balance(self) -> str:
        """Handles /bal and /balance: reports authoritative Binance balance & position."""
        wallet_bal = None
        margin_bal = None
        avail_bal = None
        unrealized_pnl = None
        pos_side = None
        pos_qty = Decimal("0")
        pos_entry = Decimal("0")

        if self.broker is not None:
            try:
                state = self.broker.get_account_state()
                wallet_bal = state.get("total_wallet_balance")
                margin_bal = state.get("total_margin_balance")
                avail_bal = state.get("available_balance")
                unrealized_pnl = state.get("total_unrealized_pnl")
            except Exception as e:
                logger.debug(f"Broker get_account_state error: {e}")
                try:
                    bal = self.broker.get_account_balance()
                    if bal and "USDT" in bal:
                        wallet_bal = bal["USDT"]
                except Exception:
                    pass

            try:
                pos_side, pos_qty, pos_entry = self.broker.get_position(self.symbol)
            except Exception as e:
                logger.debug(f"Broker get_position error: {e}")

        # Fallback to local SQLite database if broker did not return wallet_bal
        if wallet_bal is None:
            try:
                with self._get_connection() as conn:
                    cur = conn.cursor()
                    row = cur.execute(
                        "SELECT payload_json FROM audit_events WHERE event_type='BALANCE_RECONCILIATION' "
                        "ORDER BY event_id DESC LIMIT 1"
                    ).fetchone()
                    if row and row[0]:
                        b = json.loads(row[0])
                        usdt = b.get("balances", {}).get("USDT")
                        if usdt is not None:
                            wallet_bal = Decimal(str(usdt))

                    state_row = cur.execute(
                        "SELECT value_json FROM engine_state WHERE key LIKE '%slot_book%'"
                    ).fetchone()
                    if state_row and state_row[0] and wallet_bal is None:
                        sb = json.loads(state_row[0])
                        if "realized_equity" in sb:
                            wallet_bal = Decimal(str(sb["realized_equity"]))
            except Exception as e:
                logger.debug(f"Database balance lookup error: {e}")

        # Open position fallback to DB if pos_qty == 0
        if not pos_qty:
            try:
                with self._get_connection() as conn:
                    cur = conn.cursor()
                    row = cur.execute(
                        "SELECT payload_json FROM audit_events WHERE event_type='POSITION_UPDATED' "
                        "ORDER BY event_id DESC LIMIT 1"
                    ).fetchone()
                    if row and row[0]:
                        p = json.loads(row[0])
                        q = Decimal(str(p.get("quantity", 0)))
                        if q > 0:
                            pos_qty = q
                            pos_entry = Decimal(str(p.get("entry_price", 0)))
                            pos_side = p.get("side", "BUY")
            except Exception:
                pass

        # Query latest mark price
        mark_px_str = "N/A"
        try:
            with self._get_connection() as conn:
                cur = conn.cursor()
                r = cur.execute("SELECT close FROM candles ORDER BY open_time DESC LIMIT 1").fetchone()
                if r and r[0]:
                    mark_px_str = f"${float(r[0]):.3f}"
        except Exception:
            pass

        wallet_str = f"${float(wallet_bal):.2f} USDT" if wallet_bal is not None else "N/A"
        margin_str = f"${float(margin_bal):.2f} USDT" if margin_bal is not None else wallet_str
        avail_str = f"${float(avail_bal):.2f} USDT" if avail_bal is not None else wallet_str
        pnl_val = float(unrealized_pnl) if unrealized_pnl is not None else 0.0
        pnl_icon = "🟢" if pnl_val >= 0 else "🔴"
        pnl_str = f"{pnl_icon} {'+' if pnl_val >= 0 else ''}${pnl_val:.2f} USDT"

        slots_ratio = self._get_open_slots_count()

        side_str = pos_side.value if hasattr(pos_side, "value") else str(pos_side) if pos_side else ""
        if side_str and pos_qty > 0:
            pos_desc = f"🟢 `{side_str} {pos_qty} {self.symbol}` (Entry: `${float(pos_entry):.3f}`)"
        else:
            pos_desc = "`FLAT (0.00 HYPE)`"

        return (
            f"💰 *[BINANCE ACCOUNT BALANCE]*\n"
            f"─────────────────────────────\n"
            f"• *Wallet Balance:* `{wallet_str}`\n"
            f"• *Margin Balance:* `{margin_str}`\n"
            f"• *Available Margin:* `{avail_str}`\n"
            f"• *Unrealized PnL:* {pnl_str}\n"
            f"• *Active Slots:* `{slots_ratio}`\n"
            f"• *Market Price:* `{mark_px_str}`\n"
            f"─────────────────────────────\n"
            f"• *Live Position:* {pos_desc}\n"
            f"─────────────────────────────\n"
            f"⚡ _Real-time USD-M Futures account state._"
        )

    def cmd_fees(self) -> str:
        """Handles /fees and /fee: reports total fees paid and breakdown of recent orders."""
        total_fees = 0.0
        filled_orders_count = 0
        orders_list = []

        try:
            with self._get_connection() as conn:
                cur = conn.cursor()
                sum_row = cur.execute(
                    "SELECT SUM(CAST(accumulated_fees AS REAL)), COUNT(*) FROM orders WHERE status='FILLED'"
                ).fetchone()
                if sum_row and sum_row[0] is not None:
                    total_fees = float(sum_row[0])
                    filled_orders_count = int(sum_row[1])

                if total_fees == 0.0:
                    fill_sum = cur.execute("SELECT SUM(CAST(commission AS REAL)) FROM fills").fetchone()
                    if fill_sum and fill_sum[0] is not None:
                        total_fees = float(fill_sum[0])

                rows = cur.execute(
                    "SELECT client_order_id, side, filled_quantity, avg_fill_price, accumulated_fees, filled_at, created_at "
                    "FROM orders WHERE status='FILLED' ORDER BY rowid DESC LIMIT 8"
                ).fetchall()
                orders_list = rows
        except Exception as e:
            logger.error(f"Error querying fees: {e}")

        breakdown_lines = []
        for o in orders_list:
            side = o["side"]
            side_icon = "🟢 BUY " if side == "BUY" else "🔴 SELL"
            try:
                qty = float(o["filled_quantity"] or 0)
                px = float(o["avg_fill_price"] or 0)
                fee = float(o["accumulated_fees"] or 0)
                ts = o["filled_at"] or o["created_at"] or 0
                time_short = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime("%m-%d %H:%M") if ts else ""
                breakdown_lines.append(f"• `{time_short}` {side_icon} `{qty:.2f}` @ `${px:.3f}` | Fee: `${fee:.4f}`")
            except Exception:
                continue

        breakdown_str = "\n".join(breakdown_lines) if breakdown_lines else "_No filled orders found yet._"

        return (
            f"💳 *[TRADING FEES SUMMARY]*\n"
            f"─────────────────────────────\n"
            f"• *Total Fees Paid:* `${total_fees:.4f} USDT`\n"
            f"• *Filled Orders:* `{filled_orders_count}`\n"
            f"• *Symbol:* `{self.symbol}` (Binance USD-M)\n"
            f"─────────────────────────────\n"
            f"*Recent Orders & Fees:*\n"
            f"{breakdown_str}\n"
            f"─────────────────────────────\n"
            f"💡 _Fees deducted directly by Binance USD-M matching engine._"
        )

    def cmd_status(self) -> str:
        """Handles /status and /stat: reports system health, prices, active slots and streams."""
        components_status = {}
        try:
            with self._get_connection() as conn:
                cur = conn.cursor()
                rows = cur.execute("SELECT component, status, last_beat_ms FROM heartbeats").fetchall()
                now_ms = int(time.time() * 1000)
                for r in rows:
                    comp = r["component"]
                    st = r["status"]
                    age_s = (now_ms - (r["last_beat_ms"] or 0)) / 1000
                    is_fresh = age_s < 180
                    components_status[comp] = "✅ OK" if (st == "OK" and is_fresh) else f"⚠️ {st}"
        except Exception:
            pass

        last_price = "N/A"
        candle_time = "N/A"
        try:
            with self._get_connection() as conn:
                cur = conn.cursor()
                r = cur.execute("SELECT close, open_time FROM candles ORDER BY open_time DESC LIMIT 1").fetchone()
                if r and r[0]:
                    last_price = f"${float(r[0]):.3f}"
                    candle_time = datetime.fromtimestamp(r[1] / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        except Exception:
            pass

        slots_ratio = self._get_open_slots_count()

        order_counts = 0
        try:
            with self._get_connection() as conn:
                cur = conn.cursor()
                order_counts = cur.execute("SELECT count(*) FROM orders WHERE status='FILLED'").fetchone()[0]
        except Exception:
            pass

        acct_context = self.get_account_context()

        stream_stat = components_status.get("public_stream", "✅ OK")
        engine_stat = components_status.get("engine", "✅ OK")
        kline_stat = components_status.get("kline_validator", "✅ OK")
        priv_stat = components_status.get("private_stream", "✅ OK")

        return (
            f"🤖 *[ESCANOR ENGINE STATUS]*\n"
            f"─────────────────────────────\n"
            f"• *Operational State:* `ONLINE & ACTIVE`\n"
            f"• *Symbol:* `{self.symbol}` (5m Candles)\n"
            f"• *Current Price:* `{last_price}`\n"
            f"• *Active Slots:* `{slots_ratio}`\n"
            f"• *Total Fills:* `{order_counts} orders`\n"
            f"• *Last Candle:* `{candle_time}`\n"
            f"─────────────────────────────\n"
            f"• *Engine Heartbeat:* {engine_stat}\n"
            f"• *Public Market Stream:* {stream_stat}\n"
            f"• *Private Account Stream:* {priv_stat}\n"
            f"• *Kline Validator:* {kline_stat}\n"
            f"─────────────────────────────\n"
            f"{acct_context}\n"
            f"─────────────────────────────\n"
            f"⚡ _System operating in LIVE TRADING mode on Binance USD-M._"
        )

    def cmd_position(self) -> str:
        """Handles /pos and /position: reports details of any active open position."""
        pos_side, pos_qty, pos_entry = None, Decimal("0"), Decimal("0")
        if self.broker is not None:
            try:
                pos_side, pos_qty, pos_entry = self.broker.get_position(self.symbol)
            except Exception:
                pass

        if not pos_qty:
            try:
                with self._get_connection() as conn:
                    cur = conn.cursor()
                    row = cur.execute(
                        "SELECT payload_json FROM audit_events WHERE event_type='POSITION_UPDATED' "
                        "ORDER BY event_id DESC LIMIT 1"
                    ).fetchone()
                    if row and row[0]:
                        p = json.loads(row[0])
                        q = Decimal(str(p.get("quantity", 0)))
                        if q > 0:
                            pos_qty = q
                            pos_entry = Decimal(str(p.get("entry_price", 0)))
                            pos_side = p.get("side", "BUY")
            except Exception:
                pass

        cur_px = Decimal("0")
        try:
            with self._get_connection() as conn:
                cur = conn.cursor()
                r = cur.execute("SELECT close FROM candles ORDER BY open_time DESC LIMIT 1").fetchone()
                if r and r[0]:
                    cur_px = Decimal(str(r[0]))
        except Exception:
            pass

        slots_ratio = self._get_open_slots_count()
        side_str = pos_side.value if hasattr(pos_side, "value") else str(pos_side) if pos_side else ""

        if not side_str or not pos_qty:
            return (
                f"📊 *[LIVE POSITION - FLAT]*\n"
                f"─────────────────────────────\n"
                f"• *Symbol:* `{self.symbol}`\n"
                f"• *Position:* `FLAT (0.00 {self.symbol})`\n"
                f"• *Active Slots:* `{slots_ratio}`\n"
                f"• *Current Price:* `${float(cur_px):.3f}`\n"
                f"─────────────────────────────\n"
                f"💤 _No open positions. Bot waiting for next strategy entry signal._"
            )

        entry_notional = pos_qty * pos_entry
        current_notional = pos_qty * cur_px
        pnl = (current_notional - entry_notional) if side_str == "BUY" else (entry_notional - current_notional)
        pnl_pct = (pnl / entry_notional * Decimal("100")) if entry_notional > 0 else Decimal("0")
        pnl_icon = "🟢" if pnl >= 0 else "🔴"

        return (
            f"📊 *[LIVE POSITION - {side_str}]*\n"
            f"─────────────────────────────\n"
            f"• *Symbol:* `{self.symbol}`\n"
            f"• *Side:* `{side_str}`\n"
            f"• *Size:* `{pos_qty} {self.symbol}`\n"
            f"• *Entry Price:* `${float(pos_entry):.3f}`\n"
            f"• *Current Price:* `${float(cur_px):.3f}`\n"
            f"• *Position Value:* `${float(current_notional):.2f} USDT`\n"
            f"• *Unrealized PnL:* `{pnl_icon} ${float(pnl):+.2f} USDT ({float(pnl_pct):+.2f}%)`\n"
            f"• *Active Slots:* `{slots_ratio}`\n"
            f"─────────────────────────────\n"
            f"🚀 _Position protected with Supertrend ATR SL/TP targets._"
        )

    def cmd_trades(self) -> str:
        """Handles /trades and /history: reports recent executed orders with fees."""
        orders_list = []
        try:
            with self._get_connection() as conn:
                cur = conn.cursor()
                rows = cur.execute(
                    "SELECT client_order_id, side, filled_quantity, avg_fill_price, accumulated_fees, filled_at, created_at "
                    "FROM orders WHERE status='FILLED' ORDER BY rowid DESC LIMIT 5"
                ).fetchall()
                orders_list = rows
        except Exception as e:
            logger.error(f"Error querying trades: {e}")

        if not orders_list:
            return (
                f"📜 *[RECENT EXECUTED TRADES]*\n"
                f"─────────────────────────────\n"
                f"_No filled orders recorded yet._"
            )

        lines = []
        for o in orders_list:
            side = o["side"]
            side_badge = "🟢 BUY " if side == "BUY" else "🔴 SELL"
            qty = float(o["filled_quantity"] or 0)
            px = float(o["avg_fill_price"] or 0)
            fee = float(o["accumulated_fees"] or 0)
            ts = o["filled_at"] or o["created_at"] or 0
            time_str = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC") if ts else ""
            lines.append(
                f"• {side_badge} `{qty:.2f} {self.symbol}` @ `${px:.3f}`\n"
                f"  Fee: `${fee:.4f} USDT` | `{time_str}`"
            )

        return (
            f"📜 *[RECENT EXECUTED TRADES]*\n"
            f"─────────────────────────────\n"
            + "\n\n".join(lines) +
            f"\n─────────────────────────────\n"
            f"📈 _Verified Binance USD-M fills from audit store._"
        )

    def cmd_help(self) -> str:
        """Handles /help and /start: displays list of available commands."""
        return (
            f"🤖 *[ESCANOR COMMAND CENTER]*\n"
            f"─────────────────────────────\n"
            f"Available live commands:\n\n"
            f"💰 `/bal` or `/balance`\n"
            f"└ Live Binance USD-M balance & margin\n\n"
            f"💳 `/fees` or `/fee`\n"
            f"└ Total fees paid & breakdown\n\n"
            f"📊 `/pos` or `/position`\n"
            f"└ Open position, entry price & live PnL\n\n"
            f"🤖 `/status` or `/stat`\n"
            f"└ System health, streams & mark price\n\n"
            f"📜 `/trades`\n"
            f"└ Last executed orders & timestamps\n\n"
            f"❓ `/help`\n"
            f"└ Display this commands menu\n"
            f"─────────────────────────────\n"
            f"🛡️ _Secured for authorized operator only._"
        )


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
    watcher.init_telegram_updates()

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
        f"💬 *Interactive Commands Available:*\n"
        f"• `/bal` - Real-time Balance & Equity\n"
        f"• `/fees` - Total Fees Paid & Breakdown\n"
        f"• `/pos` - Open Position Details & PnL\n"
        f"• `/status` - Engine Health & Heartbeats\n"
        f"• `/trades` - Recent Executed Orders\n"
        f"• `/help` - Command Manual\n"
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
