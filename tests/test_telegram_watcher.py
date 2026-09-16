"""Unit tests for telegram_watcher alert filtering."""
import json
import sqlite3
from unittest.mock import MagicMock
from pathlib import Path

from live_engine.monitoring.telegram_watcher import DatabaseWatcher, TelegramNotifier


def test_telegram_watcher_filters_stale_heartbeats(tmp_path: Path):
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE audit_events (
            event_id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            timestamp INTEGER NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE orders (
            rowid INTEGER PRIMARY KEY AUTOINCREMENT,
            client_order_id TEXT,
            symbol TEXT,
            side TEXT,
            order_type TEXT,
            quantity REAL,
            price REAL,
            status TEXT,
            filled_quantity REAL,
            avg_fill_price REAL,
            accumulated_fees REAL,
            created_at INTEGER,
            filled_at INTEGER,
            reduce_only INTEGER
        )
    """)
    conn.commit()
    conn.close()

    notifier = MagicMock(spec=TelegramNotifier)
    watcher = DatabaseWatcher(db_path=db_path, symbol="HYPEUSDT", notifier=notifier)
    watcher.last_event_id = 0

    # Insert a STALE_HEARTBEAT alert and a SLOT_ENTRY_TRIGGERED event
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO audit_events (event_type, payload_json, timestamp) VALUES (?, ?, ?)",
        ("ALERT", json.dumps({"category": "STALE_HEARTBEAT", "severity": "HIGH", "message": "Components stopped"}), 1000)
    )
    conn.execute(
        "INSERT INTO audit_events (event_type, payload_json, timestamp) VALUES (?, ?, ?)",
        ("ALERT", json.dumps({"category": "PRIVATE_STREAM_EXPIRED", "severity": "CRITICAL", "message": "Stream expired"}), 2000)
    )
    conn.execute(
        "INSERT INTO audit_events (event_type, payload_json, timestamp) VALUES (?, ?, ?)",
        ("SLOT_ENTRY_TRIGGERED", json.dumps({"signal": {"symbol": "HYPEUSDT", "reference_price": 78.5}, "slot": {"slot_id": 1}}), 3000)
    )
    conn.commit()
    conn.close()

    watcher.poll_once()

    # Verify only the SLOT_ENTRY_TRIGGERED event was sent, and the STALE_HEARTBEAT was suppressed!
    assert notifier.send_message.call_count == 1
    sent_text = notifier.send_message.call_args[0][0]
    assert "ESCANOR STRATEGY SIGNAL - LONG ENTRY" in sent_text
    assert "STALE_HEARTBEAT" not in sent_text
