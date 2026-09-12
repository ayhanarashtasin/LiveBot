"""Section 14 regressions: reproducible install and strict configuration."""
import json
import re
from pathlib import Path

import pytest

from live_engine.config import (
    FORBIDDEN_CREDENTIAL_FIELDS,
    VALID_MODES,
    load_config,
    parse_bool,
    parse_mode,
    reject_config_credentials,
    validate_database_path,
)
from live_engine.persistence.event_store import SCHEMA_VERSION, EventStore

BASE = Path.cwd()


# --- dependency manifest -----------------------------------------------------

def test_requirements_manifest_exists_and_covers_every_third_party_import():
    req = (BASE / "requirements.txt").read_text(encoding="utf-8").lower()
    for package in ("numpy", "pandas", "pyarrow", "websockets", "requests", "pyyaml",
                    "freqtrade", "ta-lib"):
        assert package in req, package
    assert (BASE / "requirements-dev.txt").exists()


def test_requirements_are_constrained_not_open_ended():
    lines = [l.strip() for l in (BASE / "requirements.txt").read_text(encoding="utf-8").splitlines()
             if l.strip() and not l.startswith("#")]
    assert lines, "requirements.txt has no requirements"
    for line in lines:
        assert re.search(r"[><=~]=", line), f"{line} is unpinned"


def test_supported_python_version_is_documented():
    req = (BASE / "requirements.txt").read_text(encoding="utf-8")
    assert "Python" in req and "3.1" in req


# --- strict booleans ---------------------------------------------------------

@pytest.mark.parametrize("value", ["false", "False", "FALSE", "0", "no", "off", "", False, 0, None])
def test_falsy_values_never_become_true(value):
    assert parse_bool(value, "canary_mode") is False


@pytest.mark.parametrize("value", ["true", "True", "1", "yes", "on", True, 1])
def test_truthy_values_parse_as_true(value):
    assert parse_bool(value, "canary_mode") is True


@pytest.mark.parametrize("value", ["maybe", "2", "y e s", "null"])
def test_ambiguous_booleans_are_a_configuration_error(value):
    with pytest.raises(ValueError, match="not a valid boolean"):
        parse_bool(value, "canary_mode")


def test_canary_mode_false_string_does_not_enable_canary(tmp_path):
    cfg_path = tmp_path / "c.json"
    cfg_path.write_text(json.dumps({
        "mode": "SHADOW", "symbol": "BTCUSDT", "timeframe": "5m",
        "event_store_path": "data/btc_shadow.db",
        "execution": {"canary_mode": "false"},
    }), encoding="utf-8")

    assert load_config(str(cfg_path)).canary_mode is False


# --- strict mode enum --------------------------------------------------------

@pytest.mark.parametrize("mode", VALID_MODES)
def test_every_valid_mode_is_accepted(mode):
    assert parse_mode(mode.lower()) == mode


@pytest.mark.parametrize("mode", ["SHADW", "live-ish", "", None, "production"])
def test_unknown_mode_is_refused_not_mapped_to_shadow(mode):
    with pytest.raises(ValueError, match="not a valid execution mode"):
        parse_mode(mode)


def test_unknown_mode_in_a_config_file_fails_startup(tmp_path):
    cfg_path = tmp_path / "c.json"
    cfg_path.write_text(json.dumps({
        "mode": "SHADW", "symbol": "BTCUSDT", "event_store_path": "data/btc_shadow.db",
    }), encoding="utf-8")

    with pytest.raises(ValueError, match="not a valid execution mode"):
        load_config(str(cfg_path))


# --- credentials -------------------------------------------------------------

@pytest.mark.parametrize("field", FORBIDDEN_CREDENTIAL_FIELDS)
def test_every_credential_field_is_refused_in_a_config_file(field):
    with pytest.raises(ValueError, match="CREDENTIALS IN CONFIG"):
        reject_config_credentials({field: "secret-value"}, "test.json")


def test_credentials_in_a_config_file_fail_the_loader(tmp_path):
    cfg_path = tmp_path / "c.json"
    cfg_path.write_text(json.dumps({
        "mode": "SHADOW", "symbol": "BTCUSDT", "event_store_path": "data/btc_shadow.db",
        "binance_api_secret": "not-a-real-secret",
    }), encoding="utf-8")

    with pytest.raises(ValueError, match="CREDENTIALS IN CONFIG"):
        load_config(str(cfg_path))


def test_credentials_are_read_only_from_the_environment(tmp_path, monkeypatch):
    cfg_path = tmp_path / "c.json"
    cfg_path.write_text(json.dumps({
        "mode": "SHADOW", "symbol": "BTCUSDT", "event_store_path": "data/btc_shadow.db",
    }), encoding="utf-8")

    monkeypatch.delenv("BINANCE_API_KEY", raising=False)
    monkeypatch.delenv("BINANCE_API_SECRET", raising=False)
    monkeypatch.delenv("BINANCE_SECRET_KEY", raising=False)
    assert load_config(str(cfg_path)).binance_api_key is None

    monkeypatch.setenv("BINANCE_API_KEY", "from-env")
    assert load_config(str(cfg_path)).binance_api_key == "from-env"


def test_shipped_configs_carry_no_credentials():
    for cfg in sorted((BASE / "config").glob("*.json")):
        data = json.loads(cfg.read_text(encoding="utf-8"))
        reject_config_credentials(data, str(cfg))  # must not raise


# --- database containment and schema ----------------------------------------

def test_database_containment_is_preserved():
    validate_database_path("data/btc_paper.db", base_dir=BASE)
    for bad in ("data/../outside.db", "database/shared.db", "data"):
        with pytest.raises(ValueError, match="DATABASE ISOLATION VIOLATION"):
            validate_database_path(bad, base_dir=BASE)


def test_event_store_enables_the_integrity_settings(tmp_path):
    store = EventStore(tmp_path / "pragma.db")
    with store._get_connection() as conn:
        assert conn.execute("PRAGMA journal_mode;").fetchone()[0].lower() == "wal"
        assert conn.execute("PRAGMA foreign_keys;").fetchone()[0] == 1
        assert conn.execute("PRAGMA busy_timeout;").fetchone()[0] >= 1000


def test_schema_version_is_recorded_and_migration_is_forward_only(tmp_path):
    db = tmp_path / "schema.db"
    store = EventStore(db)
    with store._get_connection() as conn:
        assert conn.execute("SELECT version FROM schema_version;").fetchone()[0] == SCHEMA_VERSION

    store.log_event("PROBE", {"kept": True})
    reopened = EventStore(db)  # re-running migrations must preserve existing rows
    assert reopened.get_events_by_type("PROBE")[0]["payload"]["kept"] is True


def test_migration_adds_new_columns_to_a_legacy_orders_table(tmp_path):
    import sqlite3

    db = tmp_path / "legacy.db"
    conn = sqlite3.connect(db)
    conn.execute("""
        CREATE TABLE orders (
            client_order_id TEXT PRIMARY KEY, exchange_order_id TEXT, signal_id TEXT,
            symbol TEXT NOT NULL, side TEXT NOT NULL, order_type TEXT NOT NULL,
            quantity TEXT NOT NULL, price TEXT, stop_price TEXT, status TEXT NOT NULL,
            filled_quantity TEXT NOT NULL, accumulated_fees TEXT NOT NULL,
            avg_fill_price TEXT, rejection_reason TEXT, created_at INTEGER NOT NULL,
            submitted_at INTEGER, acknowledged_at INTEGER, filled_at INTEGER
        );
    """)
    conn.execute(
        "INSERT INTO orders (client_order_id, symbol, side, order_type, quantity, status, "
        "filled_quantity, accumulated_fees, created_at) VALUES "
        "('ESC-LEGACY','BTCUSDT','BUY','MARKET','1','FILLED','1','0',1);"
    )
    conn.commit()
    conn.close()

    store = EventStore(db)
    restored = store.get_order_by_client_id("ESC-LEGACY")
    assert restored is not None
    assert restored.applied_cumulative_qty == 0
    assert restored.reduce_only is False
