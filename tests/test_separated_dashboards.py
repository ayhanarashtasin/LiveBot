"""Coin/mode isolation, read-only HTTP, and approved 5m indicator parity."""
import json
import re
import sqlite3
import subprocess
import sys
from dataclasses import replace
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import numpy as np
import pandas as pd
import pytest

from live_engine.dashboards import BTCDashboard, LITDashboard, ZECDashboard
from live_engine.dashboards.base import read_connection, read_single_account_snapshot
from live_engine.persistence.event_store import EventStore


CLASSES = (BTCDashboard, LITDashboard, ZECDashboard)
ROOT = Path(__file__).resolve().parents[1]


def seed(dashboard, price):
    EventStore(dashboard.db_path)
    with sqlite3.connect(dashboard.db_path) as conn:
        conn.execute(
            "INSERT INTO raw_aggtrades VALUES (?, 1, 1700000000000, 1700000000000, 1700000000010, ?, '1', 1, 1, 0)",
            (dashboard.symbol, str(price)),
        )


@pytest.mark.parametrize("cls", CLASSES)
@pytest.mark.parametrize("mode", ("PAPER", "SHADOW"))
def test_coin_modes_render_and_read_only_http(cls, mode, tmp_path, monkeypatch):
    dashboard = cls(mode=mode, base_dir=tmp_path)
    assert dashboard.db_path == tmp_path / f"data/{cls.symbol[:3].lower()}_{mode.lower()}.db"
    seed(dashboard, 123)
    html = dashboard.render()
    assert cls.symbol in html and cls.strategy_class in html
    if cls is ZECDashboard:
        assert "EMA Cross (10, 100)" in html
    else:
        assert f"Supertrend ({cls.supertrend_defaults[0]}, {cls.supertrend_defaults[1]:.1f})" in html
    # The eye toggle sits inside the coin-tab row and stays collapsed until clicked.
    row = re.search(r'<div class="symbol-nav-row">(.*?)</details></div>', html, re.S).group(1)
    assert row.index("/ZECUSDT?") < row.index('<details class="strategy-eye">')
    eye = row[row.index('<details class="strategy-eye">'):]
    assert "&#128065;" in eye and " open>" not in eye
    assert 'id="configured-strategy"' in eye and cls.strategy_class in eye
    assert 'id="btn-kill" disabled' in html and "HALT: CLI ONLY" in html
    assert "toggleKillSwitch" not in html
    for coin in CLASSES:
        assert f'href="/{coin.symbol}?mode={mode.lower()}"' in html
    assert f'href="/{cls.symbol}?mode={mode.lower()}" aria-current="page"' in html
    if mode == "SHADOW":
        assert "SHADOW MODE · ZERO RISK" in html
        assert "SHADOW REPLAY / AUDIT OBSERVABILITY — ZERO REAL ORDERS" in html
    else:
        assert "PAPER MODE" in html and "SIMULATED PAPER ACCOUNTS — NO REAL ORDERS" in html

    real_connect = sqlite3.connect
    connections = []

    def connect(database, *args, **kwargs):
        assert database == dashboard.db_path.as_uri() + "?mode=ro"
        assert kwargs["uri"] is True
        connections.append(database)
        return real_connect(database, *args, **kwargs)

    monkeypatch.setattr(sqlite3, "connect", connect)
    server = dashboard.start_server(port=0)
    url = f"http://127.0.0.1:{server.server_port}"
    try:
        for path in ("/", f"/{cls.symbol}", f"/{cls.symbol[:3].lower()}"):
            with urlopen(url + path) as response:
                assert response.status == 200
                assert response.headers["Cache-Control"] == "no-store"
                assert response.headers["Access-Control-Allow-Origin"] == "*"
        with urlopen(url + "/api/data?symbol=ETHUSDT") as response:
            payload = json.load(response)
        assert payload["symbol"] == cls.symbol
        assert payload["timeframe"] == cls.timeframe
        assert payload["strategy_name"] == cls.strategy_class
        assert payload["benchmark_id"] == cls.benchmark_id
        if cls is ZECDashboard:
            assert payload["supertrend_params"] == "(10, 100)"
        else:
            assert payload["supertrend_params"] == f"({cls.supertrend_defaults[0]}, {cls.supertrend_defaults[1]:.1f})"
        if cls is LITDashboard:
            assert dashboard.atr_period == 7
        assert payload["mode"] == mode and payload["read_only"]
        assert payload["latest_price"] == "123"
        for method in ("POST", "PUT", "PATCH", "DELETE"):
            with pytest.raises(HTTPError) as error:
                urlopen(Request(url + "/api/kill", method=method))
            assert error.value.code == 405
        assert connections
        assert not dashboard._live_stream_started
        assert not (tmp_path / ".kill_switch").exists()
    finally:
        server.shutdown()
        server.server_close()


@pytest.mark.parametrize("cls", CLASSES)
@pytest.mark.parametrize("mode", ("PAPER", "SHADOW"))
def test_config_loading(cls, mode, tmp_path):
    cfg = dict(symbol=cls.symbol, mode=mode, timeframe=cls.timeframe,
               manifest_path=f"benchmarks/manifests/{cls.benchmark_id}.yaml",
               event_store_path=f"data/{cls.symbol[:3].lower()}_{mode.lower()}_custom.db")
    path = tmp_path / "engine.json"
    path.write_text(json.dumps(cfg))
    dashboard = cls(path, base_dir=tmp_path)
    assert dashboard.config.mode == mode
    assert dashboard.db_path == tmp_path / cfg["event_store_path"]


def test_navigation_switches_coin_and_mode_without_crosstalk(tmp_path):
    dashboards = [cls(mode=mode, base_dir=tmp_path) for cls in CLASSES for mode in ("PAPER", "SHADOW")]
    for price, dashboard in enumerate(dashboards, 101):
        seed(dashboard, price)
    server = dashboards[0].start_server(port=0)
    try:
        for price, dashboard in enumerate(dashboards, 101):
            url = f"http://127.0.0.1:{server.server_port}/{dashboard.symbol}/api/data?mode={dashboard.config.mode.lower()}"
            with urlopen(url) as response:
                payload = json.load(response)
            assert payload["symbol"] == dashboard.symbol
            assert payload["mode"] == dashboard.config.mode
            assert payload["latest_price"] == str(price)
    finally:
        server.shutdown()
        server.server_close()


def test_missing_empty_corrupt_and_wal_databases(tmp_path):
    dashboard = ZECDashboard(base_dir=tmp_path)
    assert dashboard.get_dashboard_payload()["feed_status"] == "DATABASE UNAVAILABLE"
    assert not dashboard.db_path.exists()
    dashboard.db_path.parent.mkdir()
    dashboard.db_path.touch()
    assert dashboard.get_dashboard_payload()["candles"] == []
    assert read_single_account_snapshot(dashboard.config, tmp_path)["mode"] == "SHADOW"
    dashboard.db_path.write_bytes(b"not sqlite")
    assert dashboard.get_dashboard_payload()["candles"] == []
    # A separate WAL writer stays open while the dashboard reads committed data.
    dashboard = ZECDashboard(mode="PAPER", base_dir=tmp_path)
    seed(dashboard, 456)
    with sqlite3.connect(dashboard.db_path) as writer:
        writer.execute("PRAGMA journal_mode=WAL")
        writer.execute("UPDATE raw_aggtrades SET price='789'")
        writer.commit()
        assert dashboard.get_dashboard_payload()["latest_price"] == "789"
        with read_connection(dashboard.db_path) as reader:
            with pytest.raises(sqlite3.OperationalError, match="readonly"):
                reader.execute("DELETE FROM raw_aggtrades")


@pytest.mark.parametrize("path", ("data/../outside.db", "data", "../outside.db"))
def test_rejects_database_escape(path, tmp_path):
    dashboard = BTCDashboard(base_dir=tmp_path)
    with pytest.raises(ValueError, match="DATABASE ISOLATION"):
        BTCDashboard(replace(dashboard.config, event_store_path=path), base_dir=tmp_path)


def test_rejects_candidate_live_wrong_strategy_and_environment(tmp_path, monkeypatch):
    for cls in (LITDashboard, ZECDashboard):
        with pytest.raises(ValueError, match="MODE NOT ALLOWED"):
            cls(mode="LIVE", base_dir=tmp_path)
    zec = ZECDashboard(base_dir=tmp_path)
    with pytest.raises(ValueError, match="requires 15m"):
        ZECDashboard(replace(zec.config, timeframe="5m"), base_dir=tmp_path)
    with pytest.raises(ValueError):
        ZECDashboard(replace(zec.config, manifest_path="benchmarks/manifests/BTC_ST_09_5M.yaml"), base_dir=tmp_path)
    monkeypatch.setenv("ESCANOR_EVENT_STORE", "data/shared.db")
    with pytest.raises(ValueError, match="DATABASE ISOLATION"):
        ZECDashboard(base_dir=tmp_path)


def test_zec_15m_resampling_and_approved_indicator_parity(tmp_path):
    dashboard = ZECDashboard(base_dir=tmp_path)
    EventStore(dashboard.db_path)
    rng = np.random.default_rng(42)
    closes = 100 + np.cumsum(rng.normal(size=300))
    start = 1_700_000_100_000 - (1_700_000_100_000 % 900_000)  # Exact UTC 15m boundary.
    assert start % 900_000 == 0
    with sqlite3.connect(dashboard.db_path) as conn:
        for i, close in enumerate(closes):
            ot = start + i * 60_000
            conn.execute("INSERT INTO candles VALUES (?, '1m', ?, ?, ?, ?, ?, ?, '10', '0', 1, 1)",
                         (dashboard.symbol, ot, ot + 59_999, close - .5, close + 1, close - 1, close))
        # Incomplete final bucket and another symbol must not affect the result.
        conn.execute("INSERT INTO candles VALUES (?, '1m', ?, ?, '999', '999', '999', '999', '10', '0', 1, 1)",
                     (dashboard.symbol, start + 300 * 60_000, start + 301 * 60_000 - 1))
        conn.execute("INSERT INTO candles VALUES ('BTCUSDT', '15m', ?, ?, '9999', '9999', '9999', '9999', '10', '0', 1, 1)",
                     (start, start + 899_999))
    payload = dashboard.get_dashboard_payload("15m")
    bars = payload["candles"]
    assert payload["timeframe"] == "15m" and len(bars) == 20
    assert all(c["open_time"] % 900_000 == 0 and c["volume"] == 150 for c in bars)
    assert bars[0]["open"] == pytest.approx(closes[0] - .5)
    assert bars[0]["close"] == pytest.approx(closes[14])
    expected = dashboard.strategy.populate_indicators(pd.DataFrame(bars), {"pair": "ZECUSDT"})
    assert payload["supertrend_val"] == str(float(expected.ema_fast.iloc[-1]))
    for point in payload["supertrend_line"]:
        index = (point["time"] - start) // 900_000
        assert point["value"] == pytest.approx(expected.ema_fast.iloc[index])
    metrics = payload["strategy_metrics"]
    assert metrics["ema_fast"] == round(float(expected.ema_fast.iloc[-1]), 4)
    assert metrics["ema_slow"] == round(float(expected.ema_slow.iloc[-1]), 4)
    assert "EMA Crossover Status" in dashboard.render()


def test_btc_live_observes_all_safety_gates(tmp_path):
    dashboard = BTCDashboard(mode="LIVE", base_dir=tmp_path)
    payload = dashboard.get_dashboard_payload()
    assert payload["mode_badge"] == "LIVE PRODUCTION"
    assert len(payload["safety_gates"]) >= 24
    assert payload["safety_gates_passed"] is False
    html = dashboard.render()
    # Canary state and gate evidence stay on screen instead of hiding behind the eye toggle.
    assert "Canary:" in html and 'id="safety-gates"' in html
    assert "Canary:" not in re.search(r'<div class="symbol-nav-row">.*?</details></div>', html, re.S).group(0)
    assert not dashboard.db_path.exists()
    assert payload["paper_balance"] is None


@pytest.mark.parametrize("mode", ("SHADOW", "LIVE"))
def test_non_paper_balances_use_reconciliation_evidence(mode, tmp_path):
    dashboard = BTCDashboard(mode=mode, base_dir=tmp_path)
    seed(dashboard, 100)
    with sqlite3.connect(dashboard.db_path) as conn:
        conn.execute("INSERT INTO audit_events (timestamp, event_type, payload_json) VALUES (1, 'BALANCE_RECONCILIATION', ?)",
                     (json.dumps({"mode": mode, "balances": {"USDT": "250.00"}}),))
    payload = dashboard.get_dashboard_payload()
    assert payload["account_available"] is True
    assert payload["paper_balance"] == "250.00"
    assert payload["total_equity"] == "250.00"


@pytest.mark.parametrize("coin", ("btc", "lit", "zec"))
def test_standalone_cli(coin):
    result = subprocess.run([sys.executable, "-m", f"live_engine.dashboards.{coin}_dashboard", "--help"],
                            cwd=ROOT, capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stderr
    assert "--config" in result.stdout and "--mode" in result.stdout and "--port" in result.stdout
    assert "RuntimeWarning" not in result.stderr
