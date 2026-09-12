"""Comprehensive unit and integration tests for the multi-symbol paper dashboard.

Covers:
- Configuration validation (exactly 3 configs, PAPER mode only, exact symbols, unique DBs, no credentials)
- Database path traversal and containment checks
- Read-only SQLite snapshot reader (no missing file creation, mode=ro enforcement)
- Resilient snapshot querying (missing DB, empty DB, corrupt data handling)
- Data isolation across BTCUSDT, LITUSDT, and ZECUSDT
- HTTP server endpoints (/index.html, /api/accounts, /api/data, /api/ping)
- Read-only enforcement (POST requests rejected with 405)
- Prominent simulation notice and XSS-safe rendering
"""
import json
import sqlite3
import tempfile
import urllib.request
import urllib.error
from pathlib import Path
import pytest

from live_engine.config import LiveEngineConfig, validate_database_path, validate_unique_databases
from live_engine.dashboard import (
    DASHBOARD_HTML,
    read_single_account_snapshot,
    load_chart_candles_and_supertrend,
    MultiSymbolDataAggregator,
    start_multi_symbol_dashboard_server,
    validate_multi_symbol_dashboard_configs,
    MULTI_SYMBOL_DASHBOARD_HTML,
)
from live_engine.persistence.event_store import EventStore


@pytest.fixture
def repo_root():
    return Path(__file__).parent.parent.resolve()


def test_validate_multi_symbol_configs_valid(repo_root):
    configs = validate_multi_symbol_dashboard_configs(
        ["config/btc-paper.json", "config/lit-paper.json", "config/zec-paper.json"],
        base_dir=repo_root,
    )
    assert len(configs) == 3
    symbols = {c.symbol for c in configs}
    assert symbols == {"BTCUSDT", "LITUSDT", "ZECUSDT"}
    for c in configs:
        assert c.mode.upper() == "PAPER"
        assert not c.binance_api_key
        assert not c.binance_api_secret


def test_validate_multi_symbol_configs_rejects_invalid_count(repo_root):
    with pytest.raises(ValueError, match="Exactly 3 configurations required"):
        validate_multi_symbol_dashboard_configs(
            ["config/btc-paper.json", "config/lit-paper.json"],
            base_dir=repo_root,
        )


def test_validate_multi_symbol_configs_rejects_non_paper_mode(repo_root):
    # Use lit-shadow instead of lit-paper
    with pytest.raises(ValueError, match="Every mode must be PAPER"):
        validate_multi_symbol_dashboard_configs(
            ["config/btc-paper.json", "config/lit-shadow.json", "config/zec-paper.json"],
            base_dir=repo_root,
        )


def test_validate_multi_symbol_configs_rejects_duplicate_or_missing_symbols(repo_root, tmp_path):
    dup_config = tmp_path / "btc-paper-dup.json"
    dup_config.write_text(json.dumps({
        "symbol": "BTCUSDT",
        "mode": "PAPER",
        "timeframe": "5m",
        "manifest_path": "benchmarks/manifests/BTC_ST_09_5M.yaml",
        "event_store_path": "data/btc_dup.db",
        "warmup_candles": 250,
        "max_open_trades": 1,
    }))

    with pytest.raises(ValueError, match="Symbols must be exactly"):
        validate_multi_symbol_dashboard_configs(
            ["config/btc-paper.json", str(dup_config), "config/zec-paper.json"],
            base_dir=repo_root,
        )


def test_validate_multi_symbol_configs_rejects_duplicate_databases(repo_root, tmp_path):
    dup_db_config = tmp_path / "lit-dup-db.json"
    dup_db_config.write_text(json.dumps({
        "symbol": "LITUSDT",
        "mode": "PAPER",
        "timeframe": "15m",
        "manifest_path": "benchmarks/manifests/LIT_SUPERTREND_15M.yaml",
        "event_store_path": "data/btc_paper.db",  # Same as BTC!
        "warmup_candles": 250,
        "max_open_trades": 1,
    }))

    with pytest.raises(ValueError, match="Duplicate database path"):
        validate_multi_symbol_dashboard_configs(
            ["config/btc-paper.json", str(dup_db_config), "config/zec-paper.json"],
            base_dir=repo_root,
        )


def test_validate_multi_symbol_configs_rejects_path_traversal(repo_root, tmp_path):
    escape_config = tmp_path / "escape.json"
    escape_config.write_text(json.dumps({
        "symbol": "LITUSDT",
        "mode": "PAPER",
        "timeframe": "15m",
        "manifest_path": "benchmarks/manifests/LIT_SUPERTREND_15M.yaml",
        "event_store_path": "data/../outside.db",
        "warmup_candles": 250,
        "max_open_trades": 1,
    }))

    with pytest.raises(ValueError, match="DATABASE ISOLATION VIOLATION"):
        validate_multi_symbol_dashboard_configs(
            ["config/btc-paper.json", str(escape_config), "config/zec-paper.json"],
            base_dir=repo_root,
        )


def test_validate_multi_symbol_configs_rejects_data_dir_itself(repo_root, tmp_path):
    data_dir_cfg = tmp_path / "data_dir.json"
    data_dir_cfg.write_text(json.dumps({
        "symbol": "LITUSDT",
        "mode": "PAPER",
        "timeframe": "15m",
        "manifest_path": "benchmarks/manifests/LIT_SUPERTREND_15M.yaml",
        "event_store_path": "data",
        "warmup_candles": 250,
        "max_open_trades": 1,
    }))

    with pytest.raises(ValueError, match="cannot be the data directory itself"):
        validate_multi_symbol_dashboard_configs(
            ["config/btc-paper.json", str(data_dir_cfg), "config/zec-paper.json"],
            base_dir=repo_root,
        )


def test_validate_multi_symbol_configs_rejects_credentials(repo_root, tmp_path):
    cred_cfg = tmp_path / "cred.json"
    cred_cfg.write_text(json.dumps({
        "symbol": "LITUSDT",
        "mode": "PAPER",
        "timeframe": "15m",
        "manifest_path": "benchmarks/manifests/LIT_SUPERTREND_15M.yaml",
        "event_store_path": "data/lit_paper.db",
        "warmup_candles": 250,
        "max_open_trades": 1,
        "binance_api_key": "sensitive-api-key",
    }))

    # Credentials are now refused by the config loader itself, before any aggregator
    # ever sees the file; the dashboard guard remains as a second line of defence.
    with pytest.raises(ValueError, match="CREDENTIALS IN CONFIG|No API credentials permitted"):
        validate_multi_symbol_dashboard_configs(
            ["config/btc-paper.json", str(cred_cfg), "config/zec-paper.json"],
            base_dir=repo_root,
        )


def test_snapshot_reader_does_not_create_missing_file(tmp_path):
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    missing_db = data_dir / "nonexistent.db"
    assert not missing_db.exists()

    cfg = LiveEngineConfig(
        symbol="BTCUSDT",
        mode="PAPER",
        event_store_path="data/nonexistent.db",
    )
    snap = read_single_account_snapshot(cfg, base_dir=tmp_path)

    # Must report DATABASE UNAVAILABLE and must NOT create the file
    assert snap["status"] == "DATABASE UNAVAILABLE"
    assert not missing_db.exists()


def test_snapshot_reader_handles_empty_db(tmp_path):
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    empty_db = data_dir / "empty.db"
    # Create empty 0-byte file
    empty_db.touch()

    cfg = LiveEngineConfig(
        symbol="LITUSDT",
        mode="PAPER",
        event_store_path="data/empty.db",
    )
    snap = read_single_account_snapshot(cfg, base_dir=tmp_path)

    assert snap["symbol"] == "LITUSDT"
    assert snap["mode"] == "PAPER"
    assert snap["status"] == "WAITING FOR DATA"
    assert snap["trades_count"] == 0
    assert snap["candle_count_15m"] == 0
    assert snap["recent_signals"] == []
    assert snap["recent_orders"] == []
    assert snap["recent_candles"] == []


def test_snapshot_reader_populated_data_and_isolation(tmp_path):
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    btc_db_path = data_dir / "btc.db"
    lit_db_path = data_dir / "lit.db"

    # Schema with symbol column
    schema = """
        CREATE TABLE raw_aggtrades (
            symbol TEXT NOT NULL,
            agg_trade_id INTEGER PRIMARY KEY,
            trade_time INTEGER NOT NULL,
            event_time INTEGER,
            received_at INTEGER,
            price TEXT NOT NULL,
            quantity TEXT NOT NULL,
            first_trade_id INTEGER,
            last_trade_id INTEGER,
            buyer_is_market_maker INTEGER NOT NULL
        );
        CREATE TABLE candles (
            symbol TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            open_time INTEGER NOT NULL,
            close_time INTEGER NOT NULL,
            open TEXT NOT NULL,
            high TEXT NOT NULL,
            low TEXT NOT NULL,
            close TEXT NOT NULL,
            volume TEXT NOT NULL,
            trade_count INTEGER NOT NULL,
            is_closed INTEGER NOT NULL,
            PRIMARY KEY (symbol, timeframe, open_time)
        );
        CREATE TABLE signals (
            signal_id TEXT PRIMARY KEY,
            benchmark_id TEXT,
            strategy_hash TEXT,
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
        CREATE TABLE orders (
            client_order_id TEXT PRIMARY KEY,
            exchange_order_id TEXT,
            signal_id TEXT,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            order_type TEXT NOT NULL,
            quantity TEXT NOT NULL,
            price TEXT,
            status TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            updated_at INTEGER,
            filled_at INTEGER,
            avg_fill_price TEXT,
            accumulated_fees TEXT
        );
        CREATE TABLE audit_events (
            event_id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT,
            payload_json TEXT,
            created_at INTEGER
        );
    """

    # Populate btc.db
    with sqlite3.connect(str(btc_db_path)) as conn:
        conn.executescript(schema)
        conn.execute("INSERT INTO raw_aggtrades VALUES ('BTCUSDT', 101, 1700000000000, 1700000000000, 1700000000010, '60000.5', '0.5', 1, 1, 0);")
        conn.execute("INSERT INTO candles VALUES ('BTCUSDT', '5m', 1700000000000, 1700000300000, '60000.0', '60500.0', '59900.0', '60200.0', '10.5', 100, 1);")
        conn.execute("INSERT INTO signals VALUES ('sig-btc-1', 'BTC_ST_09_5M', 'hash1', 'BTCUSDT', '5m', 1700000000000, 1700000300000, 1700000300000, 'ENTER_LONG', '60200.0', 'Pullback confirmation', NULL);")
        conn.execute("INSERT INTO orders VALUES ('ord-btc-1', 'ex-1', 'sig-btc-1', 'BTCUSDT', 'BUY', 'MARKET', '0.01', '60200.0', 'FILLED', 1700000900500, 1700000901000, 1700000901000, '60201.0', '0.06');")
        conn.execute("INSERT INTO audit_events VALUES (NULL, 'POSITION_UPDATED', '{\"quantity\": \"0.01\", \"entry_price\": \"60201.0\", \"realized_pnl\": \"0.0\", \"symbol\": \"BTCUSDT\"}', 1700000901000);")

    # Populate lit.db
    with sqlite3.connect(str(lit_db_path)) as conn:
        conn.executescript(schema)
        conn.execute("INSERT INTO raw_aggtrades VALUES ('LITUSDT', 202, 1700000000000, 1700000000000, 1700000000015, '0.85', '50.0', 1, 1, 1);")
        conn.execute("INSERT INTO candles VALUES ('LITUSDT', '15m', 1700000000000, 1700000900000, '0.84', '0.86', '0.83', '0.85', '500.0', 20, 1);")
        conn.execute("INSERT INTO signals VALUES ('sig-lit-1', 'LIT_SUPERTREND_15M', 'hash2', 'LITUSDT', '15m', 1700000000000, 1700000900000, 1700000900000, 'EXIT_LONG', '0.85', 'Bearish flip', NULL);")
        conn.execute("INSERT INTO orders VALUES ('ord-lit-1', 'ex-2', 'sig-lit-1', 'LITUSDT', 'SELL', 'MARKET', '50', '0.85', 'FILLED', 1700000900500, 1700000901000, 1700000901000, '0.85', '0.004');")
        conn.execute("INSERT INTO audit_events VALUES (NULL, 'POSITION_UPDATED', '{\"quantity\": \"0\", \"entry_price\": \"0\", \"realized_pnl\": \"1.25\", \"symbol\": \"LITUSDT\"}', 1700000901000);")

    cfg_btc = LiveEngineConfig(symbol="BTCUSDT", mode="PAPER", event_store_path="data/btc.db")
    cfg_lit = LiveEngineConfig(symbol="LITUSDT", mode="PAPER", event_store_path="data/lit.db")

    btc_snap = read_single_account_snapshot(cfg_btc, base_dir=tmp_path)
    lit_snap = read_single_account_snapshot(cfg_lit, base_dir=tmp_path)

    # Verify BTC isolation
    assert btc_snap["symbol"] == "BTCUSDT"
    assert btc_snap["latest_price"] == "60000.5"
    assert btc_snap["position"]["quantity"] == "0.0100"
    assert len(btc_snap["recent_signals"]) == 1
    assert btc_snap["recent_signals"][0]["signal_id"] == "sig-btc-1"
    assert len(btc_snap["recent_orders"]) == 1
    assert btc_snap["recent_orders"][0]["client_order_id"] == "ord-btc-1"
    # Markers anchor to the bar holding the fill (1700000900500 in the 5m bar at 1700000700).
    assert btc_snap["trade_markers"][0]["time"] == 1700000700

    # Verify LIT isolation
    assert lit_snap["symbol"] == "LITUSDT"
    assert lit_snap["latest_price"] == "0.85"
    assert lit_snap["position"]["quantity"] == "0.0000"
    assert lit_snap["position"]["realized_pnl"] == "+1.25"
    assert len(lit_snap["recent_signals"]) == 1
    assert lit_snap["recent_signals"][0]["signal_id"] == "sig-lit-1"
    assert len(lit_snap["recent_orders"]) == 1
    assert lit_snap["recent_orders"][0]["client_order_id"] == "ord-lit-1"
    assert lit_snap["trade_markers"][0]["time"] == 1700000700

    # Verify no crosstalk
    assert "sig-lit-1" not in [s["signal_id"] for s in btc_snap["recent_signals"]]
    assert "sig-btc-1" not in [s["signal_id"] for s in lit_snap["recent_signals"]]


def test_multi_symbol_aggregator_payload(repo_root):
    configs = [
        LiveEngineConfig(symbol="BTCUSDT", mode="PAPER", event_store_path="data/btc_paper.db"),
        LiveEngineConfig(symbol="LITUSDT", mode="PAPER", timeframe="15m", manifest_path="benchmarks/manifests/LIT_SUPERTREND_15M.yaml", event_store_path="data/lit_paper.db"),
        LiveEngineConfig(symbol="ZECUSDT", mode="PAPER", timeframe="15m", manifest_path="benchmarks/manifests/ZEC_MOMENTUM_M03_15M.yaml", event_store_path="data/zec_paper.db"),
    ]
    agg = MultiSymbolDataAggregator(configs, base_dir=repo_root)
    accounts_payload = agg.get_accounts_payload()

    assert "accounts" in accounts_payload
    accounts = accounts_payload["accounts"]
    assert len(accounts) == 3
    symbols = {a["symbol"] for a in accounts}
    assert symbols == {"BTCUSDT", "LITUSDT", "ZECUSDT"}

    for acct in accounts:
        assert acct["mode"] == "PAPER"
        assert acct["status"] in ("HEALTHY", "WAITING FOR DATA", "STALE", "DATABASE UNAVAILABLE")
        assert "balance_usdt" in acct
        assert "position" in acct
        assert "trades_count" in acct


def test_multi_symbol_server_endpoints(repo_root):
    configs = [
        LiveEngineConfig(symbol="BTCUSDT", mode="PAPER", event_store_path="data/btc_paper.db"),
        LiveEngineConfig(symbol="LITUSDT", mode="PAPER", timeframe="15m", manifest_path="benchmarks/manifests/LIT_SUPERTREND_15M.yaml", event_store_path="data/lit_paper.db"),
        LiveEngineConfig(symbol="ZECUSDT", mode="PAPER", timeframe="15m", manifest_path="benchmarks/manifests/ZEC_MOMENTUM_M03_15M.yaml", event_store_path="data/zec_paper.db"),
    ]
    port = 8195
    server = start_multi_symbol_dashboard_server(configs, host="127.0.0.1", port=port, base_dir=repo_root)

    try:
        # 1. GET / (HTML)
        res_html = urllib.request.urlopen(f"http://127.0.0.1:{port}/")
        assert res_html.status == 200
        html_content = res_html.read().decode("utf-8")
        assert "SIMULATED PAPER ACCOUNTS" in html_content
        assert "NO REAL ORDERS" in html_content
        assert "BTCUSDT" in html_content
        assert "LITUSDT" in html_content
        assert "ZECUSDT" in html_content
        assert "kill_switch" in html_content or "Kill Switch" in html_content

        # 2. GET /api/accounts
        res_accts = urllib.request.urlopen(f"http://127.0.0.1:{port}/api/accounts")
        assert res_accts.status == 200
        data_accts = json.loads(res_accts.read().decode("utf-8"))
        assert "accounts" in data_accts
        assert len(data_accts["accounts"]) == 3
        symbols = {a["symbol"] for a in data_accts["accounts"]}
        assert symbols == {"BTCUSDT", "LITUSDT", "ZECUSDT"}

        # 3. GET /api/data?symbol=BTCUSDT
        res_btc = urllib.request.urlopen(f"http://127.0.0.1:{port}/api/data?symbol=BTCUSDT")
        assert res_btc.status == 200
        data_btc = json.loads(res_btc.read().decode("utf-8"))
        assert data_btc["symbol"] == "BTCUSDT"
        assert "recent_signals" in data_btc
        assert "recent_orders" in data_btc
        assert "recent_candles" in data_btc

        # 4. GET /api/data?symbol=LITUSDT
        res_lit = urllib.request.urlopen(f"http://127.0.0.1:{port}/api/data?symbol=LITUSDT")
        assert res_lit.status == 200
        data_lit = json.loads(res_lit.read().decode("utf-8"))
        assert data_lit["symbol"] == "LITUSDT"

        # 5. GET /api/data?symbol=UNKNOWN
        res_unk = urllib.request.urlopen(f"http://127.0.0.1:{port}/api/data?symbol=UNKNOWN")
        assert res_unk.status == 200
        data_unk = json.loads(res_unk.read().decode("utf-8"))
        assert data_unk.get("error") is not None

        # 6. GET /api/ping
        res_ping = urllib.request.urlopen(f"http://127.0.0.1:{port}/api/ping")
        assert res_ping.status == 200
        assert json.loads(res_ping.read().decode("utf-8"))["status"] == "ok"

        # 7. POST requests MUST be rejected with 405 Method Not Allowed (read-only observability)
        req_post = urllib.request.Request(
            f"http://127.0.0.1:{port}/api/order",
            data=b"{}",
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with pytest.raises(urllib.error.HTTPError) as exc_info:
            urllib.request.urlopen(req_post)
        assert exc_info.value.code == 405

    finally:
        server.shutdown()
        server.server_close()


@pytest.fixture
def isolated_dashboard_server(tmp_path, repo_root):
    configs = validate_multi_symbol_dashboard_configs(
        ["config/btc-paper.json", "config/lit-paper.json", "config/zec-paper.json"],
        base_dir=repo_root,
    )
    for index, config in enumerate(configs, start=1):
        assert config.event_store_path == f"data/{config.symbol[:3].lower()}_paper.db"
        db_path = tmp_path / config.event_store_path
        EventStore(db_path)
        with sqlite3.connect(db_path) as conn:
            assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
            conn.execute(
                "INSERT INTO raw_aggtrades VALUES (?, 1, 1700000000000, 1700000000000, 1700000000010, ?, '1', 1, 1, 0)",
                (config.symbol, str(index * 100)),
            )
            for timeframe in ("1m", "5m", "15m"):
                conn.execute(
                    "INSERT INTO candles VALUES (?, ?, 1700000000000, 1700000060000, ?, ?, ?, ?, '1', '0', 1, 1)",
                    (config.symbol, timeframe, *([str(index * 100)] * 4)),
                )
            conn.execute(
                "INSERT INTO signals VALUES (?, 'benchmark', 'hash', ?, '15m', 1700000000000, 1700000900000, 1700000900000, 'ENTER_LONG', ?, 'test', NULL)",
                (f"sig-{config.symbol}", config.symbol, str(index * 100)),
            )
            conn.execute(
                "INSERT INTO orders (client_order_id, symbol, side, order_type, quantity, price, status, filled_quantity, accumulated_fees, created_at) VALUES (?, ?, 'BUY', 'MARKET', '1', ?, 'NEW', '0', '0', 1700000900000)",
                (f"ord-{config.symbol}", config.symbol, str(index * 100)),
            )
            conn.execute(
                "INSERT INTO audit_events (timestamp, event_type, payload_json) VALUES (1700000900000, 'PAPER_ACCOUNT_SNAPSHOT', ?)",
                (json.dumps({"balance": str(index * 1000)}),),
            )
    server = start_multi_symbol_dashboard_server(configs, port=0, base_dir=tmp_path)
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()


@pytest.mark.parametrize("path, active", [
    ("/", ""), ("/index.html", ""),
    ("/BTCUSDT", "BTCUSDT"), ("/LITUSDT", "LITUSDT"), ("/ZECUSDT", "ZECUSDT"),
    ("/btcusdt", "BTCUSDT"), ("/litusdt", "LITUSDT"), ("/zEcUsDt", "ZECUSDT"),
])
def test_dedicated_dashboard_pages(isolated_dashboard_server, path, active):
    port = isolated_dashboard_server.server_port
    with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=5) as response:
        assert response.status == 200
        assert response.headers.get_content_type() == "text/html"
        html = response.read().decode("utf-8")
    assert 'aria-label="Dashboards"' in html
    assert html.count(' aria-current="page"') == 1
    suffix = "?mode=paper" if active else ""
    assert f'href="/{active}{suffix}" aria-current="page"' in html
    for symbol in ("BTCUSDT", "LITUSDT", "ZECUSDT"):
        assert f'href="/{symbol}{suffix}"' in html
    assert "SIMULATED PAPER ACCOUNTS — NO REAL ORDERS · READ-ONLY" in html
    if active:
        context = json.loads(html.split("const dashboardConfig = ", 1)[1].split(";", 1)[0])
        config = isolated_dashboard_server.RequestHandlerClass.aggregator.dashboards[active].config
        assert context == {"symbol": active, "timeframe": config.timeframe, "mode": "PAPER", "readOnly": True}
        assert 'id="btn-kill" disabled' in html
        assert "HALT: CLI ONLY" in html
        assert "LightweightCharts" in html
        assert "const endpoint = dashboardConfig.symbol ? '/' + dashboardConfig.symbol + '/api/data' : '/api/data';" in html
        # Rendering dedicated pages must leave the standalone template untouched.
        assert "const dashboardConfig = {};" in DASHBOARD_HTML
        assert 'id="btn-kill" disabled' not in DASHBOARD_HTML


@pytest.mark.parametrize("symbol, index", [("BTCUSDT", 1), ("LITUSDT", 2), ("ZECUSDT", 3)])
def test_scoped_dashboard_data_is_read_only_and_isolated(isolated_dashboard_server, symbol, index, monkeypatch):
    server = isolated_dashboard_server
    aggregator = server.RequestHandlerClass.aggregator
    real_connect = sqlite3.connect
    connections = []

    def read_only_connect(database, *args, **kwargs):
        assert database == aggregator.dashboards[symbol].db_path.as_uri() + "?mode=ro"
        assert kwargs.get("uri") is True
        connections.append(database)
        return real_connect(database, *args, **kwargs)

    monkeypatch.setattr(sqlite3, "connect", read_only_connect)
    for suffix, timeframe in [("", aggregator.dashboards[symbol].strategy_timeframe), ("?timeframe=1m", "1m"), ("?timeframe=5m&symbol=ETHUSDT", "5m")]:
        url = f"http://127.0.0.1:{server.server_port}/{symbol.lower()}/api/data{suffix}"
        with urllib.request.urlopen(url, timeout=5) as response:
            assert response.status == 200
            assert response.headers.get_content_type() == "application/json"
            payload = json.load(response)
        assert payload["symbol"] == symbol
        assert payload["timeframe"] == timeframe
        assert payload["read_only"] is True
        assert payload["latest_price"] == str(index * 100)
        assert payload["paper_balance"] == str(index * 1000)
        assert [s["signal_id"] for s in payload["signals"]] == [f"sig-{symbol}"]
        assert [o["client_order_id"] for o in payload["orders"]] == [f"ord-{symbol}"]
        assert [c["close"] for c in payload["candles"]] == [index * 100]
    assert connections
    assert all(not d._live_stream_started for d in aggregator.dashboards.values())


@pytest.mark.parametrize("method, path, status", [
    ("POST", f"/{symbol}/api/{endpoint}", 405)
    for symbol in ("BTCUSDT", "litusdt", "ZECUSDT")
    for endpoint in ("data", "kill", "order")
] + [("GET", path, 404) for path in ("/ETHUSDT", "/ethusdt/api/data", "/BTCUSDT/unknown")])
def test_scoped_dashboard_rejects_mutations_and_unknown_routes(isolated_dashboard_server, method, path, status):
    server = isolated_dashboard_server
    kill_path = server.RequestHandlerClass.aggregator.base_dir / ".kill_switch"
    request = urllib.request.Request(f"http://127.0.0.1:{server.server_port}{path}", method=method)
    with pytest.raises(urllib.error.HTTPError) as error:
        urllib.request.urlopen(request, timeout=5)
    assert error.value.code == status
    assert not kill_path.exists()


def test_dashboard_xss_protection():
    # Verify that MULTI_SYMBOL_DASHBOARD_HTML includes escaping helper and textContent
    assert "escapeHtml" in MULTI_SYMBOL_DASHBOARD_HTML
    assert "&lt;" in MULTI_SYMBOL_DASHBOARD_HTML
    assert "&gt;" in MULTI_SYMBOL_DASHBOARD_HTML
    assert "&quot;" in MULTI_SYMBOL_DASHBOARD_HTML


def test_multi_symbol_chart_html_and_script():
    # Verify LightweightCharts library and canvas container in MULTI_SYMBOL_DASHBOARD_HTML
    assert "lightweight-charts" in MULTI_SYMBOL_DASHBOARD_HTML
    assert "multi-chart-container" in MULTI_SYMBOL_DASHBOARD_HTML
    assert "chart-symbol-title" in MULTI_SYMBOL_DASHBOARD_HTML
    assert "candleSeries" in MULTI_SYMBOL_DASHBOARD_HTML
    assert "ST_BREAK" in MULTI_SYMBOL_DASHBOARD_HTML
    assert "indicatorSeries.setData" in MULTI_SYMBOL_DASHBOARD_HTML
    assert "next.state !== p.state" in MULTI_SYMBOL_DASHBOARD_HTML


def test_load_chart_candles_and_supertrend(repo_root):
    # BTCUSDT chart data loading
    btc_chart = load_chart_candles_and_supertrend("BTCUSDT", repo_root, limit=150, timeframe="5m")
    assert len(btc_chart["chart_candles"]) > 0
    assert len(btc_chart["supertrend_line"]) > 0
    assert btc_chart["supertrend_direction"] in ("BULLISH", "BEARISH")
    assert btc_chart["supertrend_params"] == "(10, 3.0)"
    assert {point["state"] for point in btc_chart["supertrend_line"]} == {"bullish", "bearish"}
    assert btc_chart["supertrend_line"][-1]["state"].upper() == btc_chart["supertrend_direction"]
    assert "time" in btc_chart["chart_candles"][0]
    assert "open" in btc_chart["chart_candles"][0]

    # LITUSDT chart data loading
    lit_chart = load_chart_candles_and_supertrend("LITUSDT", repo_root, limit=150)
    assert len(lit_chart["chart_candles"]) > 0
    assert len(lit_chart["supertrend_line"]) > 0
    assert lit_chart["supertrend_direction"] in ("BULLISH", "BEARISH")
    assert lit_chart["supertrend_params"] == "(28, 2.0)"
    assert {point["state"] for point in lit_chart["supertrend_line"]} == {"bullish", "bearish"}
    assert lit_chart["supertrend_line"][-1]["state"].upper() == lit_chart["supertrend_direction"]

    # ZECUSDT chart data loading
    zec_chart = load_chart_candles_and_supertrend("ZECUSDT", repo_root, limit=150)
    assert len(zec_chart["chart_candles"]) > 0
    assert len(zec_chart["supertrend_line"]) > 0
    assert zec_chart["supertrend_direction"] in ("BULLISH", "BEARISH")
    assert {point["state"] for point in zec_chart["supertrend_line"]} == {"bullish", "bearish", "neutral"}
    assert zec_chart["supertrend_line"][-1]["state"].upper() == zec_chart["supertrend_direction"]


def test_snapshot_includes_chart_data(repo_root):
    from live_engine.config import load_config
    cfg = load_config(repo_root / "config" / "btc-paper.json")
    snap = read_single_account_snapshot(cfg, base_dir=repo_root)
    assert "chart_candles" in snap
    assert "supertrend_line" in snap
    assert "supertrend_direction" in snap
    assert "trade_markers" in snap
    assert len(snap["chart_candles"]) > 0
