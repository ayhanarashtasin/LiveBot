"""Backward-compatible entry points; implementation lives in dashboards/."""
from live_engine.config import load_config
from .dashboards.base import (
    DASHBOARD_HTML, DashboardDataAggregator, BaseHTTPHandler as DashboardHTTPHandler,
    dashboard_class, load_chart_candles_and_supertrend, read_single_account_snapshot,
    render_symbol_dashboard, SYMBOL_NAVIGATION_HTML,
)
from .dashboards.overview import (
    MULTI_SYMBOL_DASHBOARD_HTML, MultiSymbolDataAggregator, MultiSymbolHTTPHandler,
    start_multi_symbol_dashboard_server, validate_multi_symbol_dashboard_configs,
    run_multi_symbol_dashboard,
)


def start_dashboard_server(config, host="127.0.0.1", port=8080):
    return dashboard_class(config.symbol.upper())(config).start_server(host, port)


def run_standalone_dashboard(config_path=None, port=8080, host="127.0.0.1"):
    import time
    server = start_dashboard_server(load_config(config_path), host, port)
    print(f"Escanor dashboard: http://{host}:{server.server_port}", flush=True)
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        server.shutdown()
        server.server_close()
