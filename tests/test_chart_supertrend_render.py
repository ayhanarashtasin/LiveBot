"""Guards the chart-side Supertrend rendering: regime breaks, and direction vs price."""
import json
import re
import shutil
import subprocess
from pathlib import Path

import pytest

from live_engine.dashboards.base import (
    DASHBOARD_HTML,
    _trade_markers,
    load_chart_candles_and_supertrend,
)

REPO_ROOT = Path(__file__).resolve().parents[1]


def _run_render(points, slow_points=()):
    node = shutil.which("node")
    if not node:
        pytest.skip("node not installed")
    fn = re.search(r"^    function renderIndicator\(data\) \{.*?^    \}$",
                   DASHBOARD_HTML, re.S | re.M).group(0)
    script = f"""
      const ST_COLORS = {{ bullish: '#10B981', bearish: '#EF4444', neutral: '#94A3B8' }};
      const ST_BREAK = 'rgba(0,0,0,0)';
      let out = null, opts = null, slow = null;
      const indicatorSeries = {{ setData: d => out = d, applyOptions: o => opts = o }};
      const slowSeries = {{ setData: d => slow = d }};
      {fn}
      renderIndicator({{ supertrend_line: {json.dumps(points)}, indicator_slow_line: {json.dumps(slow_points)} }});
      console.log(JSON.stringify({{ out, opts, slow }}));
    """
    out = subprocess.run([node, "-e", script], capture_output=True, check=True).stdout
    return json.loads(out.decode("utf-8"))


def test_regime_flip_and_gap_break_the_line():
    # 60s candles: flip at index 2, and a missing candle between index 4 and 5.
    pts = [{"time": 0, "value": 9.0, "state": "bullish"},
           {"time": 60, "value": 9.1, "state": "bullish"},
           {"time": 120, "value": 11.0, "state": "bearish"},
           {"time": 180, "value": 10.9, "state": "bearish"},
           {"time": 240, "value": 10.8, "state": "bearish"},
           {"time": 420, "value": 10.5, "state": "bearish"}]
    res = _run_render(pts)
    colors = [p["color"] for p in res["out"]]
    assert colors == ["#10B981", "rgba(0,0,0,0)", "#EF4444", "#EF4444",
                      "rgba(0,0,0,0)", "#EF4444"]
    # Label follows the last plotted point, not a separate band.
    assert res["opts"]["color"] == "#EF4444" and res["opts"]["title"].endswith("\u2193")
    assert [p["value"] for p in res["out"]] == [p["value"] for p in pts]


def test_slow_leg_of_a_cross_indicator_is_plotted():
    """ZEC's EMA(10)/EMA(100) colours by the cross, so both legs have to be on the chart."""
    fast = [{"time": 0, "value": 9.0, "state": "bullish"}, {"time": 60, "value": 9.1, "state": "bullish"}]
    slow = [{"time": 0, "value": 8.0, "state": "neutral"}, {"time": 60, "value": 8.1, "state": "neutral"}]
    res = _run_render(fast, slow)
    assert res["slow"] == [{"time": 0, "value": 8.0}, {"time": 60, "value": 8.1}]
    # A single-line Supertrend payload carries no slow leg and must clear the series.
    assert _run_render(fast)["slow"] == []


@pytest.mark.parametrize("symbol", ["BTCUSDT", "LITUSDT", "ZECUSDT"])
@pytest.mark.parametrize("timeframe", ["1m", "3m", "5m", "15m", "30m", "1h", "4h"])
def test_active_band_matches_direction_and_label(symbol, timeframe):
    r = load_chart_candles_and_supertrend(symbol, REPO_ROOT, limit=400, timeframe=timeframe)
    points = r["supertrend_line"]
    if not points:  # timeframe has no history on disk for this symbol
        assert r["supertrend_direction"] == "NEUTRAL" and r["supertrend_val"] is None
        return
    if r["indicator_name"] == "Supertrend":  # an EMA overlay has no band-vs-price rule
        closes = {c["time"]: c["close"] for c in r["chart_candles"]}
        for p in points:  # bullish band sits under price, bearish over it
            assert (p["value"] < closes[p["time"]]) == (p["state"] == "bullish")
    assert r["supertrend_direction"] == points[-1]["state"].upper()
    assert r["supertrend_val"] == str(points[-1]["value"])


def test_btc_5m_history_contains_regime_flips():
    points = load_chart_candles_and_supertrend("BTCUSDT", REPO_ROOT, limit=400,
                                               timeframe="5m")["supertrend_line"]
    assert sum(a["state"] != b["state"] for a, b in zip(points, points[1:])) >= 3


def test_markers_sort_ascending_and_snap_to_their_bar():
    """SQL hands orders back newest-first; lightweight-charts binary-searches the marker list."""
    orders = [  # 5m bars: 900 and 600
        {"status": "FILLED", "side": "SELL", "avg_fill_price": "11", "created_at_ms": 1_000_000 + 901_234},
        {"status": "FILLED", "side": "BUY", "avg_fill_price": "10", "created_at_ms": 1_000_000 + 601_000},
        {"status": "REJECTED", "side": "BUY", "avg_fill_price": "9", "created_at_ms": 1_000_000},
        {"status": "FILLED", "side": "BUY", "avg_fill_price": "--", "created_at_ms": 1_000_000},
    ]
    markers = _trade_markers(orders, 300_000)
    assert [m["time"] for m in markers] == [1_500_000 // 1000, 1_800_000 // 1000]
    assert [m["side"] if "side" in m else m["text"] for m in markers] == ["BUY", "SELL"]
    assert [m["position"] for m in markers] == ["belowBar", "aboveBar"]
