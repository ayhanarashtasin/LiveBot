"""Shared read-only terminal, SQLite readers, and chart calculations."""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
import time
from contextlib import contextmanager
from dataclasses import replace
from html import escape
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlparse

import pandas as pd

from live_engine.config import (
    LiveEngineConfig,
    load_config,
    validate_database_path,
    validate_unique_databases,
    validate_config_against_manifest,
)
from live_engine.market_data.resampler import TIMEFRAME_MAP_MS
from live_engine.risk.kill_switch import KillSwitch
from strategies.approved.supertrend_helper import supertrend

logger = logging.getLogger("escanor.dashboard")


def _indicator_line(candles, values, directions, time_divisor=1):
    """Chart points carrying the strategy's historical state per candle."""
    points = []
    for candle, value, direction in zip(candles, values, directions):
        if pd.notna(value) and float(value) > 0:
            direction = float(direction)
            points.append({
                "time": candle["open_time"] // time_divisor,
                "value": float(value),
                "state": "bullish" if direction > 0 else ("bearish" if direction < 0 else "neutral"),
            })
    return points


def _apply_indicator_state(target, points):
    """Label state/value come from the exact point plotted last, never a separate band."""
    last = points[-1] if points else None
    target["supertrend_val"] = None if last is None else str(last["value"])
    target["supertrend_direction"] = "NEUTRAL" if last is None else last["state"].upper()


def _trade_markers(orders, tf_ms):
    """Fills as chart markers, anchored to the bar that holds them and sorted ascending.

    Orders arrive newest-first from SQL; lightweight-charts binary-searches the marker
    array for the visible range, so an unsorted list silently drops markers.
    """
    markers = []
    for order in orders:
        if order.get("status") != "FILLED" or not order.get("created_at_ms"):
            continue
        try:
            float(order["avg_fill_price"])
        except (TypeError, ValueError):
            continue
        is_buy = order.get("side") == "BUY"
        markers.append({
            "time": int(order["created_at_ms"]) // tf_ms * tf_ms // 1000,
            "position": "belowBar" if is_buy else "aboveBar",
            "color": "#10B981" if is_buy else "#EF4444",
            "shape": "arrowUp" if is_buy else "arrowDown",
            "text": "BUY" if is_buy else "SELL",
        })
    markers.sort(key=lambda m: m["time"])
    return markers

DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en" class="dark" data-theme="dark">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Escanor Live Trading Terminal</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <script src="https://cdn.jsdelivr.net/npm/lightweight-charts@4.1.1/dist/lightweight-charts.standalone.production.js"></script>
  <script>
    tailwind.config = {
      darkMode: 'class',
      theme: {
        extend: {
          colors: {
            brand: { 500: '#F59E0B', 600: '#D97706' },
            dark: { 900: '#0B0F19', 800: '#111827', 700: '#1F2937', 600: '#374151' }
          }
        }
      }
    }
  </script>
  <style>
    body { background-color: #0B0F19; color: #E2E8F0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }
    .pulse-dot { animation: pulse 2s cubic-bezier(0.4, 0, 0.6, 1) infinite; }
    @keyframes pulse { 0%, 100% { opacity: 1; transform: scale(1); } 50% { opacity: .4; transform: scale(0.9); } }
    ::-webkit-scrollbar { width: 6px; height: 6px; }
    ::-webkit-scrollbar-track { background: #111827; }
    ::-webkit-scrollbar-thumb { background: #374151; border-radius: 3px; }
    /* Light mode (palette: #F8EDE3 bg, #BDD2B6 card, #A2B29F border, #798777 accent) */
    html[data-theme="light"] body { background-color: #F8EDE3; color: #2F3A2E; }
    html[data-theme="light"] .bg-dark-800 { background-color: #FFFCF5 !important; }
    html[data-theme="light"] div[class*="bg-dark-900"] { background-color: #BDD2B644 !important; }
    html[data-theme="light"] .bg-dark-900 { background-color: #F1E7D7 !important; }
    html[data-theme="light"] .border-dark-700 { border-color: #A2B29F !important; }
    html[data-theme="light"] .divide-dark-700\/50 > * { border-color: #A2B29F88 !important; }
    html[data-theme="light"] .text-white { color: #2F3A2E !important; }
    html[data-theme="light"] .text-gray-300 { color: #37453A !important; }
    html[data-theme="light"] .text-gray-400 { color: #46543F !important; }
    html[data-theme="light"] .text-gray-500 { color: #55624E !important; }
    html[data-theme="light"] .text-emerald-400 { color: #046C4E !important; }
    html[data-theme="light"] .text-red-400 { color: #B91C1C !important; }
    html[data-theme="light"] .text-amber-400 { color: #8A5A00 !important; }
    html[data-theme="light"] .bg-amber-400\/10 { background-color: #E8B93C2E !important; }
    html[data-theme="light"] .border-amber-400\/20, html[data-theme="light"] .border-amber-500\/30 { border-color: #B98A1F88 !important; }
    html[data-theme="light"] .bg-dark-700 { background-color: #BDD2B6 !important; color: #2F3A2E !important; }
    html[data-theme="light"] #chart-container { background: #F1E7D7 !important; }
    html[data-theme="light"] ::-webkit-scrollbar-track { background: #F8EDE3; }
    html[data-theme="light"] ::-webkit-scrollbar-thumb { background: #A2B29F; }
    /* Clean, High-Contrast Light Mode Navbar */
    html[data-theme="light"] header[class*="bg-dark-800"] {
      background-color: #FFFFFF !important;
      border-color: #E2E8F0 !important;
      box-shadow: 0 1px 3px 0 rgba(0, 0, 0, 0.05), 0 1px 2px -1px rgba(0, 0, 0, 0.05) !important;
    }
    html[data-theme="light"] header .text-white {
      color: #0F172A !important;
    }
    html[data-theme="light"] header .h-5 {
      background-color: #CBD5E1 !important;
    }
    html[data-theme="light"] #nav-live-badge {
      background-color: #FEF3C7 !important;
      color: #92400E !important;
      border: 1px solid #FCD34D !important;
      font-weight: 700 !important;
    }
    html[data-theme="light"] #nav-symbol {
      background-color: #F1F5F9 !important;
      color: #0F172A !important;
      border: 1px solid #CBD5E1 !important;
      font-weight: 700 !important;
      letter-spacing: 0.05em !important;
    }
    html[data-theme="light"] #nav-tf-badge {
      background-color: #FEF3C7 !important;
      color: #B45309 !important;
      border: 1px solid #FCD34D !important;
      font-weight: 700 !important;
    }
    html[data-theme="light"] #mode-badge {
      background-color: #ECFDF5 !important;
      color: #065F46 !important;
      border: 1px solid #6EE7B7 !important;
      font-weight: 700 !important;
    }
    html[data-theme="light"] #mode-badge span {
      color: #065F46 !important;
    }
    html[data-theme="light"] #mode-badge .pulse-dot {
      background-color: #10B981 !important;
    }
    html[data-theme="light"] #kill-badge {
      background-color: #ECFDF5 !important;
      color: #065F46 !important;
      border: 1px solid #6EE7B7 !important;
      font-weight: 700 !important;
    }
    html[data-theme="light"] #kill-badge span {
      color: #065F46 !important;
    }
    html[data-theme="light"] #kill-badge.bg-red-500\/10 {
      background-color: #FEF2F2 !important;
      color: #991B1B !important;
      border: 1px solid #F87171 !important;
    }
    html[data-theme="light"] #kill-badge.bg-red-500\/10 span {
      color: #991B1B !important;
    }
    html[data-theme="light"] #btn-kill {
      background-color: #FEF2F2 !important;
      color: #DC2626 !important;
      border: 1px solid #FCA5A5 !important;
      font-weight: 700 !important;
    }
    html[data-theme="light"] #btn-kill:hover {
      background-color: #FEE2E2 !important;
      color: #B91C1C !important;
      border-color: #F87171 !important;
    }
    html[data-theme="light"] #btn-kill.bg-emerald-600\/20 {
      background-color: #ECFDF5 !important;
      color: #047857 !important;
      border: 1px solid #6EE7B7 !important;
    }
    html[data-theme="light"] #btn-kill.bg-emerald-600\/20:hover {
      background-color: #D1FAE5 !important;
      color: #065F46 !important;
    }
    html[data-theme="light"] #btn-theme {
      background-color: #F8FAFC !important;
      color: #334155 !important;
      border: 1px solid #CBD5E1 !important;
      font-weight: 600 !important;
    }
    html[data-theme="light"] #btn-theme:hover {
      background-color: #F1F5F9 !important;
      color: #0F172A !important;
      border-color: #94A3B8 !important;
    }
    html[data-theme="light"] #clock-utc {
      color: #64748B !important;
      font-weight: 600 !important;
    }
    /* Timeframe pill -> olive bar, cream labels, amber active kept */
    html[data-theme="light"] #tf-toolbar { background-color: #5F6E5B !important; border-color: #5F6E5B !important; }
    html[data-theme="light"] #tf-toolbar .tf-btn:not(.bg-amber-500) { color: #F8EDE3 !important; }
    html[data-theme="light"] #tf-toolbar .tf-btn:not(.bg-amber-500):hover { color: #FFFFFF !important; }
    /* Orders table header -> olive bar, cream labels (matches navbar/toolbar) */
    html[data-theme="light"] thead[class*="bg-dark-900"] { background-color: #5F6E5B !important; }
    html[data-theme="light"] thead[class*="bg-dark-900"] th { color: #F8EDE3 !important; }
  </style>
</head>
<body class="min-h-screen flex flex-col antialiased selection:bg-amber-500/30">

  <!-- TOP NAVIGATION BAR -->
  <header class="border-b border-dark-700 bg-dark-800/90 backdrop-blur px-6 py-3 sticky top-0 z-50 flex flex-wrap items-center justify-between gap-4">
    <div class="flex items-center space-x-4">
      <div class="flex items-center space-x-2">
        <div class="w-8 h-8 rounded-lg bg-gradient-to-tr from-amber-600 to-amber-400 flex items-center justify-center font-black text-black text-lg shadow-lg shadow-amber-500/20">
          E
        </div>
        <span class="text-xl font-bold tracking-tight text-white">ESCANOR <span id="nav-live-badge" class="text-amber-400 font-normal text-sm ml-1 px-1.5 py-0.5 rounded bg-amber-400/10 border border-amber-400/20">LIVE ENGINE</span></span>
      </div>
      <div class="h-5 w-px bg-dark-700"></div>
      <div class="flex items-center space-x-2 text-xs font-mono">
        <span id="nav-symbol" class="px-2.5 py-1 rounded bg-dark-700 font-semibold text-white tracking-wide">BTCUSDT</span>
        <span id="nav-tf-badge" class="px-2 py-1 rounded bg-amber-500/20 text-amber-400 font-bold border border-amber-500/30">--</span>
      </div>
    </div>

    <div class="flex items-center space-x-3">
      <!-- Mode Badge -->
      <div id="mode-badge" class="flex items-center space-x-1.5 px-3 py-1 rounded-full text-xs font-semibold bg-emerald-500/10 text-emerald-400 border border-emerald-500/20">
        <span class="w-2 h-2 rounded-full bg-emerald-400 pulse-dot"></span>
        <span id="nav-mode">SHADOW MODE</span>
      </div>

      <!-- Kill Switch Status -->
      <div id="kill-badge" class="flex items-center space-x-1.5 px-3 py-1 rounded-full text-xs font-semibold bg-emerald-500/10 text-emerald-400 border border-emerald-500/20">
        <span>SAFETY:</span>
        <span id="kill-status-text">CLEAR</span>
      </div>

      <!-- Emergency Button -->
      <button id="btn-kill" class="px-3 py-1 text-xs font-semibold rounded bg-red-600/20 hover:bg-red-600/40 text-red-400 border border-red-500/30 transition">
        EMERGENCY HALT
      </button>
      <button onclick="toggleTheme()" id="btn-theme" title="Toggle dark / light mode" class="px-3 py-1 text-xs font-semibold rounded bg-dark-700 text-gray-300 border border-dark-700 transition">
        ☀️ LIGHT
      </button>

      <div class="text-xs text-gray-500 font-mono" id="clock-utc">--:--:-- UTC</div>
    </div>
  </header>

  <!-- MAIN BODY -->
  <main class="flex-1 p-6 space-y-6 max-w-[1700px] w-full mx-auto">
    <!-- SYMBOL_NAVIGATION -->
    <div id="dashboard-status" class="text-xs text-gray-400" role="status">Connecting to account data...</div>

    <!-- KPI STATS CARDS -->
    <section class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
      <!-- Card 1: Live BTC Price -->
      <div class="bg-dark-800 border border-dark-700 rounded-xl p-4 shadow-sm relative overflow-hidden">
        <div class="text-xs font-medium text-gray-400 uppercase tracking-wider">Live Binance USD-M Price</div>
        <div class="mt-2 flex items-baseline justify-between">
          <span id="metric-price" class="text-3xl font-extrabold text-white tracking-tight">$0.00</span>
          <span id="metric-latency" class="text-xs font-mono px-2 py-0.5 rounded bg-dark-700 text-gray-300">-- ms</span>
        </div>
        <div class="mt-2 text-xs text-gray-500 flex items-center justify-between">
          <span>Binance Futures aggTrade</span>
          <span id="metric-updated" class="font-mono">Connecting...</span>
        </div>
      </div>

      <!-- Card 2: Strategy Decision / Direction -->
      <div class="bg-dark-800 border border-dark-700 rounded-xl p-4 shadow-sm">
        <div class="text-xs font-medium text-gray-400 uppercase tracking-wider flex items-center justify-between">
          <span id="metric-st-label">Supertrend</span>
          <span id="metric-st-tf" class="text-[10px] text-amber-400 font-mono px-1.5 py-0.5 rounded bg-amber-400/10">--</span>
        </div>
        <div class="mt-2 flex items-center justify-between">
          <span id="metric-st-direction" class="text-base font-bold px-2.5 py-1 rounded bg-dark-700 text-gray-300">NEUTRAL</span>
          <span id="metric-st-val" class="text-xs font-mono text-gray-400">ST: $--</span>
        </div>
        <div class="mt-2 text-xs text-gray-500 flex items-center justify-between">
          <span title="Chart interpretation; actual engine signals appear in Strategy Decisions">Chart context:</span>
          <span id="metric-last-action" class="font-semibold text-gray-300">HOLD</span>
        </div>
      </div>

      <!-- Card 3: Active Position & PnL -->
      <div class="bg-dark-800 border border-dark-700 rounded-xl p-4 shadow-sm">
        <div class="text-xs font-medium text-gray-400 uppercase tracking-wider">Active Position & PnL</div>
        <div class="mt-2 flex items-baseline justify-between">
          <span id="metric-position-qty" class="text-2xl font-bold text-white font-mono">0.0000</span>
          <span id="metric-realized-pnl" class="text-xs font-mono font-semibold px-2 py-0.5 rounded bg-dark-700 text-gray-300">PnL: $0.00</span>
        </div>
        <div class="mt-2 text-xs text-gray-500 flex items-center justify-between">
          <span>Avg Entry: <span id="metric-entry-px" class="text-gray-300 font-mono">$0.00</span></span>
          <span id="metric-open-status" class="text-gray-400">FLAT</span>
        </div>
        <div class="mt-1.5 text-xs flex items-center justify-between">
          <span>Unrealized: <span id="metric-unrealized-pnl" class="font-mono text-gray-300">$0.00</span></span>
          <span>Notional: <span id="metric-notional" class="font-mono text-gray-300">$0.00</span></span>
        </div>
      </div>

      <!-- Card 4: Ingestion Pipeline Status -->
      <div class="bg-dark-800 border border-dark-700 rounded-xl p-4 shadow-sm">
        <div class="text-xs font-medium text-gray-400 uppercase tracking-wider">Market Data Pipeline</div>
        <div class="mt-2 flex items-baseline justify-between">
          <span id="metric-trade-count" class="text-2xl font-bold text-white font-mono">0</span>
          <span id="metric-parity" class="text-xs text-gray-400 bg-dark-700 px-2 py-0.5 rounded font-mono">Parity: --</span>
        </div>
        <div class="mt-2 text-xs text-gray-500 flex items-center justify-between">
          <span>1m Bars: <span id="metric-1m-bars" class="text-gray-300 font-mono">0</span></span>
          <span id="metric-tf-bars-label">TF Bars: <span id="metric-15m-bars" class="text-gray-300 font-mono">0</span></span>
        </div>
      </div>
    </section>

    <!-- ACCOUNT SUMMARY STRIP -->
    <section class="bg-dark-800 border border-dark-700 rounded-xl p-4 shadow-sm">
      <div class="flex flex-wrap items-center justify-between gap-4">
        <div class="flex items-center space-x-7">
          <div>
            <div class="text-[10px] text-gray-500 uppercase tracking-wider">Total Equity (This Account)</div>
            <div id="acct-equity" class="text-xl font-bold text-white font-mono">$10,000.00</div>
          </div>
          <div>
            <div class="text-[10px] text-gray-500 uppercase tracking-wider">Available Cash</div>
            <div id="acct-balance" class="text-sm font-semibold text-gray-300 font-mono">$10,000.00</div>
          </div>
          <div>
            <div class="text-[10px] text-gray-500 uppercase tracking-wider">Total Return</div>
            <div id="acct-return" class="text-xl font-bold text-gray-300 font-mono">0.00%</div>
          </div>
          <div>
            <div class="text-[10px] text-gray-500 uppercase tracking-wider">Win / Loss</div>
            <div class="text-sm font-bold font-mono"><span id="acct-wins" class="text-emerald-400">0</span><span class="text-gray-500">W</span> / <span id="acct-losses" class="text-red-400">0</span><span class="text-gray-500">L</span></div>
          </div>
          <div>
            <div class="text-[10px] text-gray-500 uppercase tracking-wider">Profit Factor</div>
            <div id="acct-pf" class="text-sm font-bold text-gray-300 font-mono">N/A</div>
          </div>
          <div>
            <div class="text-[10px] text-gray-500 uppercase tracking-wider">Max Drawdown</div>
            <div id="acct-dd" class="text-sm font-bold text-gray-300 font-mono">0.00%</div>
          </div>
          <div>
            <div class="text-[10px] text-gray-500 uppercase tracking-wider">Fees Paid</div>
            <div id="acct-fees" class="text-sm font-bold text-gray-300 font-mono">$0.00</div>
          </div>
        </div>
        <div class="flex items-center space-x-2">
          <span class="text-[10px] text-gray-500 uppercase">Trades:</span>
          <span id="acct-trades" class="text-xs font-bold text-white font-mono">0</span>
        </div>
      </div>
    </section>

    <!-- CHART & SIGNAL FEED SPLIT -->
    <section class="grid grid-cols-1 lg:grid-cols-3 gap-6">

      <!-- LEFT: CANDLESTICK CHART (2 COLS) -->
      <div class="lg:col-span-2 bg-dark-800 border border-dark-700 rounded-xl p-4 shadow-sm flex flex-col">
        <div class="flex flex-wrap items-center justify-between pb-3 border-b border-dark-700 mb-3 gap-2">
          <div class="flex items-center space-x-3">
            <span class="font-semibold text-white text-sm">Candlestick Chart</span>
            <span id="chart-tf-label" class="text-xs px-2 py-0.5 rounded bg-amber-400/10 border border-amber-400/20 text-amber-400 font-mono">--</span>
          </div>

          <!-- TIMEFRAME SELECTOR TOOLBAR -->
          <div id="tf-toolbar" class="flex items-center space-x-1 bg-dark-900/90 p-1 rounded-lg border border-dark-700">
            <button onclick="setTimeframe('1m')" id="tf-btn-1m" class="tf-btn px-2.5 py-1 text-xs font-semibold rounded text-gray-400 hover:text-white transition">1m</button>
            <button onclick="setTimeframe('3m')" id="tf-btn-3m" class="tf-btn px-2.5 py-1 text-xs font-semibold rounded text-gray-400 hover:text-white transition">3m</button>
            <button onclick="setTimeframe('5m')" id="tf-btn-5m" class="tf-btn px-2.5 py-1 text-xs font-semibold rounded text-gray-400 hover:text-white transition">5m</button>
            <button onclick="setTimeframe('15m')" id="tf-btn-15m" class="tf-btn px-2.5 py-1 text-xs font-semibold rounded text-gray-400 hover:text-white transition">15m</button>
            <button onclick="setTimeframe('30m')" id="tf-btn-30m" class="tf-btn px-2.5 py-1 text-xs font-semibold rounded text-gray-400 hover:text-white transition">30m</button>
            <button onclick="setTimeframe('1h')" id="tf-btn-1h" class="tf-btn px-2.5 py-1 text-xs font-semibold rounded text-gray-400 hover:text-white transition">1h</button>
            <button onclick="setTimeframe('4h')" id="tf-btn-4h" class="tf-btn px-2.5 py-1 text-xs font-semibold rounded text-gray-400 hover:text-white transition">4h</button>
          </div>

          <div class="flex items-center space-x-2 text-xs">
            <span class="w-2 h-2 rounded-full bg-emerald-400 inline-block pulse-dot"></span>
            <span class="text-gray-300 font-mono text-[11px]">Real-time feed</span>
          </div>
        </div>
        <div id="chart-container" class="w-full h-[470px] rounded-lg overflow-hidden bg-dark-900"></div>
      </div>

      <!-- RIGHT: LIVE DECISION & SIGNAL FEED (1 COL) -->
      <div class="bg-dark-800 border border-dark-700 rounded-xl p-4 shadow-sm flex flex-col">
        <div class="flex items-center justify-between pb-3 border-b border-dark-700 mb-3">
          <div class="flex items-center space-x-2">
            <span class="font-semibold text-white text-sm">Strategy Decisions</span>
            <span id="signal-count-badge" class="text-xs px-2 py-0.5 rounded-full bg-dark-700 text-gray-300 font-mono">0</span>
          </div>
          <span class="text-xs text-gray-500">Next close: <span id="candle-countdown" class="text-amber-400 font-mono">--:--</span></span>
        </div>
        <div id="signals-feed" class="flex-1 overflow-y-auto space-y-3 max-h-[470px] pr-1">
          <div class="text-center py-16 text-gray-500 text-xs">Waiting for strategy signals...</div>
        </div>
      </div>
    </section>

    <!-- ORDERS & AUDIT HISTORY -->
    <section class="bg-dark-800 border border-dark-700 rounded-xl p-4 shadow-sm">
      <div class="flex items-center justify-between pb-3 border-b border-dark-700 mb-3">
        <div class="flex items-center space-x-3">
          <span class="font-semibold text-white text-sm">Order Execution & Audit Log</span>
          <span class="text-xs text-gray-500 font-mono">Immutable SQLite WAL Ledger</span>
        </div>
        <span class="text-xs text-gray-400">All execution events are persisted idempotently</span>
      </div>
      <div class="overflow-x-auto">
        <table class="w-full text-left text-xs font-mono">
          <thead class="bg-dark-900/60 text-gray-400 border-b border-dark-700">
            <tr>
              <th class="py-2.5 px-3">Time (UTC)</th>
              <th class="py-2.5 px-3">Client Order ID</th>
              <th class="py-2.5 px-3">Side</th>
              <th class="py-2.5 px-3" id="table-qty-header">Qty</th>
              <th class="py-2.5 px-3">Target Price</th>
              <th class="py-2.5 px-3">Fill Price</th>
              <th class="py-2.5 px-3">Fee (USDT)</th>
              <th class="py-2.5 px-3">Status</th>
              <th class="py-2.5 px-3">Reason</th>
            </tr>
          </thead>
          <tbody id="orders-tbody" class="divide-y divide-dark-700/50">
            <tr>
              <td colspan="9" class="text-center py-8 text-gray-500">No orders executed in database yet.</td>
            </tr>
          </tbody>
        </table>
      </div>
    </section>

    <!-- TRADE JOURNAL -->
    <section id="trade-journal-section" class="bg-dark-800 border border-dark-700 rounded-xl p-4 shadow-sm">
      <div class="flex items-center justify-between pb-3 border-b border-dark-700 mb-3">
        <div class="flex items-center space-x-3">
          <span class="font-semibold text-white text-sm">Trade Journal</span>
          <span id="journal-count" class="text-xs px-2 py-0.5 rounded-full bg-dark-700 text-gray-300 font-mono">0 round-trips</span>
        </div>
      </div>
      <div class="overflow-x-auto">
        <table class="w-full text-left text-xs font-mono">
          <thead class="bg-dark-900/60 text-gray-400 border-b border-dark-700">
            <tr>
              <th class="py-2.5 px-3">#</th>
              <th class="py-2.5 px-3">Entry</th>
              <th class="py-2.5 px-3">Exit</th>
              <th class="py-2.5 px-3">Side</th>
              <th class="py-2.5 px-3">Qty</th>
              <th class="py-2.5 px-3">Entry $</th>
              <th class="py-2.5 px-3">Exit $</th>
              <th class="py-2.5 px-3">P&amp;L</th>
              <th class="py-2.5 px-3">Hold Time</th>
            </tr>
          </thead>
          <tbody id="journal-tbody" class="divide-y divide-dark-700/50">
            <tr><td colspan="9" class="text-center py-8 text-gray-500">No completed round-trip trades yet.</td></tr>
          </tbody>
        </table>
      </div>
    </section>

    <!-- SYSTEM HEALTH -->
    <section class="bg-dark-800 border border-dark-700 rounded-xl p-3 shadow-sm">
      <div class="flex flex-wrap items-center justify-between gap-3 text-xs">
        <div class="flex items-center space-x-2">
          <span class="text-gray-400">&#128737; System Health</span>
        </div>
        <div class="flex items-center space-x-4 font-mono">
          <span class="text-gray-500">Incidents: <span id="health-total" class="text-gray-300">0</span></span>
          <span class="text-gray-500">Errors: <span id="health-errors" class="text-red-400">0</span></span>
          <span class="text-gray-500">Warnings: <span id="health-warnings" class="text-amber-400">0</span></span>
          <span id="health-last" class="text-gray-500 truncate max-w-[300px]"></span>
        </div>
      </div>
    </section>
  </main>

  <footer class="border-t border-dark-700 py-3 px-6 text-xs text-gray-500 flex flex-wrap justify-between items-center gap-2 mt-auto">
    <div>Escanor Live Engine &bull; Binance USD-M Futures &bull; Interactive Multi-Timeframe Supertrend</div>
    <div class="flex items-center space-x-4 font-mono">
      <span>Storage: SQLite WAL</span>
      <span>Safety: Fail-Closed</span>
      <span>API: Localhost</span>
    </div>
  </footer>

  <script>
    // Theme (dark / light)
    const dashboardConfig = {};
    function modeBadge(mode) {
      return {PAPER: 'PAPER MODE', SHADOW: 'SHADOW MODE · ZERO RISK', TESTNET: 'TESTNET MODE', LIVE: 'LIVE PRODUCTION'}[mode];
    }
    function escapeHtml(value) {
      const el = document.createElement('span');
      el.textContent = String(value ?? '');
      return el.innerHTML;
    }
    if (dashboardConfig.symbol) {
      document.getElementById('nav-symbol').textContent = dashboardConfig.symbol;
      document.getElementById('nav-mode').textContent = modeBadge(dashboardConfig.mode);
      document.getElementById('nav-tf-badge').textContent = dashboardConfig.timeframe;
    }
    if (dashboardConfig.readOnly) {
      const halt = document.getElementById('btn-kill');
      halt.disabled = true;
      halt.textContent = 'HALT: CLI ONLY';
      halt.title = 'Shared emergency halt is managed through the operator CLI. This dashboard is read-only.';
    }
    function applyChartTheme(theme) {
      if (typeof chart === 'undefined' || !chart) return;
      if (theme === 'light') {
        chart.applyOptions({
          layout: { background: { color: '#F1E7D7' }, textColor: '#3F4A3C' },
          grid: { vertLines: { color: '#A2B29F66' }, horzLines: { color: '#A2B29F66' } },
          rightPriceScale: { borderColor: '#A2B29F' },
          timeScale: { borderColor: '#A2B29F' },
        });
      } else {
        chart.applyOptions({
          layout: { background: { color: '#0B0F19' }, textColor: '#94A3B8' },
          grid: { vertLines: { color: '#1E293B' }, horzLines: { color: '#1E293B' } },
          rightPriceScale: { borderColor: '#334155' },
          timeScale: { borderColor: '#334155' },
        });
      }
    }
    function setThemeLabel(theme) {
      const btn = document.getElementById('btn-theme');
      if (btn) btn.innerText = theme === 'light' ? '🌙 DARK' : '☀️ LIGHT';
    }
    function toggleTheme() {
      const root = document.documentElement;
      const next = (root.getAttribute('data-theme') === 'light') ? 'dark' : 'light';
      root.setAttribute('data-theme', next);
      try { localStorage.setItem('escanor-theme', next); } catch (e) {}
      setThemeLabel(next);
      applyChartTheme(next);
    }
    (function initTheme() {
      let saved = 'dark';
      try { saved = localStorage.getItem('escanor-theme') || 'dark'; } catch (e) {}
      document.documentElement.setAttribute('data-theme', saved);
      // label applied once DOM ready (script is at end of body, element exists)
      setThemeLabel(saved);
    })();
    // Clock
    setInterval(() => {
      document.getElementById('clock-utc').innerText = new Date().toISOString().replace('T', ' ').substring(11, 19) + ' UTC';
    }, 1000);

    // Active timeframe state
    let currentTimeframe = '';
    let isTimeframeInitialized = false;

    function setTimeframe(tf) {
      if (tf === currentTimeframe) return;
      currentTimeframe = tf;
      isTimeframeInitialized = true;

      // Update button styles
      document.querySelectorAll('.tf-btn').forEach(btn => {
        btn.className = 'tf-btn px-2.5 py-1 text-xs font-semibold rounded text-gray-400 hover:text-white transition';
      });
      const activeBtn = document.getElementById('tf-btn-' + tf);
      if (activeBtn) {
        activeBtn.className = 'tf-btn px-2.5 py-1 text-xs font-semibold rounded bg-amber-500 text-black shadow font-bold transition';
      }

      if (document.getElementById('chart-tf-label')) document.getElementById('chart-tf-label').innerText = tf;
      if (document.getElementById('metric-st-tf')) document.getElementById('metric-st-tf').innerText = tf;

      // Reset rendered count to force redraw of chart with new timeframe candles
      renderedKey = '';
      candleSeries.setData([]);
      volumeSeries.setData([]);
      if (indicatorSeries) indicatorSeries.setData([]);
      if (slowSeries) slowSeries.setData([]);
      candleSeries.setMarkers([]);
      if (pollTimer) {
        clearTimeout(pollTimer);
        pollTimer = null;
      }
      updateDashboard();
    }

    // Initialize TradingView Lightweight Chart
    let chart, candleSeries, volumeSeries, slowSeries;
    const ST_COLORS = { bullish: '#10B981', bearish: '#EF4444', neutral: '#94A3B8' };
    const ST_BREAK = 'rgba(0,0,0,0)';
    let indicatorSeries = null;
    const chartContainer = document.getElementById('chart-container');

    function initChart() {
      chart = LightweightCharts.createChart(chartContainer, {
        width: chartContainer.clientWidth,
        height: 470,
        layout: {
          background: { color: '#0B0F19' },
          textColor: '#94A3B8',
        },
        grid: {
          vertLines: { color: '#1E293B' },
          horzLines: { color: '#1E293B' },
        },
        crosshair: {
          mode: LightweightCharts.CrosshairMode.Normal,
        },
        rightPriceScale: {
          borderColor: '#334155',
        },
        timeScale: {
          borderColor: '#334155',
          timeVisible: true,
          secondsVisible: false,
        },
      });

      indicatorSeries = chart.addLineSeries({ color: ST_COLORS.neutral, lineWidth: 2, title: '', lastValueVisible: true, priceLineVisible: true });
      slowSeries = chart.addLineSeries({ color: '#F59E0B', lineWidth: 1, title: 'EMA Slow', lastValueVisible: true, priceLineVisible: false });

      candleSeries = chart.addCandlestickSeries({
        upColor: '#10B981',
        downColor: '#EF4444',
        borderVisible: false,
        wickUpColor: '#10B981',
        wickDownColor: '#EF4444',
      });

      volumeSeries = chart.addHistogramSeries({
        color: '#26a69a',
        priceFormat: { type: 'volume' },
        priceScaleId: '',
        lastValueVisible: false,
        priceLineVisible: false,
      });
      // scaleMargins belongs to the price scale; as a series option it is silently ignored
      // and the volume overlay then autoscales over the whole pane, hiding the candles.
      volumeSeries.priceScale().applyOptions({ scaleMargins: { top: 0.8, bottom: 0 } });
      candleSeries.priceScale().applyOptions({ scaleMargins: { top: 0.08, bottom: 0.24 } });

      window.addEventListener('resize', () => {
        chart.applyOptions({ width: chartContainer.clientWidth });
      });
    }

    initChart();
    applyChartTheme(document.documentElement.getAttribute('data-theme') || 'dark');

    // lightweight-charts joins consecutive line items and ignores whitespace, so a hole in the
    // data cannot break the line. A point's color paints the segment LEAVING it, so a transparent
    // point at every regime flip / missing candle is the break.
    function renderIndicator(data) {
      if (!indicatorSeries) return;
      const points = (data.supertrend_line || []).map(p => ({
        time: p.time > 100000000000 ? Math.floor(p.time / 1000) : p.time,
        value: parseFloat(p.value),
        state: p.state,
      }));
      const gaps = points.slice(1).map((p, i) => p.time - points[i].time).sort((a, b) => a - b);
      const step = gaps.length ? gaps[Math.floor(gaps.length / 2)] : 0;
      indicatorSeries.setData(points.map((p, i) => {
        const next = points[i + 1];
        const broken = next && (next.state !== p.state || (step > 0 && next.time - p.time > 1.5 * step));
        return { time: p.time, value: p.value, color: broken ? ST_BREAK : (ST_COLORS[p.state] || ST_COLORS.neutral) };
      }));
      // Second leg of a cross indicator (ZEC EMA 10/100); empty for single-line Supertrend.
      if (slowSeries) {
        slowSeries.setData((data.indicator_slow_line || []).map(p => ({
          time: p.time > 100000000000 ? Math.floor(p.time / 1000) : p.time,
          value: parseFloat(p.value),
        })));
      }
      const state = points.length ? points[points.length - 1].state : 'neutral';
      indicatorSeries.applyOptions({
        color: ST_COLORS[state] || ST_COLORS.neutral,
        title: (data.indicator_name || 'Supertrend') + ' ' + ({ bullish: '↑', bearish: '↓', neutral: '•' })[state],
      });
    }

    let renderedKey = '';
    let requestSequence = 0;
    let latestRenderedRequestId = 0;
    let pollTimer = null;

    // Fetch and update dashboard data
    async function updateDashboard() {
      if (pollTimer) {
        clearTimeout(pollTimer);
        pollTimer = null;
      }
      const requestId = ++requestSequence;
      try {
        const endpoint = dashboardConfig.symbol ? '/' + dashboardConfig.symbol + '/api/data' : '/api/data';
        const query = new URLSearchParams(window.location.search);
        query.set('mode', dashboardConfig.mode);
        if (currentTimeframe) query.set('timeframe', currentTimeframe);
        const url = endpoint + '?' + query;
        const res = await fetch(url);
        if (!res.ok) throw new Error('HTTP ' + res.status);
        const data = await res.json();
        if (requestId < latestRenderedRequestId) return;
        latestRenderedRequestId = requestId;
        document.getElementById('dashboard-status').textContent = data.feed_status || 'Waiting for data';
        const paramPrefix = (data.strategy_name && data.strategy_name.includes('EMA')) ? 'EMA ' : 'Supertrend ';
        document.getElementById('configured-strategy').textContent = 'Configured strategy: ' + data.strategy_name + ' · ' + data.strategy_timeframe + ' · ' + paramPrefix + data.strategy_supertrend_params;

        document.querySelectorAll('[data-metric]').forEach(el => {
          const value = data.strategy_metrics?.[el.dataset.metric];
          el.textContent = value == null ? '--' : typeof value === 'number' ? value.toFixed(4) : value;
        });
        const gates = document.getElementById('safety-gates');
        if (gates && data.safety_gates) {
          gates.textContent = data.safety_gates.map(g => `${g.gate_id}. ${g.name}: ${g.passed ? 'PASS' : 'FAIL'} — ${g.details}`).join('\n');
        }

        // 0. Synchronize initial timeframe with strategy timeframe from server
        if (!isTimeframeInitialized && data.timeframe) {
          currentTimeframe = data.timeframe;
          isTimeframeInitialized = true;
          document.querySelectorAll('.tf-btn').forEach(btn => {
            btn.className = 'tf-btn px-2.5 py-1 text-xs font-semibold rounded text-gray-400 hover:text-white transition';
          });
          const activeBtn = document.getElementById('tf-btn-' + currentTimeframe);
          if (activeBtn) {
            activeBtn.className = 'tf-btn px-2.5 py-1 text-xs font-semibold rounded bg-amber-500 text-black shadow font-bold transition';
          }
        }

        // 1. Header and Badges
        document.getElementById('nav-symbol').innerText = data.symbol || 'BTCUSDT';
        document.getElementById('nav-mode').innerText = modeBadge(data.mode || 'SHADOW');
        if (data.timeframe) {
          if (document.getElementById('nav-tf-badge')) document.getElementById('nav-tf-badge').innerText = data.strategy_timeframe;
          if (document.getElementById('chart-tf-label')) document.getElementById('chart-tf-label').innerText = data.timeframe;
          if (document.getElementById('metric-st-tf')) document.getElementById('metric-st-tf').innerText = data.timeframe;
        }

        const killStatusEl = document.getElementById('kill-status-text');
        const killBadge = document.getElementById('kill-badge');
        const btnKill = document.getElementById('btn-kill');

        if (data.kill_switch_engaged) {
          killStatusEl.innerText = 'ENGAGED (HALTED)';
          killBadge.className = 'flex items-center space-x-1.5 px-3 py-1 rounded-full text-xs font-semibold bg-red-500/10 text-red-400 border border-red-500/20';
          btnKill.innerText = 'DISENGAGE HALT';
          btnKill.className = 'px-3 py-1 text-xs font-semibold rounded bg-emerald-600/20 hover:bg-emerald-600/40 text-emerald-400 border border-emerald-500/30 transition';
        } else {
          killStatusEl.innerText = 'CLEAR';
          killBadge.className = 'flex items-center space-x-1.5 px-3 py-1 rounded-full text-xs font-semibold bg-emerald-500/10 text-emerald-400 border border-emerald-500/20';
          btnKill.innerText = 'EMERGENCY HALT';
          btnKill.className = 'px-3 py-1 text-xs font-semibold rounded bg-red-600/20 hover:bg-red-600/40 text-red-400 border border-red-500/30 transition';
        }
        if (dashboardConfig.readOnly) btnKill.textContent = 'HALT: CLI ONLY';

        // 2. Metrics
        if (data.latest_price) {
          document.getElementById('metric-price').innerText = '$' + parseFloat(data.latest_price).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
        }
        document.getElementById('metric-updated').innerText = data.feed_status || 'Waiting for data';
        document.getElementById('metric-latency').innerText = data.latency_ms == null ? '-- ms' : data.latency_ms + ' ms';

        // Position & PnL
        const posQty = parseFloat(data.position?.quantity || 0);
        const posEl = document.getElementById('metric-position-qty');
        const baseAsset = (data.symbol || 'BTC').replace(/USDT$/, '');
        posEl.innerText = posQty.toFixed(4) + ' ' + baseAsset;

        // Update table header with correct asset
        const qtyHeaderEl = document.getElementById('table-qty-header');
        if (qtyHeaderEl) qtyHeaderEl.innerText = 'Qty (' + baseAsset + ')';
        if (posQty > 0) {
          posEl.className = 'text-2xl font-bold text-emerald-400 font-mono';
          document.getElementById('metric-open-status').innerText = 'LONG';
          document.getElementById('metric-open-status').className = 'text-emerald-400 font-bold';
        } else {
          posEl.className = 'text-2xl font-bold text-white font-mono';
          document.getElementById('metric-open-status').innerText = 'FLAT';
          document.getElementById('metric-open-status').className = 'text-gray-400';
        }

        if (data.position?.entry_price) {
          document.getElementById('metric-entry-px').innerText = '$' + parseFloat(data.position.entry_price).toLocaleString('en-US', { minimumFractionDigits: 2 });
        }

        const realizedPnl = parseFloat(data.position?.realized_pnl || 0);
        const pnlEl = document.getElementById('metric-realized-pnl');
        pnlEl.innerText = 'Realized PnL: ' + (realizedPnl >= 0 ? '+$' : '-$') + Math.abs(realizedPnl).toFixed(2);
        pnlEl.className = realizedPnl >= 0 ? 'text-xs font-mono font-semibold px-2 py-0.5 rounded bg-emerald-500/10 text-emerald-400' : 'text-xs font-mono font-semibold px-2 py-0.5 rounded bg-red-500/10 text-red-400';

        // Strategy indicator state for selected timeframe
        const stDirEl = document.getElementById('metric-st-direction');
        const stValEl = document.getElementById('metric-st-val');
        const actionEl = document.getElementById('metric-last-action');

        if (data.supertrend_direction) {
          if (data.supertrend_direction === 'BULLISH' || data.supertrend_direction === 1) {
            stDirEl.innerText = 'BULLISH ↑';
            stDirEl.className = 'text-sm font-bold px-2.5 py-1 rounded bg-emerald-500/20 text-emerald-400 border border-emerald-500/30';
            stValEl.className = 'text-xs font-mono text-emerald-400';
          } else if (data.supertrend_direction === 'BEARISH' || data.supertrend_direction === -1) {
            stDirEl.innerText = 'BEARISH ↓';
            stDirEl.className = 'text-sm font-bold px-2.5 py-1 rounded bg-red-500/20 text-red-400 border border-red-500/30';
            stValEl.className = 'text-xs font-mono text-red-400';
          } else {
            stDirEl.innerText = 'NEUTRAL •';
            stDirEl.className = 'text-sm font-bold px-2.5 py-1 rounded bg-dark-700 text-gray-300';
            stValEl.className = 'text-xs font-mono text-gray-400';
          }
        }
        if (data.supertrend_val) {
          const valPrefix = data.indicator_name === 'Momentum EMA' ? 'EMA: $' : 'ST: $';
          stValEl.innerText = valPrefix + parseFloat(data.supertrend_val).toLocaleString('en-US', { minimumFractionDigits: 2 });
        }
        if (data.supertrend_params && document.getElementById('metric-st-label')) {
          const arrows = { BULLISH: '↑', BEARISH: '↓', NEUTRAL: '•' };
          document.getElementById('metric-st-label').innerText = (data.indicator_name || 'Supertrend') + ' ' + (arrows[data.supertrend_direction] || '•') + ' ' + data.supertrend_params;
        }
        if (data.timeframe_decision) {
          actionEl.innerText = data.timeframe_decision;
          actionEl.className = data.timeframe_decision.includes('BUY')
            ? 'font-bold text-emerald-400'
            : (data.timeframe_decision.includes('SELL')
              ? 'font-bold text-red-400'
              : (data.timeframe_decision.includes('HOLD') || data.timeframe_decision.includes('BULLISH') || data.timeframe_decision.includes('LONG')
                ? 'font-semibold text-emerald-400/80'
                : 'font-semibold text-gray-300'));
        }

        // Ingestion stats
        document.getElementById('metric-trade-count').innerText = (data.trades_count || 0).toLocaleString();
        document.getElementById('metric-1m-bars').innerText = data.candles_1m_count || 0;
        document.getElementById('metric-15m-bars').innerText = data.candles_15m_count || 0;
        const tfBarsLabel = document.getElementById('metric-tf-bars-label');
        if (tfBarsLabel && (data.strategy_timeframe || data.timeframe)) {
          const tfText = (data.strategy_timeframe || data.timeframe);
          tfBarsLabel.innerHTML = `${tfText} Bars: <span id="metric-15m-bars" class="text-gray-300 font-mono">${data.candles_15m_count || 0}</span>`;
        }

        // 3. Update Chart Candles
        if (data.candles && data.candles.length > 0) {
          const currentKey = data.timeframe + '_' + data.candles.length + '_' + (data.candles[data.candles.length - 1].close) + '_' + data.supertrend_direction + '_' + (data.trade_markers || []).length;
          if (currentKey !== renderedKey) {
            const candleData = data.candles.map(c => ({
              time: Math.floor(c.open_time / 1000),
              open: parseFloat(c.open),
              high: parseFloat(c.high),
              low: parseFloat(c.low),
              close: parseFloat(c.close),
            }));

            const volData = data.candles.map(c => ({
              time: Math.floor(c.open_time / 1000),
              value: parseFloat(c.volume),
              color: parseFloat(c.close) >= parseFloat(c.open) ? '#10B98144' : '#EF444444',
            }));

            candleSeries.setData(candleData);
            volumeSeries.setData(volData);

            renderIndicator(data);

            // Set trade markers
            if (data.trade_markers && data.trade_markers.length > 0) {
              candleSeries.setMarkers(data.trade_markers);
            } else {
              candleSeries.setMarkers([]);
            }

            chart.timeScale().fitContent();
            renderedKey = currentKey;
          }
        }

        // 4. Update Signals Feed
        const feedContainer = document.getElementById('signals-feed');
        document.getElementById('signal-count-badge').innerText = (data.signals || []).length;

        if (data.signals && data.signals.length > 0) {
          feedContainer.innerHTML = data.signals.map(s => {
            const isBuy = s.action === 'ENTER_LONG' || s.action === 'BUY';
            const isSell = s.action === 'EXIT_LONG' || s.action === 'SELL';
            const badgeClass = isBuy
              ? 'bg-emerald-500/20 text-emerald-400 border border-emerald-500/30'
              : isSell
              ? 'bg-red-500/20 text-red-400 border border-red-500/30'
              : 'bg-dark-700 text-gray-400';

            const actionLabel = isBuy ? 'BUY / ENTER LONG' : isSell ? 'SELL / EXIT LONG' : s.action;

            return `
              <div class="p-3 bg-dark-900/80 border border-dark-700 rounded-lg space-y-1.5 hover:border-dark-600 transition">
                <div class="flex items-center justify-between">
                  <span class="px-2 py-0.5 rounded text-xs font-bold ${badgeClass}">${escapeHtml(actionLabel)}</span>
                  <span class="text-xs text-gray-500 font-mono">${escapeHtml(s.timestamp_utc || '')}</span>
                </div>
                <div class="flex items-center justify-between text-xs font-mono">
                  <span class="text-gray-400">Trigger Price:</span>
                  <span class="text-white font-bold">$${parseFloat(s.price || 0).toLocaleString('en-US', { minimumFractionDigits: 2 })}</span>
                </div>
                <div class="text-xs text-gray-400 truncate">${escapeHtml(s.reason || 'Supertrend indicator signal')}</div>
              </div>
            `;
          }).join('');
        } else {
          const indName = data.indicator_name || 'Strategy';
          feedContainer.innerHTML = '<div class="text-center py-16 text-gray-500 text-xs">No signals generated yet. ' + escapeHtml(indName) + ' evaluates at each candle close.</div>';
        }

        // 5. Update Orders Table
        const tbody = document.getElementById('orders-tbody');
        if (data.orders && data.orders.length > 0) {
          tbody.innerHTML = data.orders.map(o => {
            const isBuy = o.side === 'BUY';
            const sideClass = isBuy ? 'text-emerald-400 font-bold' : 'text-red-400 font-bold';
            const statusClass = o.status === 'FILLED' ? 'bg-emerald-500/10 text-emerald-400 border border-emerald-500/20'
              : o.status === 'REJECTED' ? 'bg-red-500/10 text-red-400 border border-red-500/20'
              : 'bg-dark-700 text-gray-300';

            return `
              <tr class="hover:bg-dark-700/30 transition">
                <td class="py-2.5 px-3 text-gray-400">${escapeHtml(o.created_at_utc || '--')}</td>
                <td class="py-2.5 px-3 text-gray-300 truncate max-w-[180px]">${escapeHtml(o.client_order_id)}</td>
                <td class="py-2.5 px-3 ${sideClass}">${escapeHtml(o.side)}</td>
                <td class="py-2.5 px-3 text-white font-bold">${parseFloat(o.quantity || 0).toFixed(4)}</td>
                <td class="py-2.5 px-3 text-gray-400">$${parseFloat(o.price || 0).toFixed(2)}</td>
                <td class="py-2.5 px-3 text-white">$${parseFloat(o.avg_fill_price || o.price || 0).toFixed(2)}</td>
                <td class="py-2.5 px-3 text-gray-400">$${parseFloat(o.accumulated_fees || 0).toFixed(4)}</td>
                <td class="py-2.5 px-3"><span class="px-2 py-0.5 rounded text-[10px] font-semibold ${statusClass}">${escapeHtml(o.status)}</span></td>
                <td class="py-2.5 px-3 text-gray-500 truncate max-w-[160px]">${escapeHtml(o.rejection_reason || '')}</td>
              </tr>
            `;
          }).join('');
        } else {
          tbody.innerHTML = '<tr><td colspan="9" class="text-center py-8 text-gray-500">No orders placed yet. Watching for next signal.</td></tr>';
        }

        // 6. Account Summary Strip
        const equity = parseFloat(data.total_equity || data.paper_balance || 0);
        const eqEl = document.getElementById('acct-equity');
        if (eqEl) eqEl.innerText = '$' + equity.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });

        const bal = parseFloat(data.paper_balance || 0);
        document.getElementById('acct-balance').innerText = '$' + bal.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
        const retPct = parseFloat(data.total_return_pct || 0);
        const retEl = document.getElementById('acct-return');
        retEl.innerText = (retPct >= 0 ? '+' : '') + retPct.toFixed(2) + '%';
        retEl.className = retPct >= 0
          ? 'text-xl font-bold text-emerald-400 font-mono'
          : 'text-xl font-bold text-red-400 font-mono';
        document.getElementById('acct-wins').innerText = data.win_count || 0;
        document.getElementById('acct-losses').innerText = data.loss_count || 0;
        document.getElementById('acct-pf').innerText = data.profit_factor || 'N/A';
        document.getElementById('acct-dd').innerText = '-' + (data.max_drawdown_pct || '0.00') + '%';
        document.getElementById('acct-fees').innerText = '$' + parseFloat(data.total_fees_paid || 0).toFixed(2);
        
        const openPosQty = parseFloat(data.position?.quantity || 0);
        const closedCount = data.completed_trades || 0;
        if (openPosQty > 0) {
          document.getElementById('acct-trades').innerHTML = `<span class="text-gray-300 font-bold">${closedCount}</span> <span class="text-gray-500">closed</span> <span class="text-emerald-400 font-semibold">(1 OPEN)</span>`;
        } else {
          document.getElementById('acct-trades').innerHTML = `<span class="text-gray-300 font-bold">${closedCount}</span> <span class="text-gray-500">closed</span>`;
        }

        if (!data.account_available) {
          for (const id of ['acct-equity', 'acct-balance', 'acct-return']) document.getElementById(id).textContent = '--';
        }

        // 7. Unrealized PnL & Notional on Position Card
        const uPnl = parseFloat(data.unrealized_pnl || 0);
        const uPnlEl = document.getElementById('metric-unrealized-pnl');
        uPnlEl.innerText = (uPnl >= 0 ? '+$' : '-$') + Math.abs(uPnl).toFixed(2);
        uPnlEl.className = uPnl >= 0 ? 'font-mono text-emerald-400' : 'font-mono text-red-400';
        const notional = parseFloat(data.position_notional || 0);
        document.getElementById('metric-notional').innerText = '$' + notional.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });

        // 8. Candle Countdown
        if (data.next_candle_close_ms > 0) {
          const totalSec = Math.floor(data.next_candle_close_ms / 1000);
          const mm = Math.floor(totalSec / 60);
          const ss = totalSec % 60;
          document.getElementById('candle-countdown').innerText = mm + 'm ' + String(ss).padStart(2, '0') + 's';
        }

        // 9. Trade Journal
        const jTbody = document.getElementById('journal-tbody');
        const jCount = document.getElementById('journal-count');
        if (data.trade_journal && data.trade_journal.length > 0) {
          jCount.innerText = data.trade_journal.length + ' round-trips';
          jTbody.innerHTML = data.trade_journal.map((t, i) => {
            const pnlClass = t.pnl >= 0 ? 'text-emerald-400' : 'text-red-400';
            const pnlIcon = t.pnl >= 0 ? '✅' : '❌';
            const holdMin = Math.floor(t.hold_seconds / 60);
            const holdH = Math.floor(holdMin / 60);
            const holdStr = holdH > 0 ? holdH + 'h ' + (holdMin % 60) + 'm' : holdMin + 'm';
            return `
              <tr class="hover:bg-dark-700/30 transition">
                <td class="py-2 px-3 text-gray-500">${data.trade_journal.length - i}</td>
                <td class="py-2 px-3 text-gray-300">${t.entry_time}</td>
                <td class="py-2 px-3 text-gray-300">${t.exit_time}</td>
                <td class="py-2 px-3 text-emerald-400 font-bold">${t.side}</td>
                <td class="py-2 px-3 text-white">${t.qty}</td>
                <td class="py-2 px-3 text-gray-400">$${t.entry_price}</td>
                <td class="py-2 px-3 text-gray-400">$${t.exit_price}</td>
                <td class="py-2 px-3 ${pnlClass} font-bold">${pnlIcon} ${t.pnl >= 0 ? '+' : ''}$${t.pnl.toFixed(2)}</td>
                <td class="py-2 px-3 text-gray-500">${holdStr}</td>
              </tr>
            `;
          }).join('');
        } else {
          jCount.innerText = '0 round-trips';
          jTbody.innerHTML = '<tr><td colspan="9" class="text-center py-8 text-gray-500">No completed round-trip trades yet.</td></tr>';
        }

        // 10. System Health
        if (data.incidents_summary) {
          const inc = data.incidents_summary;
          document.getElementById('health-total').innerText = inc.total;
          document.getElementById('health-errors').innerText = inc.errors;
          document.getElementById('health-warnings').innerText = inc.warnings;
          const lastEl = document.getElementById('health-last');
          if (inc.last_detail) {
            const agoH = Math.floor((inc.last_ago || 0) / 3600);
            const agoM = Math.floor(((inc.last_ago || 0) % 3600) / 60);
            const agoStr = agoH > 0 ? agoH + 'h ago' : agoM + 'm ago';
            lastEl.innerText = 'Last: "' + inc.last_detail.substring(0, 60) + '" ' + agoStr;
          } else {
            lastEl.innerText = '';
          }
        }

      } catch (err) {
        if (requestId < latestRenderedRequestId) return;
        document.getElementById('dashboard-status').textContent = 'Dashboard disconnected — displayed values may be stale';
        document.getElementById('metric-updated').textContent = 'DISCONNECTED';
        document.getElementById('kill-status-text').textContent = 'UNKNOWN';
        console.warn("Dashboard polling error:", err);
      } finally {
        if (!pollTimer) {
          pollTimer = setTimeout(updateDashboard, 2000);
        }
      }
    }

    // Initial load
    updateDashboard();
  </script>
</body>
</html>
"""


class DashboardDataAggregator:
    """Aggregates real-time data from SQLite and historical parquet files with dynamic multi-timeframe resampling."""

    def __init__(self, config: LiveEngineConfig, *, base_dir: Optional[Path | str] = None, read_only: bool = True):
        self.config = config
        self.base_dir = Path(base_dir or Path.cwd()).resolve()
        self.read_only = True
        self.db_path = validate_database_path(config.event_store_path, self.base_dir)
        self.candles_dir = self.base_dir / f"{config.symbol}_USDM_DATA/candles"
        self._live_stream_started = False
        self.strategy = None

        # Strategy timeframe and Supertrend configuration
        self.strategy_timeframe = getattr(self.config, "timeframe", "15m").lower()
        self.strategy_name = "Unavailable"
        if self.config.symbol.upper() == "HYPEUSDT":
            self.strategy_st_length = 10
            self.strategy_st_multiplier = 2.0
            self.is_pullback_strategy = False
            self.pb_atr = 0.0
            self.atr_period = 14
        elif self.config.symbol.upper() == "ZECUSDT":
            self.strategy_st_length = 10
            self.strategy_st_multiplier = 100.0
            self.is_pullback_strategy = False
            self.pb_atr = 0.0
            self.atr_period = 14
        elif self.config.symbol.upper() == "LITUSDT":
            self.strategy_st_length = 28
            self.strategy_st_multiplier = 2.0
            self.is_pullback_strategy = True
            self.pb_atr = 0.5
            self.atr_period = 7
        else:
            self.strategy_st_length = 10
            self.strategy_st_multiplier = 3.0
            self.is_pullback_strategy = True
            self.pb_atr = 0.5
            self.atr_period = 10

        # Attempt loading approved strategy dynamically from manifest for exact values
        try:
            manifest_p = self.base_dir / self.config.manifest_path
            if manifest_p.exists():
                from live_engine.strategy.loader import StrategyLoader
                strat_inst, manifest = StrategyLoader.load_strategy(manifest_p, self.base_dir)
                self.strategy = strat_inst
                self.strategy_name = type(strat_inst).__name__
                strat_tf = getattr(strat_inst, "timeframe", None) or manifest.get("data", {}).get("strategy_timeframe")
                if strat_tf:
                    self.strategy_timeframe = str(strat_tf).lower()
                if hasattr(strat_inst, "st_atr_length"):
                    self.strategy_st_length = int(getattr(strat_inst, "st_atr_length"))
                if hasattr(strat_inst, "st_multiplier"):
                    self.strategy_st_multiplier = float(getattr(strat_inst, "st_multiplier"))
                if hasattr(strat_inst, "pb_atr"):
                    self.is_pullback_strategy = True
                    self.pb_atr = float(getattr(strat_inst, "pb_atr"))
                if hasattr(strat_inst, "atr_period"):
                    self.atr_period = int(getattr(strat_inst, "atr_period"))
                dataset_path = Path(manifest.get("data", {}).get("dataset_candle_path", ""))
                if str(dataset_path):
                    self.candles_dir = (dataset_path if dataset_path.is_absolute() else self.base_dir / dataset_path).parent
        except Exception as e:
            logger.debug(f"Strategy manifest inspection in dashboard: {e}")

    def get_candles_for_timeframe(self, target_tf: str, limit: int = 150) -> List[Dict[str, Any]]:
        return read_chart_candles(
            self.config.symbol, target_tf, self.base_dir, self.db_path, limit,
            candles_dir=self.candles_dir,
        )

    def get_dashboard_payload(self, requested_timeframe: Optional[str] = None) -> Dict[str, Any]:
        """Prepares real-time dashboard data payload for the requested timeframe."""
        timeframe = (requested_timeframe or self.strategy_timeframe or self.config.timeframe).lower()
        if timeframe not in TIMEFRAME_MAP_MS:
            timeframe = self.strategy_timeframe or self.config.timeframe

        ks = KillSwitch(self.base_dir / self.config.kill_switch_path)
        is_killed = ks.is_engaged()

        payload: Dict[str, Any] = {
            "symbol": self.config.symbol,
            "timeframe": timeframe,
            "strategy_timeframe": self.strategy_timeframe,
            "strategy_name": self.strategy_name,
            "strategy_supertrend_params": f"({self.strategy_st_length}, {self.strategy_st_multiplier:.1f})",
            "read_only": self.read_only,
            "database_available": self.db_path.is_file(),
            "feed_status": "WAITING FOR DATA" if self.db_path.is_file() else "DATABASE UNAVAILABLE",
            "last_trade_time_ms": None,
            "mode": self.config.mode,
            "kill_switch_engaged": is_killed,
            "latest_price": None,
            "latency_ms": None,
            "trades_count": 0,
            "candles_1m_count": 0,
            "candles_15m_count": 0,
            "position": {
                "quantity": "0.0000",
                "entry_price": "0.00",
                "realized_pnl": "0.00",
            },
            "supertrend_direction": "NEUTRAL",
            "supertrend_val": None,
            "supertrend_params": f"({self.strategy_st_length}, {self.strategy_st_multiplier:.1f})",
            "indicator_name": "LuxAlgo Rank 12" if self.config.symbol.upper() == "HYPEUSDT" else ("Momentum EMA" if self.config.symbol.upper() == "ZECUSDT" else "Supertrend"),
            "timeframe_decision": "HOLD",
            "candles": [],
            "supertrend_line": [],
            "indicator_slow_line": [],
            "trade_markers": [],
            "signals": [],
            "orders": [],
            # Account summary
            "account_available": False,
            "paper_balance": "10000.00",
            "total_equity": "10000.00",
            "initial_balance": "10000.00",
            "total_return_pct": "0.00",
            "total_fees_paid": "0.00",
            "win_count": 0,
            "loss_count": 0,
            "completed_trades": 0,
            "profit_factor": "N/A",
            "max_drawdown_pct": "0.00",
            "unrealized_pnl": "0.00",
            "position_notional": "0.00",
            "position_hold_time_s": None,
            "trade_journal": [],
            "incidents_summary": {"total": 0, "errors": 0, "warnings": 0, "last_detail": None, "last_ago": None},
            "next_candle_close_ms": 0,
        }

        # 1. Query Database metrics, signals, orders, and position
        if self.db_path.exists():
            try:
                with read_connection(self.db_path) as conn:
                    conn.row_factory = sqlite3.Row
                    cur = conn.cursor()
                    tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}

                    # Trade count & latest price
                    try:
                        if "raw_aggtrades" not in tables:
                            raise LookupError("Waiting for raw_aggtrades")
                        cur.execute("SELECT COUNT(*) FROM raw_aggtrades WHERE symbol=?;", (self.config.symbol,))
                        row = cur.fetchone()
                        if row and row[0] is not None:
                            payload["trades_count"] = row[0]

                        cur.execute("SELECT price, received_at, trade_time FROM raw_aggtrades WHERE symbol=? ORDER BY trade_time DESC, agg_trade_id DESC LIMIT 1;", (self.config.symbol,))
                        latest_trade = cur.fetchone()
                        if latest_trade:
                            payload["latest_price"] = str(latest_trade["price"])
                            payload["last_trade_time_ms"] = latest_trade["trade_time"]
                            payload["latency_ms"] = max(0, latest_trade["received_at"] - latest_trade["trade_time"])
                    except Exception as e:
                        logger.debug(f"Failed to load trade count and latest price from dashboard: {e}")

                    # Candle counts
                    try:
                        if "candles" not in tables:
                            raise LookupError("Waiting for candles")
                        cur.execute("SELECT timeframe, COUNT(*) FROM candles WHERE symbol=? AND is_closed=1 GROUP BY timeframe;", (self.config.symbol,))
                        for tf, cnt in cur.fetchall():
                            if tf == "1m":
                                payload["candles_1m_count"] = cnt
                            if tf == self.strategy_timeframe:
                                payload["candles_15m_count"] = cnt
                            elif tf == "15m" and payload["candles_15m_count"] == 0:
                                payload["candles_15m_count"] = cnt
                    except Exception as e:
                        logger.debug(f"Failed to load candle counts from dashboard: {e}")

                    # Position
                    try:
                        if "audit_events" not in tables:
                            raise LookupError("Waiting for audit_events")
                        cur.execute("SELECT payload_json FROM audit_events WHERE event_type='POSITION_UPDATED' ORDER BY event_id DESC LIMIT 1;")
                        pos_row = cur.fetchone()
                        if pos_row:
                            pos_data = json.loads(pos_row[0])
                            payload["position"] = {
                                "quantity": str(pos_data.get("quantity", "0.0000")),
                                "entry_price": str(pos_data.get("entry_price", "0.00")),
                                "realized_pnl": str(pos_data.get("realized_pnl", "0.00")),
                            }
                    except Exception as e:
                        logger.debug(f"Failed to load position data from dashboard: {e}")

                    # Recent Signals
                    try:
                        if "signals" not in tables:
                            raise LookupError("Waiting for signals")
                        cur.execute("SELECT signal_id, action, reference_price, candle_open_time, reason FROM signals WHERE symbol=? ORDER BY generated_at DESC LIMIT 20;", (self.config.symbol,))
                        for s_row in cur.fetchall():
                            dt_str = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(s_row["candle_open_time"] / 1000))
                            payload["signals"].append({
                                "signal_id": s_row["signal_id"],
                                "action": s_row["action"],
                                "price": s_row["reference_price"],
                                "timestamp_utc": dt_str + " UTC",
                                "reason": s_row["reason"],
                            })
                    except Exception as e:
                        logger.debug(f"Failed to load signals from dashboard: {e}")

                    # Recent Orders
                    try:
                        if "orders" not in tables:
                            raise LookupError("Waiting for orders")
                        cur.execute("SELECT client_order_id, side, quantity, price, status, avg_fill_price, accumulated_fees, created_at, rejection_reason, filled_at FROM orders WHERE symbol=? ORDER BY created_at DESC LIMIT 25;", (self.config.symbol,))
                        for o_row in cur.fetchall():
                            dt_str = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(o_row["created_at"] / 1000))
                            payload["orders"].append({
                                "client_order_id": o_row["client_order_id"],
                                "side": o_row["side"],
                                "quantity": o_row["quantity"],
                                "price": o_row["price"],
                                "status": o_row["status"],
                                "avg_fill_price": o_row["avg_fill_price"],
                                "accumulated_fees": o_row["accumulated_fees"],
                                "created_at_utc": dt_str + " UTC",
                                "created_at_ms": o_row["created_at"],
                                "rejection_reason": o_row["rejection_reason"] or "",
                                "filled_at": o_row["filled_at"],
                            })
                    except Exception as e:
                        logger.debug(f"Failed to load orders from dashboard: {e}")

            except Exception as db_err:
                logger.warning(f"Error querying database for dashboard: {db_err}")

        # --- Account summary, trade journal, incidents ---
        if self.db_path.exists():
            try:
                with read_connection(self.db_path) as conn:
                    conn.row_factory = sqlite3.Row
                    cur = conn.cursor()
                    tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}

                    # PAPER, SHADOW, and LIVE balances come from persisted engine evidence.
                    try:
                        if "audit_events" in tables:
                            rows = conn.execute("SELECT event_type, payload_json FROM audit_events WHERE event_type IN ('PAPER_ACCOUNT_SNAPSHOT', 'BALANCE_RECONCILIATION') ORDER BY event_id DESC LIMIT 1").fetchall()
                            for row in rows:
                                snap = json.loads(row["payload_json"])
                                balance = snap.get("balance") if row["event_type"] == "PAPER_ACCOUNT_SNAPSHOT" else snap.get("balances", {}).get("USDT")
                                if balance is not None:
                                    payload["paper_balance"] = str(balance)
                                    payload["account_available"] = True
                            first = conn.execute("SELECT payload_json FROM audit_events WHERE event_type='BALANCE_RECONCILIATION' ORDER BY event_id LIMIT 1").fetchone()
                            if first:
                                payload["initial_balance"] = json.loads(first[0]).get("balances", {}).get("USDT", "10000.00")
                    except (ValueError, TypeError, AttributeError, sqlite3.Error) as exc:
                        logger.debug("Account snapshot unavailable: %s", exc)

                    # Total fees from all filled orders
                    try:
                        if "orders" not in tables:
                            raise LookupError("Waiting for orders")
                        cur.execute("SELECT SUM(CAST(accumulated_fees AS REAL)) FROM orders WHERE symbol=? AND status='FILLED';", (self.config.symbol,))
                        fee_row = cur.fetchone()
                        if fee_row and fee_row[0] is not None:
                            payload["total_fees_paid"] = f"{fee_row[0]:.4f}"
                    except Exception:
                        pass

                    # Trade journal: pair BUY→SELL fills into round-trips
                    try:
                        cur.execute("SELECT client_order_id, side, quantity, price, avg_fill_price, accumulated_fees, created_at, filled_at, status FROM orders WHERE symbol=? AND status='FILLED' ORDER BY created_at ASC;", (self.config.symbol,))
                        all_fills = cur.fetchall()
                        journal = []
                        pending_entry = None
                        total_wins = 0
                        total_losses = 0
                        gross_profit = 0.0
                        gross_loss = 0.0
                        for fill in all_fills:
                            if fill["side"] == "BUY" and pending_entry is None:
                                pending_entry = fill
                            elif fill["side"] == "SELL" and pending_entry is not None:
                                entry_px = float(pending_entry["avg_fill_price"] or pending_entry["price"])
                                exit_px = float(fill["avg_fill_price"] or fill["price"])
                                qty = float(pending_entry["quantity"])
                                entry_fee = float(pending_entry["accumulated_fees"] or 0)
                                exit_fee = float(fill["accumulated_fees"] or 0)
                                pnl = (exit_px - entry_px) * qty - entry_fee - exit_fee
                                entry_dt = time.strftime("%b %d %H:%M", time.gmtime(pending_entry["created_at"] / 1000))
                                exit_dt = time.strftime("%b %d %H:%M", time.gmtime(fill["created_at"] / 1000))
                                hold_s = (fill["created_at"] - pending_entry["created_at"]) / 1000
                                journal.append({
                                    "entry_time": entry_dt,
                                    "exit_time": exit_dt,
                                    "side": "LONG",
                                    "qty": f"{qty:.4f}",
                                    "entry_price": f"{entry_px:.2f}",
                                    "exit_price": f"{exit_px:.2f}",
                                    "pnl": round(pnl, 2),
                                    "hold_seconds": int(hold_s),
                                })
                                if pnl >= 0:
                                    total_wins += 1
                                    gross_profit += pnl
                                else:
                                    total_losses += 1
                                    gross_loss += abs(pnl)
                                pending_entry = None
                        payload["trade_journal"] = list(reversed(journal))  # newest first
                        payload["win_count"] = total_wins
                        payload["loss_count"] = total_losses
                        payload["completed_trades"] = total_wins + total_losses
                        if gross_loss > 0:
                            payload["profit_factor"] = f"{gross_profit / gross_loss:.2f}"
                        elif gross_profit > 0:
                            payload["profit_factor"] = "∞"

                        # Max drawdown from journal equity curve
                        if journal:
                            initial_bal = float(payload["initial_balance"])
                            equity = initial_bal
                            peak = equity
                            max_dd = 0.0
                            for t in journal:
                                equity += t["pnl"]
                                if equity > peak:
                                    peak = equity
                                dd = (peak - equity) / peak * 100 if peak > 0 else 0
                                if dd > max_dd:
                                    max_dd = dd
                            payload["max_drawdown_pct"] = f"{max_dd:.2f}"
                    except Exception as e:
                        logger.debug(f"Failed to build trade journal: {e}")

                    # Incidents summary
                    try:
                        if "incidents" not in tables:
                            raise LookupError("Waiting for incidents")
                        cur.execute("SELECT COUNT(*) FROM incidents;")
                        row = cur.fetchone()
                        total_incidents = row[0] if row else 0
                        err_count = 0
                        warn_count = 0
                        if total_incidents > 0:
                            cur.execute("SELECT COUNT(*) FROM incidents WHERE severity='ERROR';")
                            r = cur.fetchone()
                            err_count = r[0] if r else 0
                            cur.execute("SELECT COUNT(*) FROM incidents WHERE severity='WARNING';")
                            r = cur.fetchone()
                            warn_count = r[0] if r else 0
                            cur.execute("SELECT details, timestamp FROM incidents ORDER BY incident_id DESC LIMIT 1;")
                            last = cur.fetchone()
                            last_detail = last["details"] if last else None
                            last_ago_s = int(time.time() - last["timestamp"] / 1000) if last else None
                        else:
                            last_detail = None
                            last_ago_s = None
                        payload["incidents_summary"] = {
                            "total": total_incidents,
                            "errors": err_count,
                            "warnings": warn_count,
                            "last_detail": last_detail,
                            "last_ago": last_ago_s,
                        }
                    except Exception as e:
                        logger.debug(f"Failed to load incidents: {e}")

                    # Position hold time (time since entry order for open position)
                    pos_qty = float(payload["position"]["quantity"])
                    if pos_qty > 0:
                        try:
                            cur.execute("SELECT created_at FROM orders WHERE symbol=? AND side='BUY' AND status='FILLED' ORDER BY created_at DESC LIMIT 1;", (self.config.symbol,))
                            entry_row = cur.fetchone()
                            if entry_row:
                                hold_s = int(time.time() - entry_row["created_at"] / 1000)
                                payload["position_hold_time_s"] = hold_s
                        except Exception:
                            pass

            except Exception as e:
                logger.debug(f"Failed to load account summary data: {e}")

        if payload["last_trade_time_ms"] is not None:
            age_ms = int(time.time() * 1000) - payload["last_trade_time_ms"]
            payload["feed_status"] = "Live Feed Active" if age_ms < 60_000 else "STALE FEED"

        # 2. Get candles for the requested timeframe
        # ponytail: bounded chart history; use persisted engine indicators for full-history decisions.
        candles = self.get_candles_for_timeframe(timeframe, limit=getattr(self, "chart_history", 400))
        payload["candles"] = candles

        # Fallback price
        if payload["latest_price"] is None and candles:
            payload["latest_price"] = str(candles[-1]["close"])

        # Compute unrealized PnL and position notional
        u_pnl = 0.0
        try:
            pos_qty = float(payload["position"]["quantity"])
            entry_px = float(payload["position"]["entry_price"])
            current_px = float(payload["latest_price"] or 0)
            if pos_qty > 0 and current_px > 0:
                u_pnl = (current_px - entry_px) * pos_qty
                payload["unrealized_pnl"] = f"{u_pnl:.2f}"
                payload["position_notional"] = f"{pos_qty * current_px:.2f}"
        except (ValueError, TypeError):
            pass

        # Compute total portfolio equity and true net return %
        try:
            cash = float(payload["paper_balance"])
            init = float(payload["initial_balance"])
            pos_qty = float(payload["position"]["quantity"])
            entry_px = float(payload["position"]["entry_price"])
            margin_locked = (pos_qty * entry_px) if pos_qty > 0 else 0.0
            equity = cash + margin_locked + u_pnl
            payload["total_equity"] = f"{equity:.2f}"
            if init > 0:
                payload["total_return_pct"] = f"{((equity - init) / init) * 100:.2f}"
        except (ValueError, ZeroDivisionError, TypeError):
            payload["total_equity"] = payload["paper_balance"]

        # Next candle close countdown
        tf_ms = TIMEFRAME_MAP_MS.get(self.strategy_timeframe, 900_000)
        now_ms = int(time.time() * 1000)
        payload["next_candle_close_ms"] = tf_ms - (now_ms % tf_ms)

        # 3. Calculate indicator dynamically on the selected timeframe
        if self.config.symbol.upper() == "ZECUSDT":
            ema_fast_span, ema_slow_span = 10, 100
            payload["supertrend_params"] = "(10, 100)"
            if len(candles) >= 10:
                df = pd.DataFrame(candles)
                try:
                    if self.strategy:
                        df = self.strategy.populate_indicators(df, {"pair": self.config.symbol})
                    else:
                        df["ema_fast"] = df["close"].ewm(span=ema_fast_span, adjust=False).mean()
                        df["ema_slow"] = df["close"].ewm(span=ema_slow_span, adjust=False).mean()
                    direction = df["ema_fast"].gt(df["ema_slow"]).astype(int) - df["ema_fast"].lt(df["ema_slow"]).astype(int)
                    payload["supertrend_line"] = _indicator_line(candles, df["ema_fast"], direction)
                    payload["indicator_slow_line"] = _indicator_line(candles, df["ema_slow"], [0] * len(candles))
                    _apply_indicator_state(payload, payload["supertrend_line"])
                    payload["timeframe_decision"] = payload["supertrend_direction"] + " CHART TREND"
                except Exception as calc_err:
                    logger.warning(f"EMA calculation error: {calc_err}")
        else:
            if timeframe == self.strategy_timeframe:
                st_length = self.strategy_st_length
                st_multiplier = self.strategy_st_multiplier
            elif timeframe == "15m":
                if self.config.symbol.upper() == "LITUSDT":
                    st_length, st_multiplier = 28, 2.0
                else:
                    st_length, st_multiplier = 10, 3.0
            elif timeframe == "5m":
                st_length = 10
                st_multiplier = 3.0
            else:
                st_length = 10
                st_multiplier = 3.0
            payload["supertrend_params"] = f"({st_length}, {st_multiplier:.1f})"

            if len(candles) > st_length:
                df = pd.DataFrame(candles)

                try:
                    if timeframe == self.strategy_timeframe and self.strategy:
                        calculated = self.strategy.populate_indicators(df, {"pair": self.config.symbol})
                        st_trend = calculated["supertrend"]
                        st_dir = calculated["supertrend_direction"]
                    else:
                        st_trend, st_dir = supertrend(df, length=st_length, multiplier=st_multiplier)
                    payload["supertrend_line"] = _indicator_line(candles, st_trend, st_dir)
                    _apply_indicator_state(payload, payload["supertrend_line"])
                    payload["timeframe_decision"] = payload["supertrend_direction"] + " CHART TREND"
                except Exception as calc_err:
                    logger.warning(f"Supertrend calculation error: {calc_err}")

        # 4. Generate trade markers (use order timestamp, not wall-clock)
        payload["trade_markers"] = _trade_markers(payload["orders"], TIMEFRAME_MAP_MS[timeframe])
        if not payload["account_available"]:
            for key in ("paper_balance", "initial_balance", "total_equity", "total_return_pct"):
                payload[key] = None

        return payload


@contextmanager
def read_connection(db_path: Path):
    """One WAL snapshot, no creation, and deterministic connection cleanup."""
    conn = sqlite3.connect(db_path.as_uri() + "?mode=ro", uri=True, timeout=2.0)
    try:
        conn.row_factory = sqlite3.Row
        conn.execute("BEGIN")
        yield conn
    finally:
        conn.close()


def read_chart_candles(symbol, timeframe, repo_root, db_path=None, limit=150, candles_dir=None):
    """Merge same-symbol closed bars; aggregate complete UTC buckets from finer bars."""
    tf_ms = TIMEFRAME_MAP_MS[timeframe]
    frames = {}
    source_tfs = [tf for tf in ("5m", "1m") if TIMEFRAME_MAP_MS[tf] < tf_ms and tf_ms % TIMEFRAME_MAP_MS[tf] == 0]
    for tf in [timeframe] + source_tfs:
        rows = []
        path = (Path(candles_dir) if candles_dir else repo_root / f"{symbol}_USDM_DATA/candles") / f"{symbol}_{tf}.parquet"
        if not path.exists():
            path = repo_root / "data" / f"{symbol}_{tf}.parquet"
        if path.is_file():
            try:
                frame = pd.read_parquet(path).tail(limit * (tf_ms // TIMEFRAME_MAP_MS[tf]))
                if "is_closed" in frame:
                    frame = frame[frame.is_closed == 1]
                rows.extend(frame.to_dict("records"))
            except (OSError, ValueError) as exc:
                logger.warning("Cannot read chart history: %s", exc)
        if db_path and db_path.is_file():
            try:
                with read_connection(db_path) as conn:
                    if conn.execute("SELECT 1 FROM sqlite_master WHERE name='candles' AND type='table'").fetchone():
                        has_taker_volume = any(row[1] == "taker_buy_base_volume" for row in conn.execute("PRAGMA table_info(candles)"))
                        taker_column = ", taker_buy_base_volume" if has_taker_volume else ", '0' AS taker_buy_base_volume"
                        rows.extend(dict(row) for row in conn.execute(
                            "SELECT open_time, close_time, open, high, low, close, volume" + taker_column + " FROM candles "
                            "WHERE symbol=? AND timeframe=? AND is_closed=1 ORDER BY open_time DESC LIMIT ?",
                            (symbol, tf, limit * (tf_ms // TIMEFRAME_MAP_MS[tf])),
                        ))
            except sqlite3.Error as exc:
                logger.debug("Chart database unavailable: %s", exc)
        if rows:
            frame = pd.DataFrame(rows).drop_duplicates("open_time", keep="last").sort_values("open_time")
            for column in ("open_time", "open", "high", "low", "close", "volume"):
                frame[column] = pd.to_numeric(frame[column], errors="coerce")
            if "taker_buy_base_volume" not in frame:
                frame["taker_buy_base_volume"] = 0.0
            frame["taker_buy_base_volume"] = pd.to_numeric(frame["taker_buy_base_volume"], errors="coerce").fillna(0.0)
            frame = frame.dropna(subset=["open_time", "open", "high", "low", "close", "volume"])
            frames[tf] = frame
    candles = {}
    for tf in source_tfs:
        if tf not in frames:
            continue
        source_ms = TIMEFRAME_MAP_MS[tf]
        minutes = frames[tf]
        minutes = minutes[minutes.open_time % source_ms == 0]
        grouped = minutes.groupby((minutes.open_time // tf_ms) * tf_ms)
        bars = grouped.agg(open=("open", "first"), high=("high", "max"), low=("low", "min"),
                           close=("close", "last"), volume=("volume", "sum"),
                           taker_buy_base_volume=("taker_buy_base_volume", "sum"), count=("open_time", "count"))
        for ot, row in bars[bars["count"] == tf_ms // source_ms].iterrows():
            candles[int(ot)] = row.to_dict()
    if timeframe in frames:
        for row in frames[timeframe].to_dict("records"):
            candles[int(row["open_time"])] = row
    return [dict(open_time=ot, close_time=ot + tf_ms - 1,
                 **{key: float(candles[ot][key]) for key in ("open", "high", "low", "close", "volume", "taker_buy_base_volume")})
            for ot in sorted(candles)[-limit:]]


def load_chart_candles_and_supertrend(symbol, repo_root, db_path=None, limit=150, timeframe=None):
    """Compatibility chart payload using the same isolated reader as coin pages."""
    tf = timeframe or "15m"
    if db_path is not None:
        db_path = validate_database_path(db_path, repo_root)
    candles = read_chart_candles(symbol, tf, repo_root, db_path, limit)
    if symbol == "ZECUSDT":
        length = 10
        result = dict(chart_candles=[dict(time=c["open_time"] // 1000,
                                        **{k: c[k] for k in ("open", "high", "low", "close", "volume")}) for c in candles],
                      supertrend_line=[], indicator_slow_line=[], supertrend_direction="NEUTRAL", supertrend_val=None,
                      supertrend_params="(10, 100)", indicator_name="Momentum EMA", timeframe=tf)
        if len(candles) >= length:
            df = pd.DataFrame(candles)
            from live_engine.strategy.loader import StrategyLoader
            strategy, _ = StrategyLoader.load_strategy(
                Path(repo_root) / "benchmarks/manifests/ZEC_MOMENTUM_M03_15M.yaml", repo_root
            )
            df = strategy.populate_indicators(df, {"pair": symbol})
            direction = df["ema_fast"].gt(df["ema_slow"]).astype(int) - df["ema_fast"].lt(df["ema_slow"]).astype(int)
            result["supertrend_line"] = _indicator_line(candles, df["ema_fast"], direction, 1000)
            result["indicator_slow_line"] = _indicator_line(candles, df["ema_slow"], [0] * len(candles), 1000)
            _apply_indicator_state(result, result["supertrend_line"])
        return result

    length, multiplier = {"BTCUSDT": (10, 3.0), "LITUSDT": (28, 2.0)}[symbol]
    result = dict(chart_candles=[dict(time=c["open_time"] // 1000,
                                    **{k: c[k] for k in ("open", "high", "low", "close", "volume")}) for c in candles],
                  supertrend_line=[], indicator_slow_line=[], supertrend_direction="NEUTRAL", supertrend_val=None,
                  supertrend_params=f"({length}, {multiplier:.1f})", indicator_name="Supertrend", timeframe=tf)
    if len(candles) > length:
        frame = pd.DataFrame(candles)
        strategy_timeframe = {"BTCUSDT": "5m", "LITUSDT": "15m"}[symbol]
        if tf == strategy_timeframe:
            from live_engine.strategy.loader import StrategyLoader
            strategy, _ = StrategyLoader.load_strategy(
                Path(repo_root) / f"benchmarks/manifests/{'BTC_ST_09_5M' if symbol == 'BTCUSDT' else 'LIT_SUPERTREND_15M'}.yaml", repo_root
            )
            calculated = strategy.populate_indicators(frame, {"pair": symbol})
            trend, direction = calculated["supertrend"], calculated["supertrend_direction"]
        else:
            trend, direction = supertrend(frame, length=length, multiplier=multiplier)
        result["supertrend_line"] = _indicator_line(candles, trend, direction, 1000)
        _apply_indicator_state(result, result["supertrend_line"])
    return result


def read_single_account_snapshot(config: LiveEngineConfig, base_dir: Optional[Path | str] = None) -> Dict[str, Any]:
    """Reads a snapshot for a single symbol from its isolated SQLite database.

    Guarantees:
    - Never creates a missing database.
    - Connects in read-only mode (mode=ro).
    - Uses short-lived connections with timeout.
    - Handles missing tables, locked databases, empty databases, and malformed JSON safely.
    - Derives no trading decisions.
    - All SQL uses parameterized queries.
    """
    repo_root = Path(base_dir or Path.cwd()).resolve()

    snapshot: Dict[str, Any] = {
        "symbol": config.symbol,
        "mode": config.mode.upper(),
        "status": "HEALTHY",
        "database": str(config.event_store_path).replace("\\", "/"),
        "database_available": False,
        "latest_price": None,
        "last_event_time_utc": None,
        "freshness": "NO DATA",
        "staleness_seconds": None,
        "warmup_state": "WARMUP INCOMPLETE (0/250)",
        "candle_count_15m": 0,
        "candle_count_1m": 0,
        "trades_count": 0,
        "balance_usdt": "10000.00",
        "paper_equity_usdt": "10000.00",
        "position": {
            "side": "FLAT",
            "quantity": "0.000",
            "entry_price": "0.00",
            "unrealized_pnl": "0.00",
            "realized_pnl": "0.00",
        },
        "latest_signal": None,
        "open_orders_count": 0,
        "benchmark_id": "UNKNOWN",
        "filters": {"tick_size": "--", "step_size": "--"},
        "recent_signals": [],
        "recent_orders": [],
        "recent_fills": [],
        "recent_candles": [],
        "recent_incidents": [],
        "chart_candles": [],
        "supertrend_line": [],
        "indicator_slow_line": [],
        "supertrend_direction": "NEUTRAL",
        "supertrend_val": None,
        "supertrend_params": "(10, 100)" if config.symbol.upper() == "ZECUSDT" else ("(28, 2.0)" if config.symbol.upper() == "LITUSDT" else "(10, 3.0)"),
        "indicator_name": "Momentum EMA" if config.symbol.upper() == "ZECUSDT" else "Supertrend",
        "trade_markers": [],
    }

    # Extract benchmark_id from manifest
    try:
        manifest_p = (repo_root / config.manifest_path).resolve()
        if manifest_p.exists():
            import yaml
            m_data = yaml.safe_load(manifest_p.read_text(encoding="utf-8")) or {}
            snapshot["benchmark_id"] = m_data.get("benchmark_id", "UNKNOWN")
    except Exception:
        pass

    # Static filter defaults by symbol
    KNOWN_FILTERS = {
        "BTCUSDT": {"tick_size": "0.10", "step_size": "0.001"},
        "LITUSDT": {"tick_size": "0.0001", "step_size": "0.1"},
        "ZECUSDT": {"tick_size": "0.01", "step_size": "0.001"},
    }
    if config.symbol in KNOWN_FILTERS:
        snapshot["filters"] = KNOWN_FILTERS[config.symbol]

    # Validate database path (must be contained in data/)
    try:
        db_path = validate_database_path(config.event_store_path, base_dir=repo_root)
    except Exception as e:
        logger.warning(f"Database path invalid for {config.symbol}: {e}")
        snapshot["status"] = "DATABASE UNAVAILABLE"
        snapshot["database_available"] = False
        return snapshot

    # Check if database file exists. NEVER create a missing database!
    if not db_path.exists():
        snapshot["status"] = "DATABASE UNAVAILABLE"
        snapshot["database_available"] = False
        snapshot.update(load_chart_candles_and_supertrend(
            symbol=config.symbol,
            repo_root=repo_root,
            limit=150,
            timeframe=getattr(config, "timeframe", "15m"),
        ))
        return snapshot

    snapshot["database_available"] = True
    conn = None
    try:
        # SQLite read-only connection
        db_uri = db_path.as_uri() + "?mode=ro"
        conn = sqlite3.connect(db_uri, uri=True, timeout=2.0)
        conn.row_factory = sqlite3.Row
        conn.execute("BEGIN")
        cur = conn.cursor()

        # Check existing tables to handle empty or partial databases safely
        cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = {row[0] for row in cur.fetchall()}

        # 1. Raw aggtrades (trades count, latest price, last event time)
        if "raw_aggtrades" in tables:
            try:
                cur.execute("SELECT COUNT(*) FROM raw_aggtrades WHERE symbol = ?;", (config.symbol,))
                r_cnt = cur.fetchone()
                if r_cnt and r_cnt[0] is not None:
                    snapshot["trades_count"] = int(r_cnt[0])

                cur.execute(
                    "SELECT price, received_at, trade_time FROM raw_aggtrades WHERE symbol = ? ORDER BY trade_time DESC, agg_trade_id DESC LIMIT 1;",
                    (config.symbol,),
                )
                latest_trade = cur.fetchone()
                if latest_trade:
                    snapshot["latest_price"] = str(latest_trade["price"])
                    t_time = latest_trade["received_at"] or latest_trade["trade_time"]
                    if t_time:
                        snapshot["last_event_time_utc"] = time.strftime(
                            "%Y-%m-%d %H:%M:%S UTC", time.gmtime(t_time / 1000)
                        )
                        age_s = max(0, int(time.time() - t_time / 1000))
                        snapshot["staleness_seconds"] = age_s
                        snapshot["freshness"] = "FRESH" if age_s < 60 else ("WARM" if age_s < 300 else "STALE")
            except Exception as e:
                logger.warning(f"Error querying raw_aggtrades for {config.symbol}: {e}")

        # 2. Candles
        if "candles" in tables:
            try:
                strat_tf = getattr(config, "timeframe", "15m").lower()
                cur.execute("SELECT timeframe, COUNT(*) FROM candles WHERE symbol = ? GROUP BY timeframe;", (config.symbol,))
                for tf_name, cnt in cur.fetchall():
                    if tf_name == "1m":
                        snapshot["candle_count_1m"] = int(cnt)
                    if tf_name == strat_tf:
                        snapshot["candle_count_15m"] = int(cnt)
                    elif tf_name == "15m" and snapshot["candle_count_15m"] == 0:
                        snapshot["candle_count_15m"] = int(cnt)

                c15_cnt = snapshot["candle_count_15m"]
                if c15_cnt >= config.warmup_candles:
                    snapshot["warmup_state"] = f"READY ({c15_cnt}/{config.warmup_candles})"
                else:
                    snapshot["warmup_state"] = f"WARMING UP ({c15_cnt}/{config.warmup_candles})"

                # Recent candles for strategy timeframe
                cur.execute(
                    "SELECT open_time, close_time, open, high, low, close, volume, trade_count FROM candles WHERE symbol = ? AND timeframe = ? ORDER BY open_time DESC LIMIT 20;",
                    (config.symbol, strat_tf),
                )
                candles_list = []
                for cr in cur.fetchall():
                    dt_str = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(cr["open_time"] / 1000))
                    candles_list.append({
                        "open_time": cr["open_time"],
                        "time_utc": dt_str + " UTC",
                        "open": str(cr["open"]),
                        "high": str(cr["high"]),
                        "low": str(cr["low"]),
                        "close": str(cr["close"]),
                        "volume": str(cr["volume"]),
                        "trades": int(cr["trade_count"]),
                    })
                snapshot["recent_candles"] = candles_list

                if snapshot["latest_price"] is None and candles_list:
                    snapshot["latest_price"] = candles_list[0]["close"]
            except Exception as e:
                logger.warning(f"Error querying candles for {config.symbol}: {e}")

        # 3. Position and Paper Balance
        pos_qty = Decimal("0")
        entry_price = Decimal("0")
        realized_pnl = Decimal("0")
        pos_side = "FLAT"
        balance = Decimal("10000.00")

        if "audit_events" in tables:
            try:
                cur.execute(
                    "SELECT payload_json FROM audit_events WHERE event_type = 'PAPER_ACCOUNT_SNAPSHOT' ORDER BY event_id DESC LIMIT 1;"
                )
                row = cur.fetchone()
                if row and row[0]:
                    snap_data = json.loads(row[0])
                    if isinstance(snap_data, dict):
                        balance = Decimal(str(snap_data.get("balance", "10000.00")))
                        positions = snap_data.get("positions", {})
                        if isinstance(positions, dict) and config.symbol in positions:
                            p_info = positions[config.symbol]
                            pos_qty = Decimal(str(p_info.get("quantity", "0")))
                            entry_price = Decimal(str(p_info.get("entry_price", "0")))
                            realized_pnl = Decimal(str(p_info.get("realized_pnl", "0")))
                            side_val = p_info.get("side")
                            pos_side = str(side_val) if side_val and pos_qty > 0 else "FLAT"
            except Exception as e:
                logger.warning(f"Error reading paper snapshot for {config.symbol}: {e}")

            if pos_qty == 0:
                try:
                    cur.execute(
                        "SELECT payload_json FROM audit_events WHERE event_type = 'POSITION_UPDATED' ORDER BY event_id DESC LIMIT 1;"
                    )
                    row = cur.fetchone()
                    if row and row[0]:
                        p_info = json.loads(row[0])
                        if isinstance(p_info, dict) and p_info.get("symbol") in (config.symbol, None):
                            realized_pnl = Decimal(str(p_info.get("realized_pnl", "0")))
                            q = Decimal(str(p_info.get("quantity", "0")))
                            if q > 0:
                                pos_qty = q
                                entry_price = Decimal(str(p_info.get("entry_price", "0")))
                                pos_side = "LONG"
                except Exception:
                    pass

        # Calculate unrealized PnL and paper equity
        unrealized_pnl = Decimal("0.00")
        if pos_qty > 0 and snapshot["latest_price"]:
            try:
                cur_px = Decimal(str(snapshot["latest_price"]))
                if pos_side in ("BUY", "LONG"):
                    unrealized_pnl = (cur_px - entry_price) * pos_qty
                elif pos_side in ("SELL", "SHORT"):
                    unrealized_pnl = (entry_price - cur_px) * pos_qty
            except Exception:
                pass

        paper_equity = balance + unrealized_pnl
        snapshot["balance_usdt"] = f"{balance:.2f}"
        snapshot["paper_equity_usdt"] = f"{paper_equity:.2f}"
        snapshot["position"] = {
            "side": pos_side if pos_qty > 0 else "FLAT",
            "quantity": f"{pos_qty:.4f}",
            "entry_price": f"{entry_price:.2f}",
            "unrealized_pnl": f"{unrealized_pnl:+.2f}",
            "realized_pnl": f"{realized_pnl:+.2f}",
        }

        # 4. Signals
        if "signals" in tables:
            try:
                cur.execute(
                    "SELECT signal_id, action, reference_price, candle_open_time, generated_at, reason FROM signals WHERE symbol = ? ORDER BY generated_at DESC LIMIT 20;",
                    (config.symbol,),
                )
                signals_list = []
                for sr in cur.fetchall():
                    dt_str = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(sr["candle_open_time"] / 1000))
                    signals_list.append({
                        "signal_id": sr["signal_id"],
                        "action": sr["action"],
                        "price": str(sr["reference_price"]),
                        "timestamp_utc": dt_str + " UTC",
                        "reason": sr["reason"],
                    })
                snapshot["recent_signals"] = signals_list
                if signals_list:
                    snapshot["latest_signal"] = signals_list[0]
            except Exception as e:
                logger.warning(f"Error querying signals for {config.symbol}: {e}")

        # 5. Orders & Fills
        if "orders" in tables:
            try:
                cur.execute(
                    "SELECT client_order_id, side, order_type, quantity, price, status, avg_fill_price, accumulated_fees, created_at, filled_at FROM orders WHERE symbol = ? ORDER BY created_at DESC LIMIT 25;",
                    (config.symbol,),
                )
                orders_list = []
                fills_list = []
                open_cnt = 0
                for o_row in cur.fetchall():
                    created_dt = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(o_row["created_at"] / 1000))
                    st = o_row["status"]
                    if st in ("CREATED", "SUBMITTING", "NEW", "PARTIALLY_FILLED", "UNKNOWN"):
                        open_cnt += 1
                    order_dict = {
                        "client_order_id": o_row["client_order_id"],
                        "side": o_row["side"],
                        "order_type": o_row["order_type"],
                        "quantity": str(o_row["quantity"]),
                        "price": str(o_row["price"] or "--"),
                        "status": st,
                        "avg_fill_price": str(o_row["avg_fill_price"] or "--"),
                        "fees": str(o_row["accumulated_fees"] or "0.00"),
                        "created_at_utc": created_dt + " UTC",
                        "created_at_ms": int(o_row["created_at"]),
                    }
                    orders_list.append(order_dict)
                    if st == "FILLED":
                        f_time = o_row["filled_at"] or o_row["created_at"]
                        filled_dt = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(f_time / 1000))
                        fills_list.append({
                            **order_dict,
                            "filled_at_utc": filled_dt + " UTC",
                        })
                snapshot["recent_orders"] = orders_list
                snapshot["recent_fills"] = fills_list
                snapshot["open_orders_count"] = open_cnt
            except Exception as e:
                logger.warning(f"Error querying orders for {config.symbol}: {e}")

        # 6. Audit & Incidents
        if "incidents" in tables:
            try:
                cur.execute("SELECT incident_id, timestamp, category, severity, details FROM incidents ORDER BY incident_id DESC LIMIT 10;")
                inc_list = []
                for ir in cur.fetchall():
                    dt_str = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(ir["timestamp"] / 1000))
                    inc_list.append({
                        "id": ir["incident_id"],
                        "timestamp_utc": dt_str + " UTC",
                        "category": ir["category"],
                        "severity": ir["severity"],
                        "details": ir["details"],
                    })
                snapshot["recent_incidents"] = inc_list
            except Exception as e:
                logger.warning(f"Error querying incidents for {config.symbol}: {e}")

        # Update overall health status
        if snapshot["trades_count"] == 0 and snapshot["candle_count_15m"] == 0:
            snapshot["status"] = "WAITING FOR DATA"
        elif snapshot["freshness"] == "STALE":
            snapshot["status"] = "STALE"
        else:
            snapshot["status"] = "HEALTHY"

    except sqlite3.OperationalError as oe:
        logger.warning(f"Database operational error for {config.symbol}: {oe}")
        snapshot["status"] = "DATABASE LOCKED/BUSY"
    except Exception as e:
        logger.error(f"Error reading snapshot for {config.symbol}: {e}")
        snapshot["status"] = "ERROR"
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass

    # Load chart candles and Supertrend for interactive TradingView chart
    try:
        chart_data = load_chart_candles_and_supertrend(
            symbol=config.symbol,
            repo_root=repo_root,
            db_path=db_path if snapshot["database_available"] else None,
            limit=150,
            timeframe=getattr(config, "timeframe", "15m"),
        )
        snapshot.update(chart_data)

        # Build trade markers from filled orders
        snapshot["trade_markers"] = _trade_markers(
            snapshot.get("recent_orders", []),
            TIMEFRAME_MAP_MS.get(getattr(config, "timeframe", "15m"), 900_000),
        )
    except Exception as chart_err:
        logger.warning(f"Error loading chart data for {config.symbol}: {chart_err}")

    return snapshot


SYMBOL_NAVIGATION_HTML = """<style>
  .symbol-navigation { display: flex; flex-wrap: wrap; gap: .75rem; font-size: .875rem; font-weight: 600; }
  .symbol-navigation a { padding: .5rem .75rem; border-radius: .375rem; background: #1F2937; color: #E2E8F0; }
  .symbol-navigation a[aria-current] { background: #F59E0B; color: #0B0F19; }
  .symbol-navigation a:hover { text-decoration: underline; }
  .symbol-navigation a:focus-visible { outline: 2px solid #F59E0B; outline-offset: 3px; }
  html[data-theme="light"] .symbol-navigation a { background: #F1E7D7; color: #37453A; }
  html[data-theme="light"] .symbol-navigation a[aria-current] { background: #92400E; color: #FFFFFF; }
  .symbol-nav-row { position: relative; display: flex; flex-wrap: wrap; align-items: center; gap: .75rem; }
  .strategy-eye > summary { list-style: none; display: grid; place-items: center; width: 2.25rem; height: 2.25rem;
    border-radius: 9999px; background: #1F2937; color: #94A3B8; cursor: pointer; user-select: none; }
  .strategy-eye > summary::-webkit-details-marker { display: none; }
  .strategy-eye > summary:hover { color: #F8FAFC; }
  .strategy-eye > summary:focus-visible { outline: 2px solid #F59E0B; outline-offset: 3px; }
  .strategy-eye[open] > summary { background: #F59E0B; color: #0B0F19; }
  .strategy-panel { position: absolute; z-index: 50; top: calc(100% + .5rem); left: 0; right: 0;
    padding: 1rem; border-radius: .75rem; background: #111827; border: 1px solid #1F2937;
    box-shadow: 0 10px 30px rgba(0, 0, 0, .45); }
  html[data-theme="light"] .strategy-eye > summary { background: #F1E7D7; color: #37453A; }
  html[data-theme="light"] .strategy-panel { background: #FFFFFF; border-color: #E5E7EB; }
</style>
<nav aria-label="Dashboards" class="symbol-navigation">
  <a href="/" aria-current="page">Overview</a>
  <a href="/BTCUSDT">BTCUSDT</a>
  <a href="/LITUSDT">LITUSDT</a>
  <a href="/ZECUSDT">ZECUSDT</a>
  <a href="/HYPEUSDT">HYPEUSDT</a>
</nav>"""


MODE_LABELS = {
    "PAPER": ("PAPER MODE", "SIMULATED PAPER ACCOUNTS — NO REAL ORDERS", "#F59E0B"),
    "TESTNET": ("TESTNET MODE", "BINANCE TESTNET — REAL ORDERS, TEST CAPITAL", "#38BDF8"),
    "SHADOW": ("SHADOW MODE · ZERO RISK", "SHADOW REPLAY / AUDIT OBSERVABILITY — ZERO REAL ORDERS", "#A78BFA"),
    "LIVE": ("LIVE PRODUCTION", "LIVE PRODUCTION — REAL CAPITAL AT RISK", "#10B981"),
}


def render_symbol_dashboard(config: LiveEngineConfig, cards: str = "", inline: str = "", show_nav: bool = True) -> str:
    context = json.dumps({"symbol": config.symbol, "timeframe": config.timeframe,
                          "mode": config.mode, "readOnly": True}).replace("<", "\\u003c")
    badge, disclaimer, color = MODE_LABELS[config.mode]
    # Strategy detail drops out of an eye toggle sitting beside the coin tabs; anything inline stays always-visible.
    eye = ('<details class="strategy-eye">'
           '<summary title="Strategy details" aria-label="Strategy details">&#128065;</summary>'
           '<div class="strategy-panel">'
           '<div id="configured-strategy" class="text-sm text-amber-400 font-mono" role="status"></div>'
           f'<div class="space-y-4 pt-3">{cards}</div></div></details>')

    if show_nav:
        navigation = SYMBOL_NAVIGATION_HTML.replace('  <a href="/" aria-current="page">Overview</a>\n', '')
        for symbol in ("BTCUSDT", "LITUSDT", "ZECUSDT", "HYPEUSDT"):
            # A coin tab never inherits an authenticated mode from another coin's dashboard:
            # LIT and ZEC reject TESTNET/LIVE, so such a link would only ever raise.
            mode = ("PAPER" if config.mode in ("TESTNET", "LIVE")
                    and symbol not in ("BTCUSDT", "HYPEUSDT") else config.mode)
            active = ' aria-current="page"' if symbol == config.symbol else ''
            navigation = navigation.replace(f'href="/{symbol}"', f'href="/{symbol}?mode={mode.lower()}"{active}')
            navigation = navigation.replace(f'>{symbol}</a>', f'>{symbol[:3]} Dashboard</a>')
        navigation = navigation.replace('href="/"', f'href="/?mode={config.mode.lower()}"')
        navigation = (navigation.replace("<nav ", '<div class="symbol-nav-row"><nav ')
                                .replace("</nav>", "</nav>" + eye + "</div>"))
    else:
        # Isolated single-coin mode: hide other coin tabs completely
        navigation = f'<div class="symbol-nav-row">{eye}</div>'

    banner = f'<p style="color:{color}" class="text-xs font-semibold">{disclaimer} · READ-ONLY</p>'
    cards = inline
    style = f'<style>html #mode-badge, html[data-theme="light"] #mode-badge {{background:{color}22!important;border-color:{color}!important;color:{color}!important}} html #mode-badge span, html[data-theme="light"] #mode-badge span {{color:{color}!important}} #mode-badge .pulse-dot {{background:{color}!important}} #strategy-eye::-webkit-details-marker {{display:none}}</style>'
    return (DASHBOARD_HTML
            .replace("const dashboardConfig = {};", f"const dashboardConfig = {context};")
            .replace('id="btn-kill"', 'id="btn-kill" disabled')
            .replace("EMERGENCY HALT", "HALT: CLI ONLY")
            .replace('>SHADOW MODE</span>', f'>{badge}</span>')
            .replace('>BTCUSDT</span>', f'>{config.symbol}</span>')
            .replace('>LIVE ENGINE</span>', f'>{config.symbol[:3]} DASHBOARD</span>')
            .replace("<!-- SYMBOL_NAVIGATION -->", navigation + banner + cards)
            .replace('</head>', style + '</head>')
            .replace("<title>Escanor Live Trading Terminal</title>", f"<title>{escape(config.symbol)} | Escanor Dashboard</title>"))


def metric_cards(items):
    return '<section class="grid grid-cols-1 sm:grid-cols-3 gap-4">' + ''.join(
        f'<div class="bg-dark-800 border border-dark-700 rounded-xl p-4"><div class="text-xs text-gray-400">{label}</div><div class="text-lg font-mono" data-metric="{key}">--</div></div>'
        for key, label in items) + '</section>'


class BaseDashboard(DashboardDataAggregator):
    """Validated coin configuration over the shared read-only terminal."""
    allowed_modes = ("PAPER", "SHADOW")

    def __init__(self, config=None, *, mode=None, base_dir=None, show_nav: bool = True):
        root = Path(base_dir or Path.cwd()).resolve()
        if "ESCANOR_EVENT_STORE" in os.environ:
            raise ValueError("DATABASE ISOLATION VIOLATION: ESCANOR_EVENT_STORE is not permitted")
        if config is None:
            chosen_mode = (mode or "SHADOW").upper()
            if chosen_mode not in self.allowed_modes:
                raise ValueError(f"MODE NOT ALLOWED: {chosen_mode} for {self.symbol}")
            prefix = getattr(self, "config_prefix", self.symbol[:3].lower())
            config_path = root / f"config/{prefix}-{chosen_mode.lower()}.json"
            config = load_config(str(config_path)) if config_path.is_file() else LiveEngineConfig(
                symbol=self.symbol, timeframe=self.timeframe, mode=chosen_mode,
                manifest_path=f"benchmarks/manifests/{self.benchmark_id}.yaml",
                event_store_path=f"data/{prefix}_{chosen_mode.lower()}.db")
            if config.mode.upper() != chosen_mode:
                raise ValueError("Requested mode does not match configuration")
        elif isinstance(config, (str, Path)):
            config = load_config(str(root / config))
        config = replace(config, symbol=config.symbol.upper(), mode=config.mode.upper(), timeframe=config.timeframe.lower())
        if mode and config.mode != mode.upper():
            raise ValueError("Requested mode does not match configuration")
        if config.symbol != self.symbol or config.timeframe != self.timeframe:
            raise ValueError(f"{self.symbol} dashboard requires {self.timeframe} configuration")
        if config.mode not in self.allowed_modes:
            raise ValueError(f"MODE NOT ALLOWED: {config.mode} for {self.symbol}")
        from live_engine.strategy.loader import StrategyLoader
        source_root = Path(__file__).resolve().parents[2]
        manifest_path = root / config.manifest_path
        strategy_root = root
        if not manifest_path.exists() and base_dir is not None:
            manifest_path = source_root / config.manifest_path
            strategy_root = source_root
        strategy, manifest = StrategyLoader.load_strategy(manifest_path, strategy_root)
        validate_config_against_manifest(config, manifest)
        if manifest["benchmark_id"] != self.benchmark_id or type(strategy).__name__ != self.strategy_class:
            raise ValueError(f"Dashboard requires {self.benchmark_id} / {self.strategy_class}")
        super().__init__(config, base_dir=root, read_only=True)
        self.show_nav = show_nav
        self.strategy = strategy
        self.strategy_name = self.strategy_class
        self.strategy_timeframe = self.timeframe
        self.strategy_st_length, self.strategy_st_multiplier = self.supertrend_defaults
        self.atr_period = getattr(strategy, "atr_period", 10)

    def render(self):
        return render_symbol_dashboard(self.config, self.cards(), self.inline_cards(), show_nav=self.show_nav)

    def cards(self):
        """Strategy detail, reachable through the eye toggle."""
        return ''

    def inline_cards(self):
        """Evidence that must stay on screen without a click."""
        return ''

    def get_dashboard_payload(self, requested_timeframe=None):
        payload = super().get_dashboard_payload(requested_timeframe)
        payload["benchmark_id"] = self.benchmark_id
        payload["mode_badge"], payload["disclaimer"], _ = MODE_LABELS[self.config.mode]
        payload["strategy_metrics"] = {}
        if self.is_pullback_strategy and payload["timeframe"] == self.timeframe and len(payload["candles"]) > max(self.atr_period, self.strategy_st_length):
            frame = self.strategy.populate_indicators(pd.DataFrame(payload["candles"]), {"pair": self.symbol})
            latest = frame.iloc[-1]
            atr = latest.get("atr", latest.get(f"atr_{self.atr_period}"))
            if pd.notna(atr) and atr > 0:
                close = float(latest["close"])
                metrics = dict(atr=float(atr), pullback_distance=float((close - latest.supertrend) / atr),
                               pullback_limit=self.strategy.pb_atr,
                               stop_loss_preview=close - self.strategy.sl_multiplier * float(atr),
                               take_profit_preview=close + self.strategy.tp_multiplier * float(atr))
                payload["strategy_metrics"] = metrics
                payload["timeframe_decision"] = "PULLBACK SETUP" if bool(latest.entry_candidate) else "WAIT PULLBACK"
        return payload

    def start_server(self, host="127.0.0.1", port=8080):
        handler = type("CoinHTTPHandler", (BaseHTTPHandler,), {"dashboard": self})
        server = ThreadingHTTPServer((host, port), handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        return server


def dashboard_class(symbol):
    from . import BTCDashboard, HYPEDashboard, LITDashboard, ZECDashboard
    return {"BTCUSDT": BTCDashboard, "HYPEUSDT": HYPEDashboard,
            "LITUSDT": LITDashboard, "ZECUSDT": ZECDashboard}[symbol]


class BaseHTTPHandler(BaseHTTPRequestHandler):
    """Shared CORS/no-cache HTTP boundary; no state-changing methods."""
    dashboard: BaseDashboard

    def log_message(self, format, *args):
        pass

    def handle(self):
        try:
            super().handle()
        except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError):
            pass

    def end_headers(self):
        self.send_header("Cache-Control", "no-store")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        self.send_header("Access-Control-Allow-Origin", "*")
        super().end_headers()

    def respond(self, content, status=200):
        html = isinstance(content, str)
        body = (content if html else json.dumps(content, allow_nan=False)).encode("utf-8")
        self.send_response(status)
        if status == 405:
            self.send_header("Allow", "GET")
        self.send_header("Content-Type", "text/html; charset=utf-8" if html else "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/ping":
            return self.respond({"status": "ok"})
        parts = parsed.path.strip("/").split("/")
        query = parse_qs(parsed.query)
        symbol = parts[0].upper()
        symbol = {"BTC": "BTCUSDT", "HYPE": "HYPEUSDT", "LIT": "LITUSDT", "ZEC": "ZECUSDT"}.get(symbol, symbol)
        dashboard = self.dashboard
        if parsed.path in ("/", "/index.html", "/api/data"):
            symbol = dashboard.symbol
            api = parsed.path == "/api/data"
        elif symbol in ("BTCUSDT", "HYPEUSDT", "LITUSDT", "ZECUSDT") and (len(parts) == 1 or parts[1:] == ["api", "data"]):
            api = len(parts) > 1
        else:
            return self.respond({"error": "Not found"}, 404)
        mode = query.get("mode", [dashboard.config.mode])[0].upper()
        try:
            if symbol != dashboard.symbol or mode != dashboard.config.mode:
                cache = getattr(self.dashboard, "_peer_dashboards", None)
                if cache is None:
                    cache = self.dashboard._peer_dashboards = {}
                key = (symbol, mode)
                if key not in cache:
                    cache[key] = dashboard_class(symbol)(mode=mode, base_dir=dashboard.base_dir, show_nav=self.dashboard.show_nav)
                dashboard = cache[key]
            self.respond(dashboard.get_dashboard_payload(query.get("timeframe", [None])[0]) if api else dashboard.render())
        except (ValueError, FileNotFoundError) as exc:
            self.respond({"error": str(exc)}, 400)

    def do_POST(self):
        self.respond({"error": "Method Not Allowed - Dashboard is strictly an observability tool."}, 405)

    do_PUT = do_PATCH = do_DELETE = do_POST


def run_dashboard_cli(cls):
    import argparse
    parser = argparse.ArgumentParser(description=f"Read-only {cls.symbol} dashboard")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--config")
    group.add_argument("--mode", choices=cls.allowed_modes)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--show-nav", action="store_true", default=False,
                        help="Show multi-coin navigation tabs (hidden by default in isolated mode)")
    args = parser.parse_args()
    server = cls(args.config, mode=args.mode, show_nav=args.show_nav).start_server(args.host, args.port)
    print(f"{cls.symbol} dashboard: http://{args.host}:{server.server_port}", flush=True)
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        server.shutdown()
        server.server_close()
