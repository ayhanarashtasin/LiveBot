"""Compatibility portfolio overview over isolated accounts."""
from __future__ import annotations
import json
import logging
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional
from http.server import ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse
from live_engine.config import LiveEngineConfig, load_config, validate_unique_databases, validate_config_against_manifest
from .base import (BaseHTTPHandler, dashboard_class, read_single_account_snapshot, SYMBOL_NAVIGATION_HTML)
logger = logging.getLogger("escanor.dashboard")

MULTI_SYMBOL_DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en" class="dark" data-theme="dark">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Escanor Multi-Symbol Paper Terminal (BTC, LIT, ZEC)</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <script src="https://cdn.jsdelivr.net/npm/lightweight-charts@4.1.1/dist/lightweight-charts.standalone.production.js"></script>
  <script>
    tailwind.config = {
      darkMode: 'class',
      theme: {
        extend: {
          colors: {
            brand: { 500: '#F59E0B', 600: '#D97706' },
            dark: { 950: '#070A10', 900: '#0B0F19', 800: '#111827', 700: '#1F2937', 600: '#374151' }
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
    html[data-theme="light"] body { background-color: #F8EDE3; color: #2F3A2E; }
    html[data-theme="light"] .bg-dark-800 { background-color: #FFFCF5 !important; }
    html[data-theme="light"] div[class*="bg-dark-900"] { background-color: #BDD2B644 !important; }
    html[data-theme="light"] .bg-dark-900 { background-color: #F1E7D7 !important; }
    html[data-theme="light"] .border-dark-700 { border-color: #A2B29F !important; }
    html[data-theme="light"] .text-white { color: #2F3A2E !important; }
    html[data-theme="light"] .text-gray-300 { color: #37453A !important; }
    html[data-theme="light"] .text-gray-400 { color: #46543F !important; }
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
    html[data-theme="light"] #nav-sim-notice {
      background-color: #FEF3C7 !important;
      color: #92400E !important;
      border: 1px solid #FCD34D !important;
    }
    html[data-theme="light"] #nav-sim-notice span {
      color: #92400E !important;
    }
    html[data-theme="light"] #nav-sim-notice .pulse-dot {
      background-color: #D97706 !important;
    }
    html[data-theme="light"] #nav-engines-badge {
      background-color: #ECFDF5 !important;
      color: #065F46 !important;
      border: 1px solid #6EE7B7 !important;
      font-weight: 700 !important;
    }
    html[data-theme="light"] #nav-engines-badge span {
      color: #065F46 !important;
    }
    html[data-theme="light"] #nav-engines-badge .pulse-dot {
      background-color: #10B981 !important;
    }
    html[data-theme="light"] #nav-gate-badge {
      background-color: #F8FAFC !important;
      color: #475569 !important;
      border: 1px solid #CBD5E1 !important;
    }
    html[data-theme="light"] #nav-gate-badge span {
      color: #475569 !important;
    }
    html[data-theme="light"] #nav-gate-badge span span {
      color: #B45309 !important;
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
        <span class="text-xl font-bold tracking-tight text-white">ESCANOR <span id="nav-live-badge" class="text-amber-400 font-normal text-sm ml-1 px-1.5 py-0.5 rounded bg-amber-400/10 border border-amber-400/20">MULTI-SYMBOL PAPER TERMINAL</span></span>
      </div>
      <div class="h-5 w-px bg-dark-700"></div>
      <!-- Prominent Notice: SIMULATED PAPER ACCOUNTS — NO REAL ORDERS -->
      <div id="nav-sim-notice" class="px-3 py-1 rounded-full bg-amber-500/20 text-amber-300 border border-amber-500/40 text-xs font-bold uppercase tracking-wider flex items-center gap-2">
        <span class="w-2 h-2 rounded-full bg-amber-400 pulse-dot"></span>
        <span>SIMULATED PAPER ACCOUNTS — NO REAL ORDERS · READ-ONLY</span>
      </div>
    </div>

    <div class="flex items-center space-x-3">
      <div id="nav-engines-badge" class="flex items-center space-x-1.5 px-3 py-1 rounded-full text-xs font-semibold bg-emerald-500/10 text-emerald-400 border border-emerald-500/20">
        <span class="w-2 h-2 rounded-full bg-emerald-400 pulse-dot"></span>
        <span>3 ISOLATED ENGINES</span>
      </div>
      <div id="nav-gate-badge" class="flex items-center space-x-1.5 px-3 py-1 rounded-full text-xs font-mono bg-dark-900/60 text-gray-400 border border-dark-700" title="Shared emergency kill switch file">
        <span>Gate: <span class="text-amber-400 font-semibold">.kill_switch</span></span>
      </div>
      <button onclick="toggleTheme()" id="btn-theme" title="Toggle dark / light mode" class="px-3 py-1 text-xs font-semibold rounded bg-dark-700 text-gray-300 border border-dark-700 transition">
        ☀️ LIGHT
      </button>
      <div class="text-xs text-gray-500 font-mono" id="clock-utc">--:--:-- UTC</div>
    </div>
  </header>

  <!-- MAIN BODY -->
  <main class="flex-1 p-6 space-y-8 max-w-[1750px] w-full mx-auto">
    <!-- SYMBOL_NAVIGATION -->

    <!-- OVERVIEW CARDS (3 SYMBOLS: BTCUSDT, LITUSDT, ZECUSDT) -->
    <section>
      <div class="flex items-center justify-between mb-4">
        <div>
          <h2 class="text-lg font-bold text-white tracking-tight">Independent Paper Instances</h2>
          <p class="text-xs text-gray-400">Three isolated SQLite WAL event stores &bull; Real-time public market data &bull; Local paper execution</p>
        </div>
        <div class="text-xs text-gray-400 font-mono flex items-center gap-2">
          <span>Auto-refresh (2s)</span>
          <span class="w-2 h-2 rounded-full bg-emerald-400 pulse-dot inline-block"></span>
        </div>
      </div>

      <div class="grid grid-cols-1 lg:grid-cols-3 gap-6" id="summary-cards-container">
        <!-- Card 1: BTCUSDT -->
        <div id="card-BTCUSDT" class="bg-dark-800 border border-dark-700 rounded-xl p-5 shadow-sm space-y-4 relative overflow-hidden transition hover:border-dark-600">
          <div class="flex items-center justify-between pb-3 border-b border-dark-700">
            <div class="flex items-center space-x-2">
              <span class="text-lg font-black text-white tracking-wide">BTCUSDT</span>
              <span class="px-2 py-0.5 rounded text-[10px] font-bold bg-amber-500/20 text-amber-300 border border-amber-500/30">PAPER</span>
            </div>
            <div class="flex items-center space-x-1.5">
              <span id="card-status-BTCUSDT" class="px-2.5 py-0.5 rounded text-xs font-bold bg-emerald-500/20 text-emerald-400 border border-emerald-500/30">ONLINE</span>
              <span id="card-db-BTCUSDT" class="text-[10px] font-mono px-1.5 py-0.5 rounded bg-dark-700 text-gray-300">DB: OK</span>
            </div>
          </div>
          <div>
            <div class="text-xs text-gray-400 uppercase tracking-wider font-semibold">Latest Market Price</div>
            <div class="flex items-baseline justify-between mt-1">
              <span id="card-price-BTCUSDT" class="text-3xl font-extrabold text-white tracking-tight">$0.00</span>
              <span id="card-freshness-BTCUSDT" class="text-xs font-mono px-2 py-0.5 rounded bg-dark-700 text-emerald-400">FRESH</span>
            </div>
            <div class="text-[11px] text-gray-500 mt-1 font-mono flex items-center justify-between">
              <span>Last Event: <span id="card-lastevent-BTCUSDT">--</span></span>
              <span id="card-warmup-BTCUSDT">Warmup: --</span>
            </div>
          </div>
          <div class="grid grid-cols-2 gap-3 pt-2 border-t border-dark-700/60 text-xs font-mono">
            <div>
              <span class="text-gray-400 block">Paper Balance</span>
              <span id="card-balance-BTCUSDT" class="text-sm font-bold text-white">$10,000.00</span>
            </div>
            <div>
              <span class="text-gray-400 block">Paper Equity</span>
              <span id="card-equity-BTCUSDT" class="text-sm font-bold text-emerald-400">$10,000.00</span>
            </div>
            <div>
              <span class="text-gray-400 block">Position</span>
              <span id="card-pos-BTCUSDT" class="font-bold text-gray-300">FLAT (0.000)</span>
            </div>
            <div>
              <span class="text-gray-400 block">Unrealized PnL</span>
              <span id="card-unreal-BTCUSDT" class="font-bold text-gray-300">$0.00</span>
            </div>
            <div>
              <span class="text-gray-400 block">Realized PnL</span>
              <span id="card-realized-BTCUSDT" class="font-bold text-gray-300">$0.00</span>
            </div>
            <div>
              <span class="text-gray-400 block">Open Orders</span>
              <span id="card-openorders-BTCUSDT" class="font-bold text-gray-300">0 open</span>
            </div>
          </div>
          <div class="pt-2 border-t border-dark-700/60 flex items-center justify-between text-[11px] font-mono text-gray-400">
            <span id="card-signal-BTCUSDT">Latest Signal: None</span>
            <span id="card-filters-BTCUSDT">tick: 0.10 / step: 0.001</span>
          </div>
          <div class="text-[10px] text-gray-500 font-mono truncate" title="data/btc_paper.db">
            Database: data/btc_paper.db &bull; Benchmark: BTC_ST_09_5M
          </div>
        </div>

        <!-- Card 2: LITUSDT -->
        <div id="card-LITUSDT" class="bg-dark-800 border border-dark-700 rounded-xl p-5 shadow-sm space-y-4 relative overflow-hidden transition hover:border-dark-600">
          <div class="flex items-center justify-between pb-3 border-b border-dark-700">
            <div class="flex items-center space-x-2">
              <span class="text-lg font-black text-white tracking-wide">LITUSDT</span>
              <span class="px-2 py-0.5 rounded text-[10px] font-bold bg-amber-500/20 text-amber-300 border border-amber-500/30">PAPER</span>
            </div>
            <div class="flex items-center space-x-1.5">
              <span id="card-status-LITUSDT" class="px-2.5 py-0.5 rounded text-xs font-bold bg-emerald-500/20 text-emerald-400 border border-emerald-500/30">ONLINE</span>
              <span id="card-db-LITUSDT" class="text-[10px] font-mono px-1.5 py-0.5 rounded bg-dark-700 text-gray-300">DB: OK</span>
            </div>
          </div>
          <div>
            <div class="text-xs text-gray-400 uppercase tracking-wider font-semibold">Latest Market Price</div>
            <div class="flex items-baseline justify-between mt-1">
              <span id="card-price-LITUSDT" class="text-3xl font-extrabold text-white tracking-tight">$0.00</span>
              <span id="card-freshness-LITUSDT" class="text-xs font-mono px-2 py-0.5 rounded bg-dark-700 text-emerald-400">FRESH</span>
            </div>
            <div class="text-[11px] text-gray-500 mt-1 font-mono flex items-center justify-between">
              <span>Last Event: <span id="card-lastevent-LITUSDT">--</span></span>
              <span id="card-warmup-LITUSDT">Warmup: --</span>
            </div>
          </div>
          <div class="grid grid-cols-2 gap-3 pt-2 border-t border-dark-700/60 text-xs font-mono">
            <div>
              <span class="text-gray-400 block">Paper Balance</span>
              <span id="card-balance-LITUSDT" class="text-sm font-bold text-white">$10,000.00</span>
            </div>
            <div>
              <span class="text-gray-400 block">Paper Equity</span>
              <span id="card-equity-LITUSDT" class="text-sm font-bold text-emerald-400">$10,000.00</span>
            </div>
            <div>
              <span class="text-gray-400 block">Position</span>
              <span id="card-pos-LITUSDT" class="font-bold text-gray-300">FLAT (0.0)</span>
            </div>
            <div>
              <span class="text-gray-400 block">Unrealized PnL</span>
              <span id="card-unreal-LITUSDT" class="font-bold text-gray-300">$0.00</span>
            </div>
            <div>
              <span class="text-gray-400 block">Realized PnL</span>
              <span id="card-realized-LITUSDT" class="font-bold text-gray-300">$0.00</span>
            </div>
            <div>
              <span class="text-gray-400 block">Open Orders</span>
              <span id="card-openorders-LITUSDT" class="font-bold text-gray-300">0 open</span>
            </div>
          </div>
          <div class="pt-2 border-t border-dark-700/60 flex items-center justify-between text-[11px] font-mono text-gray-400">
            <span id="card-signal-LITUSDT">Latest Signal: None</span>
            <span id="card-filters-LITUSDT">tick: 0.0001 / step: 0.1</span>
          </div>
          <div class="text-[10px] text-gray-500 font-mono truncate" title="data/lit_paper.db">
            Database: data/lit_paper.db &bull; Benchmark: LIT_SUPERTREND_15M
          </div>
        </div>

        <!-- Card 3: ZECUSDT -->
        <div id="card-ZECUSDT" class="bg-dark-800 border border-dark-700 rounded-xl p-5 shadow-sm space-y-4 relative overflow-hidden transition hover:border-dark-600">
          <div class="flex items-center justify-between pb-3 border-b border-dark-700">
            <div class="flex items-center space-x-2">
              <span class="text-lg font-black text-white tracking-wide">ZECUSDT</span>
              <span class="px-2 py-0.5 rounded text-[10px] font-bold bg-amber-500/20 text-amber-300 border border-amber-500/30">PAPER</span>
            </div>
            <div class="flex items-center space-x-1.5">
              <span id="card-status-ZECUSDT" class="px-2.5 py-0.5 rounded text-xs font-bold bg-emerald-500/20 text-emerald-400 border border-emerald-500/30">ONLINE</span>
              <span id="card-db-ZECUSDT" class="text-[10px] font-mono px-1.5 py-0.5 rounded bg-dark-700 text-gray-300">DB: OK</span>
            </div>
          </div>
          <div>
            <div class="text-xs text-gray-400 uppercase tracking-wider font-semibold">Latest Market Price</div>
            <div class="flex items-baseline justify-between mt-1">
              <span id="card-price-ZECUSDT" class="text-3xl font-extrabold text-white tracking-tight">$0.00</span>
              <span id="card-freshness-ZECUSDT" class="text-xs font-mono px-2 py-0.5 rounded bg-dark-700 text-emerald-400">FRESH</span>
            </div>
            <div class="text-[11px] text-gray-500 mt-1 font-mono flex items-center justify-between">
              <span>Last Event: <span id="card-lastevent-ZECUSDT">--</span></span>
              <span id="card-warmup-ZECUSDT">Warmup: --</span>
            </div>
          </div>
          <div class="grid grid-cols-2 gap-3 pt-2 border-t border-dark-700/60 text-xs font-mono">
            <div>
              <span class="text-gray-400 block">Paper Balance</span>
              <span id="card-balance-ZECUSDT" class="text-sm font-bold text-white">$10,000.00</span>
            </div>
            <div>
              <span class="text-gray-400 block">Paper Equity</span>
              <span id="card-equity-ZECUSDT" class="text-sm font-bold text-emerald-400">$10,000.00</span>
            </div>
            <div>
              <span class="text-gray-400 block">Position</span>
              <span id="card-pos-ZECUSDT" class="font-bold text-gray-300">FLAT (0.000)</span>
            </div>
            <div>
              <span class="text-gray-400 block">Unrealized PnL</span>
              <span id="card-unreal-ZECUSDT" class="font-bold text-gray-300">$0.00</span>
            </div>
            <div>
              <span class="text-gray-400 block">Realized PnL</span>
              <span id="card-realized-ZECUSDT" class="font-bold text-gray-300">$0.00</span>
            </div>
            <div>
              <span class="text-gray-400 block">Open Orders</span>
              <span id="card-openorders-ZECUSDT" class="font-bold text-gray-300">0 open</span>
            </div>
          </div>
          <div class="pt-2 border-t border-dark-700/60 flex items-center justify-between text-[11px] font-mono text-gray-400">
            <span id="card-signal-ZECUSDT">Latest Signal: None</span>
            <span id="card-filters-ZECUSDT">tick: 0.01 / step: 0.001</span>
          </div>
          <div class="text-[10px] text-gray-500 font-mono truncate" title="data/zec_paper.db">
            Database: data/zec_paper.db &bull; Benchmark: ZEC_MOMENTUM_M03_15M
          </div>
        </div>
      </div>
    </section>

    <!-- DETAILED PER-SYMBOL SECTION (TABS) -->
    <section class="bg-dark-800 border border-dark-700 rounded-xl p-6 shadow-sm space-y-6">
      <div class="flex flex-wrap items-center justify-between pb-4 border-b border-dark-700 gap-4">
        <div class="flex items-center space-x-3">
          <span class="font-bold text-white text-base">Instance Inspection & Audit</span>
          <div class="inline-flex rounded-lg bg-dark-900 p-1 border border-dark-700" id="tabs-container">
            <button onclick="switchTab('BTCUSDT')" id="tab-btn-BTCUSDT" class="tab-btn px-4 py-1.5 text-xs font-bold rounded-md bg-amber-500 text-black shadow transition">BTCUSDT</button>
            <button onclick="switchTab('LITUSDT')" id="tab-btn-LITUSDT" class="tab-btn px-4 py-1.5 text-xs font-semibold rounded-md text-gray-400 hover:text-white transition">LITUSDT</button>
            <button onclick="switchTab('ZECUSDT')" id="tab-btn-ZECUSDT" class="tab-btn px-4 py-1.5 text-xs font-semibold rounded-md text-gray-400 hover:text-white transition">ZECUSDT</button>
          </div>
        </div>
        <div class="text-xs text-gray-400 font-mono" id="active-db-notice">
          Selected Database: <span class="text-white" id="detail-db-label">data/btc_paper.db</span>
        </div>
      </div>

      <!-- UNAVAILABLE BANNER (HIDDEN UNLESS DB FAILS) -->
      <div id="detail-unavailable-banner" class="hidden p-4 rounded-lg bg-red-500/10 border border-red-500/30 text-red-400 text-xs font-mono flex items-center justify-between">
        <span>DATABASE UNAVAILABLE: The local event database could not be reached or opened. Other symbol instances remain operational.</span>
        <span class="font-bold">STATUS: OFFLINE</span>
      </div>

      <!-- LIVE TRADINGVIEW CANDLESTICK CHART -->
      <div class="bg-dark-900/60 border border-dark-700 rounded-xl p-4 shadow-sm flex flex-col space-y-3">
        <div class="flex flex-wrap items-center justify-between pb-3 border-b border-dark-700 gap-2">
          <div class="flex items-center space-x-3">
            <span class="font-bold text-white text-sm" id="chart-symbol-title">BTCUSDT Live Candlestick Chart</span>
            <span id="chart-st-params" class="text-xs px-2.5 py-0.5 rounded bg-dark-700 text-gray-300 font-mono font-bold">Supertrend (10, 3.0)</span>
            <span id="chart-st-dir" class="text-xs font-mono font-bold px-2.5 py-0.5 rounded bg-dark-700 text-gray-300">--</span>
            <span id="chart-st-val" class="text-xs font-mono font-bold text-gray-400"></span>
          </div>
          <div class="flex items-center space-x-2 text-xs">
            <span class="w-2 h-2 rounded-full bg-emerald-400 inline-block pulse-dot"></span>
            <span class="text-gray-300 font-mono text-[11px]">Real-time indicator feed</span>
          </div>
        </div>
        <div id="multi-chart-container" class="w-full h-[380px] rounded-lg overflow-hidden bg-dark-900"></div>
      </div>

      <!-- SUB-GRIDS: SIGNALS & ORDERS -->
      <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">

        <!-- SIGNALS FEED -->
        <div class="bg-dark-900/60 border border-dark-700 rounded-lg p-4 space-y-3">
          <div class="flex items-center justify-between pb-2 border-b border-dark-700">
            <span class="font-semibold text-white text-xs uppercase tracking-wider">Recent Strategy Signals</span>
            <span id="detail-signals-count" class="text-xs font-mono px-2 py-0.5 rounded bg-dark-700 text-gray-300">0 signals</span>
          </div>
          <div id="detail-signals-list" class="space-y-2 max-h-[360px] overflow-y-auto pr-1">
            <div class="text-center py-10 text-gray-500 text-xs font-mono">No signals recorded yet.</div>
          </div>
        </div>

        <!-- ORDERS & FILLS -->
        <div class="bg-dark-900/60 border border-dark-700 rounded-lg p-4 space-y-3">
          <div class="flex items-center justify-between pb-2 border-b border-dark-700">
            <span class="font-semibold text-white text-xs uppercase tracking-wider">Recent Paper Orders & Fills</span>
            <span id="detail-orders-count" class="text-xs font-mono px-2 py-0.5 rounded bg-dark-700 text-gray-300">0 orders</span>
          </div>
          <div class="overflow-x-auto max-h-[360px] overflow-y-auto">
            <table class="w-full text-left text-xs font-mono">
              <thead class="bg-dark-900 text-gray-400 border-b border-dark-700 sticky top-0">
                <tr>
                  <th class="py-2 px-2.5">Time (UTC)</th>
                  <th class="py-2 px-2.5">Client Order ID</th>
                  <th class="py-2 px-2.5">Side</th>
                  <th class="py-2 px-2.5">Qty</th>
                  <th class="py-2 px-2.5">Price</th>
                  <th class="py-2 px-2.5">Status</th>
                </tr>
              </thead>
              <tbody id="detail-orders-tbody" class="divide-y divide-dark-700/50">
                <tr><td colspan="6" class="text-center py-8 text-gray-500">No orders executed yet.</td></tr>
              </tbody>
            </table>
          </div>
        </div>
      </div>

      <!-- RECENT 15M CANDLES & INCIDENTS -->
      <div class="grid grid-cols-1 lg:grid-cols-2 gap-6 pt-2">

        <!-- 15M RECONSTRUCTED CANDLES -->
        <div class="bg-dark-900/60 border border-dark-700 rounded-lg p-4 space-y-3">
          <div class="flex items-center justify-between pb-2 border-b border-dark-700">
            <span class="font-semibold text-white text-xs uppercase tracking-wider">Recent 15m Reconstructed Candles</span>
            <span id="detail-candles-count" class="text-xs font-mono px-2 py-0.5 rounded bg-dark-700 text-gray-300">0 candles</span>
          </div>
          <div class="overflow-x-auto max-h-[280px] overflow-y-auto">
            <table class="w-full text-left text-xs font-mono">
              <thead class="bg-dark-900 text-gray-400 border-b border-dark-700 sticky top-0">
                <tr>
                  <th class="py-1.5 px-2">Time (UTC)</th>
                  <th class="py-1.5 px-2">Open</th>
                  <th class="py-1.5 px-2">High</th>
                  <th class="py-1.5 px-2">Low</th>
                  <th class="py-1.5 px-2">Close</th>
                  <th class="py-1.5 px-2">Volume</th>
                </tr>
              </thead>
              <tbody id="detail-candles-tbody" class="divide-y divide-dark-700/50">
                <tr><td colspan="6" class="text-center py-6 text-gray-500">No candles in database yet.</td></tr>
              </tbody>
            </table>
          </div>
        </div>

        <!-- OPERATIONAL INCIDENTS & AUDIT LOG -->
        <div class="bg-dark-900/60 border border-dark-700 rounded-lg p-4 space-y-3">
          <div class="flex items-center justify-between pb-2 border-b border-dark-700">
            <span class="font-semibold text-white text-xs uppercase tracking-wider">Operational Audit & Incidents</span>
            <span id="detail-incidents-count" class="text-xs font-mono px-2 py-0.5 rounded bg-dark-700 text-gray-300">0 records</span>
          </div>
          <div id="detail-incidents-list" class="space-y-2 max-h-[280px] overflow-y-auto pr-1">
            <div class="text-center py-6 text-gray-500 text-xs font-mono">No incidents recorded. System nominal.</div>
          </div>
        </div>
      </div>
    </section>
  </main>

  <!-- FOOTER -->
  <footer class="border-t border-dark-700 py-3 px-6 text-xs text-gray-500 flex flex-wrap justify-between items-center gap-2 mt-auto">
    <div>Escanor Multi-Symbol Paper Dashboard &bull; BTCUSDT &bull; LITUSDT &bull; ZECUSDT</div>
    <div class="flex items-center space-x-4 font-mono">
      <span>Storage: 3 Isolated SQLite WAL DBs</span>
      <span>Safety: Read-Only Observability</span>
      <span>API: Localhost</span>
    </div>
  </footer>

  <script>
    // HTML escaping helper to prevent XSS
    function escapeHtml(s) {
      if (s === null || s === undefined) return '';
      return String(s)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#039;');
    }

    // Theme toggle
    function setThemeLabel(theme) {
      const btn = document.getElementById('btn-theme');
      if (btn) btn.innerText = theme === 'light' ? '🌙 DARK' : '☀️ LIGHT';
    }
    function toggleTheme() {
      const root = document.documentElement;
      const next = (root.getAttribute('data-theme') === 'light') ? 'dark' : 'light';
      root.setAttribute('data-theme', next);
      try { localStorage.setItem('escanor-multi-theme', next); } catch (e) {}
      setThemeLabel(next);
      if (chart) {
        const isLight = next === 'light';
        chart.applyOptions({
          layout: {
            background: { color: isLight ? '#FFFFFF' : '#0B0F19' },
            textColor: isLight ? '#475569' : '#94A3B8',
          },
          grid: {
            vertLines: { color: isLight ? '#E2E8F0' : '#1E293B' },
            horzLines: { color: isLight ? '#E2E8F0' : '#1E293B' },
          },
          rightPriceScale: { borderColor: isLight ? '#CBD5E1' : '#334155' },
          timeScale: { borderColor: isLight ? '#CBD5E1' : '#334155' },
        });
      }
    }
    (function initTheme() {
      let saved = 'dark';
      try { saved = localStorage.getItem('escanor-multi-theme') || 'dark'; } catch (e) {}
      document.documentElement.setAttribute('data-theme', saved);
      setThemeLabel(saved);
    })();

    // Clock
    setInterval(() => {
      const el = document.getElementById('clock-utc');
      if (el) el.innerText = new Date().toISOString().replace('T', ' ').substring(11, 19) + ' UTC';
    }, 1000);

    // Active Symbol Tab & Chart Controller
    let activeTabSymbol = 'BTCUSDT';
    let cachedAccounts = [];
    let chart, candleSeries, volumeSeries;
    const ST_COLORS = { bullish: '#10B981', bearish: '#EF4444', neutral: '#94A3B8' };
    const ST_BREAK = 'rgba(0,0,0,0)';
    let indicatorSeries = null;
    let slowSeries = null;
    let chartRenderedKey = '';

    function initChart() {
      const container = document.getElementById('multi-chart-container');
      if (!container || typeof LightweightCharts === 'undefined') return;
      container.innerHTML = '';
      const isLight = document.documentElement.getAttribute('data-theme') === 'light';

      chart = LightweightCharts.createChart(container, {
        width: container.clientWidth,
        height: 380,
        layout: {
          background: { color: isLight ? '#FFFFFF' : '#0B0F19' },
          textColor: isLight ? '#475569' : '#94A3B8',
        },
        grid: {
          vertLines: { color: isLight ? '#E2E8F0' : '#1E293B' },
          horzLines: { color: isLight ? '#E2E8F0' : '#1E293B' },
        },
        crosshair: {
          mode: LightweightCharts.CrosshairMode.Normal,
        },
        rightPriceScale: {
          borderColor: isLight ? '#CBD5E1' : '#334155',
        },
        timeScale: {
          borderColor: isLight ? '#CBD5E1' : '#334155',
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
        if (chart && container) {
          chart.applyOptions({ width: container.clientWidth });
        }
      });
    }

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

    function switchTab(sym) {
      if (activeTabSymbol !== sym) {
        activeTabSymbol = sym;
        chartRenderedKey = '';
      }
      ['BTCUSDT', 'LITUSDT', 'ZECUSDT'].forEach(s => {
        const btn = document.getElementById('tab-btn-' + s);
        if (btn) {
          if (s === sym) {
            btn.className = 'tab-btn px-4 py-1.5 text-xs font-bold rounded-md bg-amber-500 text-black shadow transition';
          } else {
            btn.className = 'tab-btn px-4 py-1.5 text-xs font-semibold rounded-md text-gray-400 hover:text-white transition';
          }
        }
      });
      const activeAcc = cachedAccounts.find(a => a.symbol === activeTabSymbol);
      if (activeAcc) {
        renderDetails(activeAcc);
      }
    }

    // Render single summary card
    function renderCard(acc) {
      const sym = acc.symbol;
      const isOnline = acc.database_available !== false;

      // Status badges
      const statusEl = document.getElementById('card-status-' + sym);
      const dbBadgeEl = document.getElementById('card-db-' + sym);
      if (statusEl) {
        if (!isOnline) {
          statusEl.textContent = 'DATABASE UNAVAILABLE';
          statusEl.className = 'px-2.5 py-0.5 rounded text-xs font-bold bg-red-500/20 text-red-400 border border-red-500/30';
        } else {
          statusEl.textContent = acc.status || 'HEALTHY';
          statusEl.className = (acc.status === 'HEALTHY' || acc.status === 'ONLINE')
            ? 'px-2.5 py-0.5 rounded text-xs font-bold bg-emerald-500/20 text-emerald-400 border border-emerald-500/30'
            : (acc.status === 'STALE' ? 'px-2.5 py-0.5 rounded text-xs font-bold bg-amber-500/20 text-amber-300 border border-amber-500/30'
            : 'px-2.5 py-0.5 rounded text-xs font-bold bg-blue-500/20 text-blue-400 border border-blue-500/30');
        }
      }
      if (dbBadgeEl) {
        dbBadgeEl.textContent = isOnline ? 'DB: ONLINE' : 'DB: UNAVAILABLE';
        dbBadgeEl.className = isOnline
          ? 'text-[10px] font-mono px-1.5 py-0.5 rounded bg-emerald-500/10 text-emerald-400 border border-emerald-500/20'
          : 'text-[10px] font-mono px-1.5 py-0.5 rounded bg-red-500/10 text-red-400 border border-red-500/20';
      }

      // Price
      const priceEl = document.getElementById('card-price-' + sym);
      if (priceEl) {
        if (acc.latest_price != null) {
          const num = parseFloat(acc.latest_price);
          priceEl.textContent = '$' + (isNaN(num) ? escapeHtml(acc.latest_price) : num.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 4 }));
        } else {
          priceEl.textContent = isOnline ? '$--' : 'UNAVAILABLE';
        }
      }

      // Freshness
      const freshEl = document.getElementById('card-freshness-' + sym);
      if (freshEl) {
        freshEl.textContent = acc.freshness || 'NO DATA';
        freshEl.className = acc.freshness === 'FRESH'
          ? 'text-xs font-mono px-2 py-0.5 rounded bg-emerald-500/10 text-emerald-400'
          : (acc.freshness === 'WARM' ? 'text-xs font-mono px-2 py-0.5 rounded bg-amber-500/10 text-amber-300'
          : 'text-xs font-mono px-2 py-0.5 rounded bg-dark-700 text-gray-400');
      }

      // Last Event & Warmup
      const lastEventEl = document.getElementById('card-lastevent-' + sym);
      if (lastEventEl) {
        lastEventEl.textContent = acc.last_event_time_utc ? acc.last_event_time_utc.replace(' UTC', '') : '--';
      }
      const warmupEl = document.getElementById('card-warmup-' + sym);
      if (warmupEl) {
        warmupEl.textContent = acc.warmup_state ? acc.warmup_state : ('15m: ' + (acc.candle_count_15m || 0));
      }

      // Financials
      const balEl = document.getElementById('card-balance-' + sym);
      if (balEl) balEl.textContent = '$' + (acc.balance_usdt || '10,000.00');

      const eqEl = document.getElementById('card-equity-' + sym);
      if (eqEl) eqEl.textContent = '$' + (acc.paper_equity_usdt || '10,000.00');

      const posEl = document.getElementById('card-pos-' + sym);
      if (posEl) {
        const p = acc.position || {};
        const side = p.side || 'FLAT';
        const qty = p.quantity || '0.000';
        posEl.textContent = side + ' (' + qty + ')';
        posEl.className = side === 'LONG' || side === 'BUY'
          ? 'font-bold text-emerald-400'
          : (side === 'SHORT' || side === 'SELL' ? 'font-bold text-red-400' : 'font-bold text-gray-300');
      }

      const unrealEl = document.getElementById('card-unreal-' + sym);
      if (unrealEl) {
        const val = parseFloat(acc.position?.unrealized_pnl || 0);
        unrealEl.textContent = (val >= 0 ? '+$' : '-$') + Math.abs(val).toFixed(2);
        unrealEl.className = val >= 0 ? 'font-bold text-emerald-400' : 'font-bold text-red-400';
      }

      const realEl = document.getElementById('card-realized-' + sym);
      if (realEl) {
        const val = parseFloat(acc.position?.realized_pnl || 0);
        realEl.textContent = (val >= 0 ? '+$' : '-$') + Math.abs(val).toFixed(2);
        realEl.className = val >= 0 ? 'font-bold text-emerald-400' : 'font-bold text-red-400';
      }

      const ordersEl = document.getElementById('card-openorders-' + sym);
      if (ordersEl) {
        ordersEl.textContent = (acc.open_orders_count || 0) + ' open';
      }

      // Signal & Filters
      const sigEl = document.getElementById('card-signal-' + sym);
      if (sigEl) {
        if (acc.latest_signal && acc.latest_signal.action) {
          sigEl.textContent = 'Signal: ' + acc.latest_signal.action + ' @ $' + acc.latest_signal.price;
        } else {
          sigEl.textContent = 'Signal: None yet';
        }
      }

      const filtersEl = document.getElementById('card-filters-' + sym);
      if (filtersEl && acc.filters) {
        filtersEl.textContent = 'tick: ' + (acc.filters.tick_size || '--') + ' / step: ' + (acc.filters.step_size || '--');
      }
    }

    // Render detailed views for selected symbol
    function renderDetails(acc) {
      document.getElementById('detail-db-label').textContent = acc.database || '--';

      const unavailBanner = document.getElementById('detail-unavailable-banner');
      if (acc.database_available === false) {
        unavailBanner.classList.remove('hidden');
      } else {
        unavailBanner.classList.add('hidden');
      }

      // 0. Update Chart Header & Indicators
      const titleEl = document.getElementById('chart-symbol-title');
      if (titleEl) {
        const tfLabel = acc.timeframe || '15m';
        titleEl.textContent = `${acc.symbol} ${tfLabel} Candlestick Chart`;
      }

      const stParamsEl = document.getElementById('chart-st-params');
      if (stParamsEl) {
        const paramLabel = `${acc.indicator_name || 'Supertrend'} ${acc.supertrend_params || '(10, 3.0)'}`;
        stParamsEl.textContent = paramLabel;
      }

      const stDirEl = document.getElementById('chart-st-dir');
      if (stDirEl) {
        const dir = acc.supertrend_direction || 'NEUTRAL';
        stDirEl.textContent = dir === 'BULLISH' ? 'BULLISH ↑' : (dir === 'BEARISH' ? 'BEARISH ↓' : 'NEUTRAL •');
        stDirEl.className = dir === 'BULLISH'
          ? 'text-xs font-mono font-bold px-2.5 py-0.5 rounded bg-emerald-500/20 text-emerald-400 border border-emerald-500/30'
          : (dir === 'BEARISH' ? 'text-xs font-mono font-bold px-2.5 py-0.5 rounded bg-red-500/20 text-red-400 border border-red-500/30'
          : 'text-xs font-mono font-bold px-2.5 py-0.5 rounded bg-dark-700 text-gray-300');
      }

      const stValEl = document.getElementById('chart-st-val');
      if (stValEl) {
        stValEl.textContent = acc.supertrend_val ? `${acc.indicator_name === 'Momentum EMA' ? 'EMA' : 'ST'}: $${acc.supertrend_val}` : '';
        stValEl.className = acc.supertrend_direction === 'BULLISH'
          ? 'text-xs font-mono font-bold text-emerald-400'
          : (acc.supertrend_direction === 'BEARISH' ? 'text-xs font-mono font-bold text-red-400' : 'text-xs font-mono font-bold text-gray-400');
      }

      // Initialize chart if ready
      if (!chart && typeof LightweightCharts !== 'undefined') {
        initChart();
      }

      // Update Chart Series
      if (chart && acc.chart_candles && acc.chart_candles.length > 0) {
        const lastC = acc.chart_candles[acc.chart_candles.length - 1];
        const newKey = `${acc.symbol}_${acc.chart_candles.length}_${lastC.time}_${lastC.close}_${acc.supertrend_direction}_${(acc.trade_markers || []).length}`;
        if (newKey !== chartRenderedKey) {
          candleSeries.setData(acc.chart_candles);

          const volData = acc.chart_candles.map(c => ({
            time: c.time,
            value: c.volume || 0,
            color: c.close >= c.open ? '#10B98144' : '#EF444444',
          }));
          volumeSeries.setData(volData);

          renderIndicator(acc);

          if (acc.trade_markers && acc.trade_markers.length > 0) {
            candleSeries.setMarkers(acc.trade_markers);
          } else {
            candleSeries.setMarkers([]);
          }

          chart.timeScale().fitContent();
          chartRenderedKey = newKey;
        }
      }

      // 1. Signals List
      const sigContainer = document.getElementById('detail-signals-list');
      const sigCountEl = document.getElementById('detail-signals-count');
      const signals = acc.recent_signals || [];
      sigCountEl.textContent = signals.length + ' signals';

      if (signals.length > 0) {
        sigContainer.innerHTML = signals.map(s => {
          const isBuy = s.action === 'ENTER_LONG' || s.action === 'BUY';
          const isSell = s.action === 'EXIT_LONG' || s.action === 'SELL';
          const badgeClass = isBuy
            ? 'bg-emerald-500/20 text-emerald-400 border border-emerald-500/30'
            : (isSell ? 'bg-red-500/20 text-red-400 border border-red-500/30' : 'bg-dark-700 text-gray-400');
          return `
            <div class="p-2.5 bg-dark-950/60 border border-dark-700 rounded-md text-xs font-mono space-y-1">
              <div class="flex items-center justify-between">
                <span class="px-2 py-0.5 rounded text-[10px] font-bold ${badgeClass}">${escapeHtml(s.action)}</span>
                <span class="text-gray-500">${escapeHtml(s.timestamp_utc)}</span>
              </div>
              <div class="flex items-center justify-between text-gray-300">
                <span>Trigger: <strong class="text-white">$${escapeHtml(s.price)}</strong></span>
                <span class="text-gray-500 truncate max-w-[200px]" title="${escapeHtml(s.reason)}">${escapeHtml(s.reason)}</span>
              </div>
            </div>
          `;
        }).join('');
      } else {
        sigContainer.innerHTML = `<div class="text-center py-10 text-gray-500 text-xs font-mono">No signals recorded in ${escapeHtml(acc.database)}.</div>`;
      }

      // 2. Orders Table
      const ordersTbody = document.getElementById('detail-orders-tbody');
      const ordersCountEl = document.getElementById('detail-orders-count');
      const orders = acc.recent_orders || [];
      ordersCountEl.textContent = orders.length + ' orders';

      if (orders.length > 0) {
        ordersTbody.innerHTML = orders.map(o => {
          const isBuy = o.side === 'BUY';
          const sideClass = isBuy ? 'text-emerald-400 font-bold' : 'text-red-400 font-bold';
          const statusClass = o.status === 'FILLED' ? 'text-emerald-400' : 'text-gray-400';
          return `
            <tr class="hover:bg-dark-700/30 transition">
              <td class="py-2 px-2.5 text-gray-400">${escapeHtml(o.created_at_utc)}</td>
              <td class="py-2 px-2.5 text-gray-300 truncate max-w-[130px]" title="${escapeHtml(o.client_order_id)}">${escapeHtml(o.client_order_id)}</td>
              <td class="py-2 px-2.5 ${sideClass}">${escapeHtml(o.side)}</td>
              <td class="py-2 px-2.5 text-white font-bold">${escapeHtml(o.quantity)}</td>
              <td class="py-2 px-2.5 text-gray-300">$${escapeHtml(o.price)}</td>
              <td class="py-2 px-2.5 ${statusClass} font-semibold">${escapeHtml(o.status)}</td>
            </tr>
          `;
        }).join('');
      } else {
        ordersTbody.innerHTML = `<tr><td colspan="6" class="text-center py-8 text-gray-500">No orders recorded in ${escapeHtml(acc.database)}.</td></tr>`;
      }

      // 3. Candles Table
      const candlesTbody = document.getElementById('detail-candles-tbody');
      const candlesCountEl = document.getElementById('detail-candles-count');
      const candles = acc.recent_candles || [];
      candlesCountEl.textContent = candles.length + ' bars';

      if (candles.length > 0) {
        candlesTbody.innerHTML = candles.map(c => `
          <tr class="hover:bg-dark-700/30 transition">
            <td class="py-1.5 px-2 text-gray-400">${escapeHtml(c.time_utc)}</td>
            <td class="py-1.5 px-2 text-gray-300">$${escapeHtml(c.open)}</td>
            <td class="py-1.5 px-2 text-emerald-400">$${escapeHtml(c.high)}</td>
            <td class="py-1.5 px-2 text-red-400">$${escapeHtml(c.low)}</td>
            <td class="py-1.5 px-2 text-white font-bold">$${escapeHtml(c.close)}</td>
            <td class="py-1.5 px-2 text-gray-400">${escapeHtml(c.volume)}</td>
          </tr>
        `).join('');
      } else {
        candlesTbody.innerHTML = `<tr><td colspan="6" class="text-center py-6 text-gray-500">No 15m candles in ${escapeHtml(acc.database)}.</td></tr>`;
      }

      // 4. Incidents List
      const incContainer = document.getElementById('detail-incidents-list');
      const incCountEl = document.getElementById('detail-incidents-count');
      const incidents = acc.recent_incidents || [];
      incCountEl.textContent = incidents.length + ' records';

      if (incidents.length > 0) {
        incContainer.innerHTML = incidents.map(i => `
          <div class="p-2 bg-dark-950/60 border border-dark-700 rounded text-xs font-mono flex items-center justify-between">
            <div class="space-x-2">
              <span class="px-1.5 py-0.5 rounded text-[10px] bg-dark-700 text-gray-300 font-bold">${escapeHtml(i.category)}</span>
              <span class="text-gray-300">${escapeHtml(i.details)}</span>
            </div>
            <span class="text-gray-500 text-[10px]">${escapeHtml(i.timestamp_utc)}</span>
          </div>
        `).join('');
      } else {
        incContainer.innerHTML = `<div class="text-center py-6 text-gray-500 text-xs font-mono">No incidents recorded. System nominal.</div>`;
      }
    }

    // Polling loop
    async function updateDashboard() {
      try {
        const res = await fetch('/api/accounts');
        if (!res.ok) return;
        const data = await res.json();
        const accounts = data.accounts || [];
        cachedAccounts = accounts;

        accounts.forEach(renderCard);

        const activeAcc = accounts.find(a => a.symbol === activeTabSymbol) || accounts[0];
        if (activeAcc) {
          renderDetails(activeAcc);
        }
      } catch (err) {
        console.warn("Multi-symbol dashboard poll error:", err);
      }
    }

    // Initial load and periodic refresh
    updateDashboard();
    setInterval(updateDashboard, 2000);
  </script>
</body>
</html>
"""



class MultiSymbolDataAggregator:
    """Aggregates snapshots from three isolated SQLite PAPER databases."""

    def __init__(self, configs: List[LiveEngineConfig], base_dir: Optional[Path | str] = None):
        self.configs = configs
        self.base_dir = Path(base_dir or Path.cwd()).resolve()
        validate_unique_databases(configs, self.base_dir)
        self.dashboards = {
            cfg.symbol: dashboard_class(cfg.symbol)(cfg, base_dir=self.base_dir)
            for cfg in configs
        }
        self.last_projection_ms: int = 0

    def record_projection_heartbeat(self) -> float:
        """Records that the read-only projection refreshed, and returns its epoch time.

        The dashboard opens every instance database in read-only mode, so its liveness
        cannot be written into them. It lands in a small file under logs/ instead, where
        the supervisor can see whether the projection is still refreshing.
        """
        import json
        import time

        now = time.time()
        self.last_projection_ms = int(now * 1000)
        try:
            path = self.base_dir / "logs" / "dashboard-heartbeat.json"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps({
                "component": "dashboard_projection",
                "last_beat_ms": self.last_projection_ms,
                "symbols": [cfg.symbol for cfg in self.configs],
            }), encoding="utf-8")
        except OSError:
            # A dashboard that cannot write its heartbeat must still serve the projection.
            pass
        return now

    def get_accounts_payload(self) -> Dict[str, Any]:
        """Returns aggregate snapshot for all configured symbols."""
        accounts = []
        for cfg in self.configs:
            snap = read_single_account_snapshot(cfg, base_dir=self.base_dir)
            accounts.append(snap)

        import datetime
        self.record_projection_heartbeat()
        return {
            "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "projection_heartbeat_ms": self.last_projection_ms,
            "disclaimer": "SIMULATED PAPER ACCOUNTS — NO REAL ORDERS",
            "accounts": accounts,
        }

    def get_symbol_payload(self, symbol: str) -> Dict[str, Any]:
        """Returns snapshot for a specific symbol."""
        for cfg in self.configs:
            if cfg.symbol.upper() == symbol.upper():
                return read_single_account_snapshot(cfg, base_dir=self.base_dir)
        return {"error": f"Symbol {symbol} not configured"}


class MultiSymbolHTTPHandler(BaseHTTPHandler):
    """HTTP request handler serving multi-symbol paper dashboard UI and REST API."""

    aggregator: MultiSymbolDataAggregator

    def log_message(self, format, *args):
        return

    def handle(self) -> None:
        try:
            super().handle()
        except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError):
            pass

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        parts = parsed.path.strip("/").split("/")
        symbol = {"BTC": "BTCUSDT", "LIT": "LITUSDT", "ZEC": "ZECUSDT"}.get(parts[0].upper(), parts[0].upper())
        dashboard = self.aggregator.dashboards.get(symbol)
        if dashboard and (len(parts) == 1 or parts[1:] == ["api", "data"]):
            self.dashboard = dashboard
            return super().do_GET()
        if parsed.path in ("/", "/index.html"):
            content = MULTI_SYMBOL_DASHBOARD_HTML.replace("<!-- SYMBOL_NAVIGATION -->", SYMBOL_NAVIGATION_HTML).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)
        elif parsed.path == "/api/accounts":
            payload = self.aggregator.get_accounts_payload()
            body = json.dumps(payload).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif parsed.path == "/api/data":
            qs = parse_qs(parsed.query)
            sym = qs.get("symbol", [None])[0]
            if sym:
                payload = self.aggregator.get_symbol_payload(sym.upper())
            else:
                payload = self.aggregator.get_accounts_payload()
            body = json.dumps(payload).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif parsed.path == "/api/ping":
            body = b'{"status": "ok"}'
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(404)
            self.end_headers()

def validate_multi_symbol_dashboard_configs(
    config_paths: List[str], base_dir: Optional[Path | str] = None
) -> List[LiveEngineConfig]:
    """Validates the 3 configurations for the multi-symbol paper dashboard.

    Requirements:
    - Exactly three configurations
    - Symbols are exactly BTCUSDT, LITUSDT, and ZECUSDT
    - Every mode is PAPER
    - Every config matches its manifest
    - All resolved database paths are unique
    - All databases resolve underneath the repository's data directory
    - No database path is provided by an HTTP parameter
    - No API credentials are required or displayed
    """
    repo_root = Path(base_dir or Path.cwd()).resolve()

    if len(config_paths) != 3:
        raise ValueError(
            f"MULTI-SYMBOL DASHBOARD ERROR: Exactly 3 configurations required, got {len(config_paths)}."
        )

    configs: List[LiveEngineConfig] = []
    for cp in config_paths:
        p = Path(cp)
        if not p.is_absolute():
            p = (repo_root / p).resolve()
        if not p.exists():
            raise FileNotFoundError(f"Configuration file not found: {cp}")
        cfg = load_config(str(p))
        configs.append(cfg)

    # 1. Require symbols are exactly BTCUSDT, LITUSDT, and ZECUSDT
    symbols = {c.symbol.upper() for c in configs}
    expected_symbols = {"BTCUSDT", "LITUSDT", "ZECUSDT"}
    if symbols != expected_symbols:
        raise ValueError(
            f"MULTI-SYMBOL DASHBOARD ERROR: Symbols must be exactly {sorted(expected_symbols)}. "
            f"Got: {sorted(symbols)}."
        )

    # 2. Require every mode is PAPER
    for c in configs:
        if c.mode.upper() != "PAPER":
            raise ValueError(
                f"MULTI-SYMBOL DASHBOARD ERROR: Every mode must be PAPER. "
                f"Symbol {c.symbol} has mode '{c.mode}'. Non-PAPER modes are rejected."
            )

    # 3. Require every config matches its manifest
    import yaml
    for c in configs:
        m_path = Path(c.manifest_path)
        if not m_path.is_absolute():
            m_path = (repo_root / m_path).resolve()
        if not m_path.exists():
            raise FileNotFoundError(f"Manifest file not found: {c.manifest_path}")
        manifest = yaml.safe_load(m_path.read_text(encoding="utf-8")) or {}
        validate_config_against_manifest(c, manifest)

    # 4. Require all resolved database paths are unique and strictly under data/
    validate_unique_databases(configs, base_dir=repo_root)

    # 5. Require no API credentials are present
    for c in configs:
        if c.binance_api_key or c.binance_api_secret:
            raise ValueError(
                f"MULTI-SYMBOL DASHBOARD ERROR: No API credentials permitted for PAPER dashboard ({c.symbol})."
            )

    return configs


def start_multi_symbol_dashboard_server(
    configs: List[LiveEngineConfig],
    host: str = "127.0.0.1",
    port: int = 8080,
    base_dir: Optional[Path | str] = None,
) -> ThreadingHTTPServer:
    """Starts the multi-symbol paper dashboard server in a daemon thread."""
    handler = type("PortfolioHTTPHandler", (MultiSymbolHTTPHandler,), {"aggregator": MultiSymbolDataAggregator(configs, base_dir=base_dir)})
    server = ThreadingHTTPServer((host, port), handler)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    logger.info(f"Multi-symbol paper dashboard online at http://{host}:{port}")
    return server


def run_multi_symbol_dashboard(
    config_paths: List[str],
    host: str = "127.0.0.1",
    port: int = 8080,
    base_dir: Optional[Path | str] = None,
) -> int:
    """CLI entrypoint to run the multi-symbol paper dashboard."""
    configs = validate_multi_symbol_dashboard_configs(config_paths, base_dir=base_dir)
    handler = type("PortfolioHTTPHandler", (MultiSymbolHTTPHandler,), {"aggregator": MultiSymbolDataAggregator(configs, base_dir=base_dir)})
    server = ThreadingHTTPServer((host, port), handler)
    print("==================================================")
    print("  ESCANOR MULTI-SYMBOL PAPER DASHBOARD")
    print(f"  URL: http://{host}:{port}")
    print("  Symbols: BTCUSDT, LITUSDT, ZECUSDT")
    print("  Mode: PAPER (Isolated Accounts)")
    print("  SIMULATED PAPER ACCOUNTS — NO REAL ORDERS")
    print("  Press Ctrl+C to stop")
    print("==================================================")
    try:
        server.serve_forever()
        return 0
    except KeyboardInterrupt:
        print("\nStopping dashboard server...")
        server.server_close()
        return 0
