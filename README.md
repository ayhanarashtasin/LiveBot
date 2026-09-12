# LiveBot (Escanor Live Trading Engine)

Institutional-grade quantitative trading engine for **Binance USD-M Futures** (`futures/um`).

## Features
- **Deterministic Zero-Divergence Execution**: 100% decision parity between historical benchmarks and live engine execution.
- **34 Pre-trade Safety Gates**: Exhaustive risk evaluation before live order submission.
- **Durable Event Sourcing**: High-performance SQLite WAL event store for fills, orders, positions, and audits.
- **Resilient Market Data**: Real-time aggTrade WebSocket feed with candle reconstruction and REST gap recovery.
- **Multi-Coin Support**: Pre-configured and validated for `HYPEUSDT`, `BTCUSDT`, `LITUSDT`, and `ZECUSDT`.

## Quick Start on VPS

### 1. System Dependencies (Linux)
```bash
sudo apt-get update && sudo apt-get install -y build-essential python3 python3-pip python3-venv wget git

# Install TA-Lib C library
wget http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz
tar -xzf ta-lib-0.4.0-src.tar.gz
cd ta-lib/
./configure --prefix=/usr
make
sudo make install
cd .. && rm -rf ta-lib ta-lib-0.4.0-src.tar.gz
```

### 2. Environment Setup
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -r requirements-dev.txt
cp .env.example .env
```

### 3. Verify Connectivity
```bash
python scripts/verify_binance_connectivity.py
```

### 4. Launching the Engine
- **HYPE Paper Trading**:
  ```bash
  python -m live_engine.main --config config/hype-paper.json
  ```
- **HYPE Dashboard**:
  ```bash
  python -m live_engine.dashboards.hype_dashboard --config config/hype-paper.json --port 8083 --host 0.0.0.0
  ```
