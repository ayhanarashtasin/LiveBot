# LiveBot (Escanor Live Trading Engine)

Institutional-grade quantitative trading engine for **Binance USD-M Futures** (`futures/um`).

## Features
- **Deterministic Zero-Divergence Execution**: 100% decision parity between historical benchmarks and live engine execution.
- **34 Pre-trade Safety Gates**: Exhaustive risk evaluation before live order submission.
- **Durable Event Sourcing**: High-performance SQLite WAL event store for fills, orders, positions, and audits.
- **Resilient Market Data**: Real-time aggTrade WebSocket feed with candle reconstruction and REST gap recovery.
- **Multi-Coin Support**: Pre-configured and validated for `HYPEUSDT`, `BTCUSDT`, `LITUSDT`, and `ZECUSDT`.

---

## Windows VPS Setup Guide

### 1. Prerequisites
- **Git for Windows**: Download and install from [git-scm.com](https://git-scm.com/) (or run `winget install Git.Git` in PowerShell).
- **Python 3.11, 3.12, or 3.13** (64-bit): During installation, check the box **"Add python.exe to PATH"**.

### 2. Clone Repository
Open PowerShell and run:
```powershell
git clone https://github.com/ayhanarashtasin/LiveBot.git
cd LiveBot
```

### 3. Install TA-Lib & Dependencies
Install the `TA-Lib` Python wheel followed by the project requirements:
```powershell
python -m pip install --upgrade pip
python -m pip install ta-lib
python -m pip install -r requirements.txt
python -m pip install -r requirements-dev.txt
```
*(Note: If `pip install ta-lib` asks for C++ tools, download the prebuilt wheel matching your Python version from [cgohlke/talib-build/releases](https://github.com/cgohlke/talib-build/releases) and install via `pip install <wheel_file>.whl`)*

### 4. Configure Credentials
```powershell
Copy-Item .env.example .env
notepad .env
```
Fill in your `BINANCE_API_KEY`, `BINANCE_API_SECRET`, and optional testnet keys, then save and close notepad.

### 5. Verify Connectivity & Whitelist VPS IP
```powershell
python scripts/verify_binance_connectivity.py
```
This prints your Windows VPS static IP, checks Binance server clock synchronization, and verifies API permissions. If needed, copy the displayed IP into Binance API Management -> "Restrict access to trusted IPs only".

### 6. Launching on Windows VPS

- **Launch HYPE in PAPER mode (Engine + Dashboard on port 8083)**:
  ```powershell
  .\scripts\run-hype.ps1 -Mode PAPER
  ```
- **Launch HYPE in TESTNET mode**:
  ```powershell
  .\scripts\run-hype.ps1 -Mode TESTNET
  ```
- **Supervise Running Processes**:
  ```powershell
  .\scripts\escanor-control.ps1 -Supervise
  ```
- **Stop Gracefully**:
  ```powershell
  .\scripts\escanor-control.ps1 -Stop -Manifest logs\hype-processes.json
  ```

---

## Linux VPS Setup Guide

### 1. System Dependencies & TA-Lib C Library
```bash
sudo apt-get update && sudo apt-get install -y build-essential python3 python3-pip python3-venv wget git

# Build and install TA-Lib C library
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
git clone https://github.com/ayhanarashtasin/LiveBot.git
cd LiveBot
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -r requirements-dev.txt
cp .env.example .env
nano .env
```

### 3. Verify Connectivity & Launch
```bash
python scripts/verify_binance_connectivity.py
python -m live_engine.main --config config/hype-paper.json
```
