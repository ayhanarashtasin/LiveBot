@echo off
cd /d "%~dp0"
echo ===================================================
echo   ESCANOR HYPEUSDT LIVE TRADING + TELEGRAM ALERTS
echo ===================================================

echo 1. Starting Telegram Alert Watcher...
start "Escanor-TelegramWatcher" python -u -m live_engine.monitoring.telegram_watcher --database data/hype_live.db --symbol HYPEUSDT

echo 2. Starting Web Dashboard (http://127.0.0.1:8083)...
start "Escanor-Dashboard" python -u -m live_engine.dashboards.hype_dashboard --config config/hype-live.json --host 127.0.0.1 --port 8083

echo 3. Starting Live Trading Engine in this window...
powershell.exe -NoProfile -ExecutionPolicy Bypass -File ".\scripts\run-hype.ps1" -Mode LIVE -Force

pause
