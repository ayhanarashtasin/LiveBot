@echo off
cd /d "%~dp0"
echo ===================================================
echo   ESCANOR HYPEUSDT LIVE TRADING + TELEGRAM ALERTS
echo ===================================================

echo 1. Starting Telegram Alert Watcher...
start "Escanor-TelegramWatcher" python -u -m live_engine.monitoring.telegram_watcher --database data/hype_live.db --symbol HYPEUSDT

rem Web Dashboard (http://127.0.0.1:8083) is managed and started automatically by run-hype.ps1

echo 3. Starting Live Trading Engine in this window...
powershell.exe -NoProfile -ExecutionPolicy Bypass -File ".\scripts\run-hype.ps1" -Mode LIVE -Force

pause
