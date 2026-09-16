@echo off
cd /d "%~dp0"
echo Starting Escanor Telegram Alert Watcher...
start "Escanor-TelegramWatcher" /min python -u -m live_engine.monitoring.telegram_watcher --database data/hype_live.db --symbol HYPEUSDT
echo Telegram Watcher running in background.
pause
