@echo off
cd /d "%~dp0"
echo Starting Escanor HYPEUSDT Live Trading Engine...
powershell.exe -NoProfile -ExecutionPolicy Bypass -File ".\scripts\run-hype.ps1" -Mode LIVE %*
pause
