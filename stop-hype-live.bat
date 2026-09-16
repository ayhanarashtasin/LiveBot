@echo off
cd /d "%~dp0"
echo Stopping Escanor HYPEUSDT Live Trading Engine...
powershell.exe -NoProfile -ExecutionPolicy Bypass -File ".\scripts\escanor-control.ps1" -Stop -Manifest "logs\hype-processes.json" %*
pause
