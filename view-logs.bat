@echo off
cd /d "%~dp0"
echo Streaming live HYPEUSDT engine logs (Press Ctrl+C to stop viewing)...
powershell.exe -NoProfile -Command "Get-Content -Wait -Tail 50 logs\hype-live.stdout.log"
pause
