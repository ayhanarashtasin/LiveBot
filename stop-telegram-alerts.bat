@echo off
cd /d "%~dp0"
echo Stopping Escanor Telegram Alert Watcher...
powershell.exe -NoProfile -Command "Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -like '*telegram_watcher*' } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force; Write-Host ('Stopped Telegram Watcher PID ' + $_.ProcessId) }"
echo Telegram Watcher stopped.
pause
