@echo off
REM Daily backtest data update launcher
REM Scheduled via Windows Task Scheduler at 06:30 JST

cd /d "%~dp0"

set LOGFILE=logs\update_%date:~0,4%%date:~5,2%%date:~8,2%.log

echo [%date% %time%] Starting update >> %LOGFILE%
python update_backdata.py >> %LOGFILE% 2>&1
echo [%date% %time%] Finished (exit %errorlevel%) >> %LOGFILE%
