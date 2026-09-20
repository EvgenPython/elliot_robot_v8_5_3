@echo off
setlocal
chcp 65001 >nul
cd /d "%~dp0"

if not exist "logs" mkdir "logs"

set "ROBOT_PYTHON=python"
if exist ".venv\Scripts\python.exe" set "ROBOT_PYTHON=.venv\Scripts\python.exe"
if exist "venv\Scripts\python.exe" set "ROBOT_PYTHON=venv\Scripts\python.exe"

"%ROBOT_PYTHON%" -X utf8 -u web_publisher.py >> "logs\web_publisher.log" 2>&1

endlocal
