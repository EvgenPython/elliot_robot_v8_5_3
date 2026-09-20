# Установка Robot Elliot V8 на Windows

## В PyCharm перед GitHub

Откройте папку `robot_elliot_windows_v8` как проект. Не добавляйте в GitHub
локальные `config/account.json`, `config/anthropic.json`, `config/web_export.json`,
папки `state`, `debug`, `logs` и `analysis_archive`.

```powershell
cd D:\robot_elliot_windows_v8
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -X utf8 -m unittest discover
```

## После клонирования на сервер

Остановите старый runner через `Ctrl+C`, затем:

```powershell
cd C:\elliot_robot
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -X utf8 -m unittest discover
```

Скопируйте локальные конфиги из резервной папки и проверьте режим:

```powershell
Get-Item .\config\account.json, .\config\anthropic.json
Select-String -Path .\execution_control.py -Pattern "^EXECUTION_MODE =","^DEMO_LIVE_ARMED ="
```

Первый запуск рекомендуется сделать в `DRY_RUN`. Компактная консоль с записью
журнала:

```powershell
powershell -ExecutionPolicy Bypass -File .\run_runner.ps1
```

Старый полный консольный вывод:

```powershell
$env:ROBOT_CONSOLE_DETAIL = "full"
.\.venv\Scripts\python.exe -X utf8 -u runner.py
```

Сводка сохранённого лога:

```powershell
.\.venv\Scripts\python.exe -X utf8 .\inspect_runtime_log.py .\logs\runner_YYYYMMDD_HHMMSS.log
```
