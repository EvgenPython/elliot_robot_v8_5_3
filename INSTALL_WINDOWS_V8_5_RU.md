# Чистая установка WaveFrame Robot V8.5 на Windows-сервер

Инструкция рассчитана на разрешённую владельцем чистую замену V8.4 в выходной
день без открытых позиций и ордеров. Старые `state`, `analysis_archive` и
`logs` намеренно не переносятся: именно в старом состоянии застряла отменённая
pending-заявка. История остаётся в PostgreSQL веб-сервера.

## 1. Остановить старые процессы

PowerShell от имени пользователя, который запускает MT5:

```powershell
$OldRoot = "C:\elliot_robot_v8_4"
$NewRoot = "C:\elliot_robot_v8_5"
$ErrorActionPreference = "Stop"

Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -match "runner\.py|web_publisher\.py" -and $_.Name -match "^python(w)?\.exe$" } | Select-Object ProcessId, Name, CommandLine | Format-List
Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -match "runner\.py|web_publisher\.py" -and $_.Name -match "^python(w)?\.exe$" } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }
```

Проверка должна ничего не вывести:

```powershell
Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -match "runner\.py|web_publisher\.py" -and $_.Name -match "^python(w)?\.exe$" } | Select-Object ProcessId, CommandLine
```

## 2. Получить новый код

После загрузки релиза в указанный GitHub-репозиторий:

```powershell
git clone https://github.com/EvgenPython/elliot_robot_v8_4.git $NewRoot
Set-Location $NewRoot
git log -1 --oneline
Test-Path .\runner.py
```

Последняя команда должна вернуть `True`.

## 3. Создать окружение Python 3.11

```powershell
Set-Location $NewRoot
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -r .\requirements.txt
```

Если `py -3.11` не найден, сначала выполните `py -0p` и используйте путь к уже
установленному 64-bit Python 3.11.

## 4. Перенести только три секретных конфигурации

```powershell
Copy-Item "$OldRoot\config\account.json" "$NewRoot\config\account.json" -Force
Copy-Item "$OldRoot\config\anthropic.json" "$NewRoot\config\anthropic.json" -Force
Copy-Item "$OldRoot\config\web_export.json" "$NewRoot\config\web_export.json" -Force
```

Не копируйте `execution.json`, `position_monitor.json`, `instruments.json`,
`state`, `analysis_archive` и `logs`: используйте новые файлы V8.5 и чистое
состояние.

```powershell
Get-Item .\config\account.json, .\config\anthropic.json, .\config\web_export.json, .\config\execution.json, .\config\position_monitor.json, .\config\instruments.json | Select-Object Name, Length, LastWriteTime
```

## 5. Бесплатные проверки

`check_trading_ready.py` подключается к MT5, но ордер не отправляет.

```powershell
Set-Location $NewRoot
.\.venv\Scripts\python.exe -X utf8 -m unittest discover -p "test_*.py" -v
.\.venv\Scripts\python.exe -X utf8 .\check_trading_ready.py
```

Ожидается `Ran 101 tests` и `OK` у тестов и `[READY]` у проверки MT5. Перед запуском убедитесь,
что `config\execution.json` действительно содержит желаемый режим торговли.

## 6. Проверить связь с вебом одним пакетом

```powershell
.\.venv\Scripts\python.exe -X utf8 -u .\web_publisher.py --once
```

Должно появиться сообщение об отправке runtime state без ошибки HTTP.

## 7. Запустить ровно один runner и один publisher

```powershell
Start-Process powershell.exe -ArgumentList @("-NoExit", "-ExecutionPolicy", "Bypass", "-File", "$NewRoot\run_runner.ps1")
Start-Process cmd.exe -ArgumentList @("/k", "$NewRoot\run_web_publisher.bat")
```

Повторный экземпляр V8.5 для того же инструмента будет отклонён lock.
Старые версии нужно остановить командами выше: их publisher ещё не знает
про новый общий lock. Если раньше настраивал автозапуск V8.4 в планировщике,
отключи старую задачу, чтобы она не запустила старый код после перезагрузки.

Проверка:

```powershell
Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -match "runner\.py|web_publisher\.py" -and $_.Name -match "^python(w)?\.exe$" } | Select-Object ProcessId, Name, CommandLine | Format-List
Get-Content (Get-ChildItem "$NewRoot\logs\runner_*.log" | Sort-Object LastWriteTime -Descending | Select-Object -First 1).FullName -Encoding UTF8 -Tail 60
Get-Content "$NewRoot\logs\web_publisher.log" -Encoding UTF8 -Tail 40
```

В Windows иногда один логический Python-процесс виден как launcher и дочерний
`python.exe`; это допустимо. Недопустимы две отдельные команды runner или две
команды publisher из разных каталогов.

## 8. Аварийная остановка

```powershell
Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -match "runner\.py|web_publisher\.py" -and $_.Name -match "^python(w)?\.exe$" } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }
```
