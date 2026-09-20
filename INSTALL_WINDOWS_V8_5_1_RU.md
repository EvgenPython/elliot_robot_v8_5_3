# Установка WaveFrame Robot V8.5.1

Команды выполняются по одной в Windows PowerShell. Текущая версия остаётся в
`C:\elliot_robot_v8_5`, новая устанавливается рядом в
`C:\elliot_robot_v8_5_1`. MT5 должен быть открыт и авторизован под тем же
Windows-пользователем.

## 1. Загрузить архив в GitHub с локального ПК

Скачайте `elliot_robot_v8_5_1.zip` в папку Downloads.

```powershell
Expand-Archive -LiteralPath "$env:USERPROFILE\Downloads\elliot_robot_v8_5_1.zip" -DestinationPath D:\ -Force
$Release = "D:\elliot_robot_v8_5_1"
$Repo = "D:\git\elliot_robot_v8_5_1"
Test-Path "$Release\runner.py"
git clone https://github.com/EvgenPython/elliot_robot_v8_5.git $Repo
robocopy $Release $Repo /E /XD .git .venv venv state logs debug analysis_archive diagnostics /XF account.json anthropic.json web_export.json
if ($LASTEXITCODE -ge 8) { throw "Robocopy завершился с ошибкой $LASTEXITCODE" }
Set-Location $Repo
Get-Content .\VERSION
git status --short
git ls-files config/account.json config/anthropic.json config/web_export.json state logs analysis_archive
```

Версия должна быть `8.5.1`. Последняя команда не должна вывести секретные или
runtime-файлы.

```powershell
git add -A
git commit -m "Release WaveFrame Robot V8.5.1"
git push origin main
git log -1 --oneline
```

## 2. Проверить текущий MT5 на Windows-сервере

Подключитесь к Windows-серверу и откройте PowerShell:

```powershell
$OldRoot = "C:\elliot_robot_v8_5"
$NewRoot = "C:\elliot_robot_v8_5_1"
Set-Location $OldRoot
.\.venv\Scripts\python.exe -X utf8 -c "import MetaTrader5 as m; from mt5_client import connect_mt5; connect_mt5(); print('POSITIONS:', m.positions_get()); print('ORDERS:', m.orders_get()); m.shutdown()"
```

Для спокойного обновления обе строки должны содержать `()`. Если там есть
позиция или pending-ордер, не продолжайте замену до его штатного завершения.

## 3. Остановить старый runner и publisher

```powershell
Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -match "runner\.py|web_publisher\.py" -and $_.Name -match "^python(w)?\.exe$" } | Select-Object ProcessId, Name, CommandLine | Format-List
Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -match "runner\.py|web_publisher\.py" -and $_.Name -match "^python(w)?\.exe$" } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }
Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -match "runner\.py|web_publisher\.py" -and $_.Name -match "^python(w)?\.exe$" }
```

Последняя команда должна ничего не вывести.

## 4. Скачать новую версию

```powershell
if (Test-Path $NewRoot) { throw "$NewRoot уже существует; переименуйте или удалите незавершённую копию" }
git clone https://github.com/EvgenPython/elliot_robot_v8_5.git $NewRoot
Set-Location $NewRoot
Get-Content .\VERSION
Test-Path .\runner.py
```

Должны появиться `8.5.1` и `True`.

## 5. Перенести конфигурацию и рабочее состояние

```powershell
Copy-Item "$OldRoot\config\account.json" "$NewRoot\config\account.json" -Force
Copy-Item "$OldRoot\config\anthropic.json" "$NewRoot\config\anthropic.json" -Force
Copy-Item "$OldRoot\config\web_export.json" "$NewRoot\config\web_export.json" -Force
if (Test-Path "$OldRoot\state") { Copy-Item "$OldRoot\state" "$NewRoot\state" -Recurse -Force }
if (Test-Path "$OldRoot\analysis_archive") { Copy-Item "$OldRoot\analysis_archive" "$NewRoot\analysis_archive" -Recurse -Force }
```

Состояние переносится, чтобы не потерять журнал уже отправленных запросов,
execution intent и отметки publisher. Старые логи остаются в старой папке.

## 6. Создать Python-окружение и проверить проект

```powershell
Set-Location $NewRoot
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -r .\requirements.txt
.\.venv\Scripts\python.exe -X utf8 .\run_tests.py
.\.venv\Scripts\python.exe -X utf8 .\check_trading_ready.py
```

Ожидается `Ran 120 tests`, `OK` и `order_send allowed: True`. Тесты блокируют
сеть и любые немокированные операции MT5; реальный ордер они не отправляют.

## 7. Проверить связь с вебом

```powershell
.\.venv\Scripts\python.exe -X utf8 -u .\web_publisher.py --once
```

Должно появиться сообщение `Runtime state отправлен` без HTTP-ошибки.

## 8. Запустить новую версию

```powershell
Start-Process powershell.exe -ArgumentList @("-NoExit", "-ExecutionPolicy", "Bypass", "-File", "$NewRoot\run_runner.ps1")
Start-Process cmd.exe -ArgumentList @("/k", "$NewRoot\run_web_publisher.bat")
```

Проверка процессов и журнала:

```powershell
Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -match "runner\.py|web_publisher\.py" -and $_.Name -match "^python(w)?\.exe$" } | Select-Object ProcessId, Name, CommandLine | Format-List
Get-Content (Get-ChildItem "$NewRoot\logs\runner_*.log" | Sort-Object LastWriteTime -Descending | Select-Object -First 1).FullName -Encoding UTF8 -Tail 80
```

После первой новой закрытой H1 в журнале должен появиться либо FULL, либо
`H1 DECISION REFRESH`, а затем `TRADE FUNNEL — ИТОГ H1`.

## 9. Посмотреть причины отсутствия сделок

```powershell
Set-Location $NewRoot
.\.venv\Scripts\python.exe -X utf8 .\show_trade_funnel.py --days 7
```

Для передачи полного отчёта на разбор:

```powershell
.\.venv\Scripts\python.exe -X utf8 .\export_diagnostics.py --from 2026-09-09 --to 2026-09-16
```

Путь к готовому ZIP будет напечатан в строке `ZIP:`.
