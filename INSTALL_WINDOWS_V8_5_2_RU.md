# Установка WaveFrame Robot V8.5.2 на Windows Server

Команды выполняются в PowerShell из папки проекта.

## 1. Подготовка

```powershell
cd C:\elliot_robot_v8_5_2
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -r .\requirements.txt
```

Перенесите только личные конфиги из прежней версии (не старые `.py` и не
старое виртуальное окружение). Папки `state`, `logs`, `debug` и старый
`claude_request_guard.json` не копируйте: новая установка должна начать с
чистого lifecycle-состояния.

```powershell
Copy-Item C:\elliot_robot_v8_5\config\account.json .\config\account.json -Force
Copy-Item C:\elliot_robot_v8_5\config\anthropic.json .\config\anthropic.json -Force
Copy-Item C:\elliot_robot_v8_5\config\web_export.json .\config\web_export.json -Force
```

Торговое разрешение уже входит в проект и должно остаться ровно таким:

```powershell
Get-Content .\config\execution.json
```

Ожидается:

```json
{
  "trading_enabled": true,
  "allowed_account_modes": ["DEMO"]
}
```

Это не тестовая имитация исполнения: на счёте MT5 с режимом `DEMO` робот после
всех проверок действительно вызывает `mt5.order_send()`. REAL в этом релизе
не разрешён случайно; для будущего проп-счёта режим нужно будет включить явно.

Старый `anthropic.json` можно перенести: V8.5.2 программно не позволит старым
значениям `2/1 attempts` опустить проверенный минимум `5 full / 3 scout /
2 repair`.

## 2. Обязательная проверка до запуска

```powershell
Get-Content .\VERSION
.\.venv\Scripts\python.exe -X utf8 .\verify_release.py
.\.venv\Scripts\python.exe -X utf8 .\check_trading_ready.py
```

Первая команда должна показать `8.5.2`, release gate — `PASS`, а readiness —
`Mode: LIVE`, `Trade mode: DEMO`, `Trading enabled: True` и
`order_send allowed: True`. `check_trading_ready.py` ордеров не отправляет.
Если текущий MT5 вошёл в REAL, readiness обязан показать блокировку — это
ожидаемая защита, а не скрытый dry-run.

## 3. Остановить старые процессы

Сначала посмотреть точные процессы:

```powershell
Get-CimInstance Win32_Process |
  Where-Object { $_.CommandLine -match 'runner.py|web_publisher.py' } |
  Select-Object ProcessId, Name, CommandLine |
  Format-List
```

Затем остановить только показанные PID старой версии:

```powershell
Stop-Process -Id <PID_СТАРОГО_RUNNER> -Force
Stop-Process -Id <PID_СТАРОГО_PUBLISHER> -Force
```

## 4. Запуск

Окно 1 — робот:

```powershell
cd C:\elliot_robot_v8_5_2
.\.venv\Scripts\python.exe -X utf8 -u .\runner.py
```

Окно 2 — передача на веб:

```powershell
cd C:\elliot_robot_v8_5_2
.\.venv\Scripts\python.exe -X utf8 -u .\web_publisher.py
```

Не запускайте второй экземпляр runner или publisher. Робот имеет single-instance
lock; Claude API имеет отдельный общий lock, поэтому параллельные платные
запросы также запрещены. Лишние старые процессы всё равно остановите, чтобы не
смешивать логи разных версий.

## 5. Быстрая диагностика

```powershell
.\.venv\Scripts\python.exe -X utf8 .\show_trade_funnel.py
.\.venv\Scripts\python.exe -X utf8 .\show_trade_statistics.py

$log = Get-ChildItem .\logs\runner_*.log |
  Sort-Object LastWriteTime -Descending |
  Select-Object -First 1
Get-Content $log.FullName -Tail 100
```

При отсутствии сделки funnel должен показать конкретную ступень: Claude,
Risk, Trade State, validation, broker send или reconciliation. Само отсутствие
сделки не является доказательством бага, но отсутствие точной причины — уже
ошибка диагностики.

Проверить M30 decision и Claude recovery:

```powershell
Select-String -Path .\logs\runner_*.log -Pattern `
  'M30 DECISION','REFUSAL_RESPONSE','RECOVERY','CLAUDE WINNER','order_send' |
  Select-Object -Last 100
```
