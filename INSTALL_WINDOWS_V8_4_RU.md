# Установка WaveFrame Robot V8.4 на Windows-сервер

## Важно перед началом

V8.3 и V8.4 нельзя запускать одновременно. Если сейчас открыта сделка, сначала
остановите старый runner через `Ctrl+C`, затем обязательно перенесите папку
`state`: в ней хранится связь робота с ticket открытой позиции.

Архив V8.4 не содержит пароль MT5, Anthropic API key и токен веба.

## 1. Распаковать проект

Создайте папку `C:\elliot_robot_v8_4` и распакуйте в неё содержимое ZIP.
Проверка в PowerShell:

```powershell
Test-Path C:\elliot_robot_v8_4\runner.py
```

Должно вернуться `True`. Если `False`, проект распакован с лишней вложенной
папкой.

## 2. Создать Python-окружение

```powershell
cd C:\elliot_robot_v8_4
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -r .\requirements.txt
```

Если команда `py -3.12` отсутствует, используйте `python -m venv .venv`.

## 3. Перенести локальные настройки V8.3

```powershell
Copy-Item C:\elliot_robot_v8_3\config\account.json C:\elliot_robot_v8_4\config\account.json -Force
Copy-Item C:\elliot_robot_v8_3\config\anthropic.json C:\elliot_robot_v8_4\config\anthropic.json -Force
Copy-Item C:\elliot_robot_v8_3\config\web_export.json C:\elliot_robot_v8_4\config\web_export.json -Force
```

Не копируйте поверх V8.4 файлы `execution.json` и `position_monitor.json`:
в них находятся новые правила исполнения и сопровождения.

## 4. Перенести состояние и архив

Старый runner на этом шаге уже должен быть остановлен.

```powershell
New-Item -ItemType Directory C:\elliot_robot_v8_4\state -Force | Out-Null
Copy-Item C:\elliot_robot_v8_3\state\* C:\elliot_robot_v8_4\state\ -Recurse -Force

New-Item -ItemType Directory C:\elliot_robot_v8_4\analysis_archive -Force | Out-Null
Copy-Item C:\elliot_robot_v8_3\analysis_archive\* C:\elliot_robot_v8_4\analysis_archive\ -Recurse -Force
```

Если `analysis_archive` в V8.3 отсутствует, вторую пару команд можно пропустить.

## 5. Выполнить бесплатные проверки

```powershell
cd C:\elliot_robot_v8_4
.\.venv\Scripts\python.exe -X utf8 -m unittest discover -v
.\.venv\Scripts\python.exe -X utf8 .\check_trading_ready.py
```

Проверка не открывает сделку и не изменяет SL/TP. В конце должны быть:

```text
order_send allowed:   True
Automatic SL/TP:      True
Stop risk rule:       NEVER WIDEN
[READY]
```

## 6. Запустить робот

```powershell
cd C:\elliot_robot_v8_4
powershell -ExecutionPolicy Bypass -File .\run_runner.ps1
```

Окно runner не закрывайте. При существующей позиции первый глубокий review
будет выполнен после получения свежего рынка. SL/TP изменятся только если
структура подтверждена с высокой уверенностью и все защитные проверки пройдены.

## 7. Запустить передачу на веб

В отдельном PowerShell:

```powershell
cd C:\elliot_robot_v8_4
.\run_web_publisher.bat
```

## Быстро отключить только автоматическое сопровождение

Остановите runner, замените в `config\position_monitor.json`:

```json
"automatic_trade_changes": false
```

После этого снова запустите runner. Анализ H4/H1/M15/M5 продолжится, но запросы
на изменение SL/TP отправляться не будут.
