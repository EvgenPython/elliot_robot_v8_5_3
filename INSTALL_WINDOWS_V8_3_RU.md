# Установка Robot Elliot Windows V8.3

## Что важно

V8.3 не привязан к AMarkets или другому брокеру. Он работает через установленный
MetaTrader 5 и использует счёт из `config/account.json`.

Торговля включается в `config/execution.json`:

```json
{
  "trading_enabled": true,
  "allowed_account_modes": ["DEMO", "CONTEST", "REAL"]
}
```

## Обновление Windows-сервера

Сначала остановите текущий runner сочетанием `Ctrl+C`.

Перейдите в каталог проекта и получите обновление из своего GitHub:

```powershell
cd C:\elliot_robot_v8_2
git pull origin main
```

Установите зависимости и запустите все бесплатные тесты:

```powershell
.\.venv\Scripts\python.exe -m pip install -r .\requirements.txt
.\.venv\Scripts\python.exe -X utf8 -m unittest discover
```

Проверьте готовность MT5. Команда не открывает сделку:

```powershell
.\.venv\Scripts\python.exe -X utf8 .\check_trading_ready.py
```

Ожидаемый результат:

```text
Mode:                 LIVE
Trading enabled:      True
order_send allowed:   True
[READY] Робот готов отправлять ордера после сигнала и всех risk-gates.
```

После этого запустите runner:

```powershell
powershell -ExecutionPolicy Bypass -File .\run_runner.ps1
```

Не закрывайте окно runner. Реальный ордер появится только после торгового
решения, одобрения Risk Manager и прохождения всех проверок MT5.

## Быстрое отключение торговли

Остановите runner, установите `"trading_enabled": false` в
`config/execution.json` и снова запустите runner.
