# Robot Elliot Windows V8.3 — универсальное MT5-исполнение

## Главное изменение

Скрытая блокировка `DRY_RUN` удалена. Режим торговли задаётся в
`config/execution.json` и не зависит от названия брокера или MT5-сервера.

Поставка настроена так:

```json
{
  "trading_enabled": true,
  "allowed_account_modes": ["DEMO", "CONTEST", "REAL"]
}
```

Перед `mt5.order_send()` по-прежнему обязательны все проверки торгового плана,
Risk Manager, дневных лимитов, MT5 `order_check`, состояния терминала и защита
от повторной отправки.

## Безопасная проверка на сервере

Она не создаёт и не отправляет ордер:

```powershell
.\.venv\Scripts\python.exe -X utf8 .\check_trading_ready.py
```

Ожидаемый итог при готовом терминале:

```text
[READY] Робот готов отправлять ордера после сигнала и всех risk-gates.
```

## Аварийное отключение торговли

Остановите runner и замените в `config/execution.json`:

```json
"trading_enabled": false
```

После запуска робот продолжит анализ, но `order_send()` будет запрещён.
