ROBOT ELLIOT: WINDOWS -> LINUX READ-ONLY EXPORT
================================================

Что изменено
------------
1. Risk Manager, Entry/SL/TP contract и Executor не изменены. Расписание
   анализа переведено на один daily FULL 08:00 FP + event-driven Scout.
2. Linux по-прежнему не вызывает Claude. На Windows один разрешённый FULL
   теперь состоит из сохраняемых FULL_MAP + FULL_DECISION; это повышает
   устойчивость и изолирует платные retries.
3. Итоговый FULL Structured Output сохраняет прежний контракт и visualization:
   - wave_points: точные time/price опорных точек волн;
   - levels: уровни, включая Entry/SL/TP текущей рекомендации;
   - zones: support/resistance/range/liquidity/pattern;
   - scenario_paths: primary/alternate projections.
4. state/web_market_snapshot.json хранит точные уже собранные MT5-свечи
   независимо от результата Claude. Новый H1 market snapshot создаётся до
   Scout/FULL и не делает отдельного запроса к MT5.
5. analysis_archive теперь хранит единый пакет:
   raw candles + Scout + Full + Risk Manager + Trade State + Executor.
6. runner.py каждые 10 секунд обновляет state/runner_status.json.
7. web_publisher.py независимо отправляет данные только Windows -> Linux.
8. Перед каждым Scout/FULL stage платным вызовом создаются:
   - архив исходного payload для веба;
   - durable no-retry запись state/claude_request_guard.json.
   Потеря SSE-ответа, ошибка JSON/валидации или аварийное завершение процесса
   приводят только к controlled retry конкретного stage. Уже validated
   FULL_MAP при сбое FULL_DECISION повторно не вызывается.

Важно
------
- Linux ничего не вызывает на Windows и не управляет роботом.
- Publisher не импортируется торговым кодом, не вызывает Claude и не вызывает MT5.
- Сбой сети/Linux не блокирует торговлю. Неотправленные archives остаются локальной
  очередью и будут повторены после восстановления связи.
- Сбой Claude больше не делает Market пустым: publisher отдельно отправляет
  state/web_market_snapshot.json. Волны и рекомендация всё равно появляются
  только после валидного FULL.
- Не заменяйте рабочие config/*.json и state/*.json файлами из архива обновления.
- state/claude_request_guard.json является рабочим cost-safety состоянием.
  Не удаляйте и не заменяйте его при обычном обновлении.

Восстановление свечей после потерянного ответа Claude
------------------------------------------------------
Если debug/claude_market_payload.json существует, а финальный ответ Claude не
был получен, при остановленном runner выполните:

   python -u recover_web_payload.py

Команда только читает уже сохранённый payload и создаёт FULL_RECOVERY archive.
Она не вызывает Claude, MT5 или сеть. Publisher отправит свечи в веб, а поля
волн/рекомендации останутся честно пустыми.

Настройка Windows
-----------------
1. Сохраните резервную копию текущей папки проекта.
2. Остановите runner и отдельный publisher, если он уже запущен.
3. Обновите Python-файлы проекта. Сохраните без изменений ваши рабочие:
   config/account.json, config/anthropic.json и state/*.json.
4. Скопируйте:

   config\web_export.example.json -> config\web_export.json

5. В web_export.json укажите:
   - enabled: true
   - base_url: HTTPS адрес Linux/Django
   - engine_id: постоянный ID этого Windows robot

6. API token лучше задать в переменной окружения Windows:

   setx ROBOT_WEB_API_TOKEN "TOKEN_КОТОРЫЙ_СОЗДАН_НА_LINUX"

   После setx откройте новую консоль/перезапустите задачу. Token не присылайте
   в чат и не коммитьте. Альтернатива: api_token в локальном web_export.json;
   этот файл исключён из git.

7. Проверка одного прохода:

   python web_publisher.py --once

8. Для постоянной работы запустите run_web_publisher.bat отдельной задачей
   Windows Task Scheduler: At startup, restart on failure. Runner и Publisher
   должны быть двумя независимыми процессами.

Контракт Linux API
------------------
Авторизация всех трёх endpoints:

  Authorization: Bearer <token>
  X-Robot-Engine-ID: <engine_id>
  Content-Type: application/json
  Idempotency-Key: <stable retry key>

POST /api/v1/robot/analysis-events

  {
    "contract_version": 1,
    "kind": "analysis_event",
    "engine_id": "xauusd-windows-01",
    "event_id": "uuid",
    "cycle_id": "uuid",
    "event_revision": 3,
    "content_sha256": "sha256",
    "source_path": "analysis_archive/2026-08-16/...json",
    "sent_at_utc": "ISO-8601",
    "payload": { "complete archive record": "..." }
  }

Linux обязан делать UPSERT по (engine_id, event_id). Более новая revision/hash
заменяет неполную раннюю версию того же event. Повтор того же Idempotency-Key
должен вернуть любой 2xx без создания дубля.

POST /api/v1/robot/market-snapshot

  {
    "contract_version": 1,
    "kind": "market_snapshot",
    "engine_id": "xauusd-windows-01",
    "snapshot_id": "stable uuid",
    "source_snapshot_at_fp": "ISO-8601 UTC+3",
    "content_sha256": "sha256",
    "sent_at_utc": "ISO-8601 UTC",
    "payload": { "exact raw MT5 market payload": "..." }
  }

Linux хранит самый новый market snapshot по engine_id. Этот пакет создаётся
из уже собранного snapshot робота: нового вызова MT5 или Claude нет.

POST /api/v1/robot/runtime-state

  {
    "contract_version": 1,
    "kind": "runtime_state",
    "engine_id": "xauusd-windows-01",
    "runtime_id": "stable uuid",
    "source_generated_at_fp": "ISO-8601 UTC+3",
    "sent_at_utc": "ISO-8601 UTC",
    "missing_sources": [],
    "payload": {
      "runner_status": {},
      "analysis_state": {},
      "trade_state": {},
      "fundingpips_risk_state": {},
      "claude_reference_state": {},
      "claude_request_guard": {}
    }
  }

Linux сохраняет последний runtime-state по engine_id. Если sent_at_utc свежий,
но runner_status.generated_at_fp устарел, Publisher жив, а robot runner завис/
остановлен. Это нужно показывать пользователю как разные статусы.

Какие данные рисовать
---------------------
- История свечей: market snapshot payload.cacheable_history.
  closed_market_history_before_day_start.<TF>.closed_bars.
- Свечи текущих FP-суток: market snapshot payload.live_market.
  raw_timeframes_since_day_start.<TF>.closed_bars_since_day_start и
  current_unclosed_bar.
- Backward-compatible fallback свечей: analysis archive payload.payload.*.
- Волны/уровни/зоны: analysis archive payload.result.visualization.
- Объяснение Claude: analysis archive payload.result.*.
- Scout timeline: payload.scout_result.
- Risk Manager/rejection: payload.risk_report.
- План и исполнение: payload.trade_state_result / payload.execution_report.
- Текущие/закрытые сделки: runtime payload.trade_state.
- Баланс/эквити/MT5 online: runtime payload.runner_status.
- Reliability V3 attempts, unknown billing, SSE recovery и circuit breaker:
  runtime payload.claude_request_guard. Это только read-only telemetry;
  Linux не может запустить retry.

Новый Structured Output будет действовать со следующего FULL-запроса. Старые
архивы останутся валидными, но в них нет visualization с точными координатами.
Если свежий FULL неуспешен, веб показывает свежие свечи и либо честно пустую
разметку, либо явно помеченную последнюю валидную разметку как предыдущую.
