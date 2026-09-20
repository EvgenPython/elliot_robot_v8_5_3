ROBOT ELLIOT — CLAUDE RELIABILITY V3.4, 2026-08-22
===================================================

Что изменено
------------
1. Торговая стратегия НЕ изменена. Один FULL теперь состоит из двух
   связанных MAX-этапов одного frozen snapshot:
   - FULL_MAP: полный независимый D1/H4/H1 режим, price action, структура,
     паттерны, Elliott primary/alternate и сценарии;
   - FULL_DECISION: validated map + H1/M15/M5, затем прежний строгий
     recommendation/Entry/SL/TP contract.
2. Каждый этап сохраняется и повторяется независимо. Если FULL_MAP уже
   validated, а поток FULL_DECISION оборвался, повторяется только
   FULL_DECISION — первый большой этап повторно не оплачивается.
3. Для каждого FULL-этапа: максимум 2 попытки одного frozen snapshot. После
   outcome-unknown разрешён только один повтор с минимальной паузой 90 секунд.
   Даже старый config с 3–5 attempts не может разрешить третью потенциально
   оплаченную генерацию того же этапа.
4. SCOUT: до 2 попыток; если ответа всё равно нет, H1 не пропускается —
   выполняется независимый глубокий FULL.
5. Каждый SSE text delta сохраняется в debug/claude_stream_journal. Если поток
   оборвался после полного JSON, Python проверяет JSON по той же закрытой
   schema и использует его без нового API-запроса. Неполный JSON не дописывается
   и никогда не попадает в торговый pipeline.
6. Retry выполняется при timeout/обрыве SSE и временных 408/409/429/5xx.
   Retry-After учитывается. После второго outcome-unknown stage переходит в
   EXHAUSTED/OUTCOME_UNKNOWN_LIMIT и требует внимания оператора.
7. Полученный, но не прошедший локальную семантическую проверку ответ больше
   НЕ вызывает полный повтор. Разрешён один отдельно журналируемый REPAIR:
   FULL_MAP_REPAIR или FULL_DECISION_REPAIR. REPAIR исправляет известную
   validation_error и не меняет стратегию.
8. 401/403 и неисправимый 400 не повторяются вслепую: тот же запрос не
   исправит API key, права или некорректный вход.
9. Каждая попытка записывается в state/claude_request_guard.json ДО
   Messages API. Сохраняются token count, полный request SHA-256, request ID,
   response ID, usage, ошибка и статус.
10. Journal дополнительно хранит failure_class, billing_status,
    delivery_recovered, stream state/bytes и operator alert. Статус
    UNKNOWN_MAY_BE_BILLED честно отделён от подтверждённого usage.
11. Только один локально валидированный response каждого этапа становится
   winner. Только локально собранный и повторно валидированный полный contract
   может попасть в Risk Manager / Executor.
12. После рестарта продолжается исходный payload из analysis_archive, а не
   новый изменившийся снимок. Validated FULL_MAP восстанавливается без API.
13. Сырые ответы сохраняются в debug/claude_staged_attempts и
    debug/claude_scout_attempts, если финальный Message успел поступить.
14. Свечи отправляются Claude в формате columns + rows. Все времена, OHLC,
    tick volume, spread и real volume сохранены; повторяющиеся JSON-ключи
    удалены только из transport-копии. Web/archive остаются прежнего формата.
15. Базовые аналитические инструкции двух специализированных этапов вместе
    уменьшены с 23 989 до 8 751 символа. С обязательным описанием compact
    wire-формата и completion reserve эффективный объём составляет 12 444
    символа (-48,1%).

Compact wire и compiled grammar
--------------------------------
Anthropic Structured Outputs компилирует JSON Schema до начала генерации.
Канонический ответ с большим количеством вложенных объектов может превысить
внутренний лимит compiled grammar и получить permanent HTTP 400 ещё до
анализа. Reliability V3.2 разделяет два контракта:

  MARKET_MAP_WIRE_SCHEMA / TRADE_DECISION_WIRE_SCHEMA
    — маленький API transport contract;

  MARKET_MAP_SCHEMA / TRADE_DECISION_SCHEMA
    — прежний полный локальный contract робота и веба.

Claude по-прежнему анализирует все переданные raw candles и возвращает все
аналитические разделы, wave_points, levels, zones, scenario_paths и торговые
поля. В API-ответе вложенные записи передаются позиционными массивами строк.
Python проверяет число/порядок ячеек, строго преобразует integer/float/bool/null,
восстанавливает прежние именованные objects и только затем запускает старые
semantic/chart/trading validators. Ошибка wire-конвертации считается известным
INVALID_RESPONSE и может попасть только в отдельный маленький REPAIR, но не в
Risk Manager или Executor.

Размер schema без пробелов:

  FULL_MAP canonical:       5 034 bytes
  FULL_MAP API wire:        1 686 bytes (-66,5%)
  FULL_DECISION canonical:  3 492 bytes
  FULL_DECISION API wire:   1 057 bytes (-69,7%)

Bounded effort Claude Sonnet 5
------------------------------
У Sonnet 5 adaptive thinking и финальный JSON расходуют общий max_tokens.
Первый реальный FULL_MAP использовал 47 998 thinking tokens из старого лимита
48 000 и завершился stop_reason=max_tokens до готового JSON. Второй реальный
тест с effort=max использовал 127 998 thinking tokens из лимита 128 000 и
снова не создал готовый JSON. Увеличение лимита не решает этот сценарий.

Anthropic рекомендует при таком поведении снижать effort. В Reliability V3.3
сохранены Sonnet 5, полные raw-данные, аналитические правила, compact wire и
все локальные validators, но завершённый проверяемый ответ получает приоритет:

  FULL_MAP max_tokens:              64 000, effort=medium
  FULL_DECISION max_tokens:         48 000, effort=medium
  FULL_MAP_REPAIR max_tokens:       32 000, effort=medium
  FULL_DECISION_REPAIR max_tokens:  24 000, effort=medium

max_tokens — верхний предел, а не заранее списываемое количество. Usage и цена
зависят от фактически сгенерированных thinking/output tokens. Полный этап при
известном stop_reason=max_tokens автоматически второй раз не покупается.

Подтверждённые волны
--------------------
Последний успешный FULL остаётся reference-only, а свежие raw свечи — источником
истины. Каждая прежняя confirmed wave point получает стабильный anchor_id.
FULL_MAP обязан явно поместить каждый id ровно в один список:

  preserved_anchor_ids   — координата/label/роль подтверждены свежим анализом;
  invalidated_anchor_ids — структура объективно требует удалить/заменить точку.

Python переносит preserved points без повторной генерации Claude. Новые точки
добавляются как delta. Если разметка сломана, Claude обязан сделать recount:
экономия токенов не имеет права замораживать ошибочную волну.

Дневная политика вызовов
------------------------
Единственный обязательный FULL выполняется после закрытия H1 в 08:00 FP.
На всех остальных новых H1 в рабочем окне сначала работает дешёвый Scout.
Внеплановый FULL запускается только при новом смысловом событии: pullback,
retest, базе/консолидации, reversal, character/structure/wave change,
инвалидации reference, конкретном reference conflict или возможном setup.
Продолжение уже описанного импульса, новый экстремум или ожидаемый пробой
очередного уровня сами по себе FULL не запускают. Low/medium confidence без конкретного
события тоже не является причиной FULL. Cooldown и лимита на число FULL нет.
Ошибка Scout или отсутствие дневного FULL-reference по-прежнему включают fail-open FULL.

Успешный FULL-reference доступен Scout до конца тех же FP-суток (технический
предел 18 часов). На следующий FP-день reference отклоняется.

При managed position Claude полностью выключен: нет Scout, scheduled FULL и
event-driven FULL. После подтверждённого закрытия позиции анализ возобновляется
на последней актуальной закрытой H1. Пропущенные внутри сделки H1 не догоняются.

Политика по умолчанию
---------------------
FULL_MAP attempts:       2
FULL_DECISION attempts:  2
FULL delay:              [90] seconds
Outcome-unknown hard max: 2 attempts total per stage
FULL_MAP_REPAIR:         1 attempt, no retry
FULL_DECISION_REPAIR:    1 attempt, no retry
SCOUT attempts:          2
SCOUT delay:             [30] seconds
SCOUT model default:     claude-haiku-4-5-20251001

Оба FULL-этапа используют существующие настройки full_* в
config/anthropic.json:

  "full_max_attempts": 2,
  "full_retry_delays_seconds": [90],
  "outcome_unknown_min_delay_seconds": 90,
  "repair_max_attempts": 1,
  "scout_model": "claude-haiku-4-5-20251001",
  "scout_max_attempts": 2,
  "scout_retry_delays_seconds": [30]

Известные transient attempts остаются ограничены диапазоном 1..5. Для
outcome-unknown действует отдельный жёсткий предел: максимум две попытки
этапа независимо от config.

Установка на Windows
--------------------
Runner должен быть остановлен.

1. Сделать резервную копию C:\claude_robot.
2. Скопировать содержимое новой сборки в C:\claude_robot с заменой.
3. НЕ заменять и НЕ удалять:
   config\anthropic.json
   config\account.json
   config\web_export*.json
   state\*.json
   analysis_archive\*
4. Проверить в PowerShell:

   Set-Location C:\claude_robot
   .\venv\Scripts\python.exe -m py_compile main.py claude_client.py claude_staged_client.py claude_stream_recovery.py scout_client.py claude_request_guard.py analysis_archive.py analysis_state.py
   .\venv\Scripts\python.exe -m unittest discover -v

Ожидается: Ran 59 tests, OK.

Торговая стратегия
------------------
risk_manager.py, trade_executor.py, trade_state.py, live_executor.py,
pending_executor.py, execution_control.py и runtime_policy.py проверены
byte-for-byte: они не изменены. Правила Entry/SL/TP, FundingPips risk и HOLD
until SL/TP не менялись. По прямому решению владельца изменено только
расписание анализа: один daily FULL 08:00 FP + event-driven Scout escalation.

Важные ограничения
-------------------
Ни один внешний API нельзя гарантировать на 100%. Если исчерпаны все attempts
или ошибка неисправима точным повтором, робот не создаёт сделку без валидного
ответа. Это безопасная остановка, а не NO_TRADE от Claude.

Нельзя гарантировать и снижение цены каждого отдельного FULL до накопления
реальной статистики: два этапа обмениваются validated map. Архив хранит usage
по FULL_MAP, FULL_DECISION и totals, поэтому стоимость сравнивается по фактам.
Главная гарантированная экономия — завершённый этап не оплачивается повторно,
полный JSON после позднего SSE-обрыва восстанавливается локально, а известная
ошибка ответа исправляется коротким REPAIR вместо повторного MAX-этапа.

Prompt cache
------------
Пяти­минутный cache намеренно не включён безусловно: cache write увеличивает
стоимость первой входной обработки, поэтому при редких retry он может стоить
дороже. Его следует включать только после накопления реальной статистики
частоты повторов. Reliability V3 не уменьшает market context ради кеша.
