# WaveFrame Robot V8.5.3 — Resilient Claude Pipeline

## Зачем эта версия

V8.5.2 18.09.2026 отправлял один большой `FULL_MAP` Structured Output.
Anthropic отклонял его до анализа рынка:

```text
HTTP 400 invalid_request_error
The compiled grammar is too large
```

V8.5.3 убирает эту единую точку отказа. Анализ строится строго
последовательными маленькими этапами, а каждый валидный кусок сразу сохраняется.

## Главный принцип

```text
получили -> проверили -> сохранили -> нашли недостающие поля
-> запросили только недостающее -> объединили -> продолжили
```

Если этап должен вернуть 5 полей и полностью пришли только 3, следующий платный
запрос содержит schema **только для оставшихся 2 полей**. Уже валидные 3 поля
повторно не запрашиваются.

При перезапуске Windows pipeline читает durable state и продолжает с первого
незавершённого микроэтапа/поля, а не начинает анализ заново.

## Market Map — 12 последовательных этапов

Raw MT5 candles специально не отправляются в каждом этапе. Они читаются Claude
только в первых трёх глубоких сканах — один раз на timeframe; последующие этапы
работают с уже сохранёнными результатами.

1. `MM_D1` — один полный raw scan D1.
2. `MM_H4` — один полный raw scan H4 + сохранённый D1 context.
3. `MM_H1` — один полный raw scan H1 + сохранённые D1/H4 results.
4. `MM_CONTEXT` — market regime, D1/H4/H1 relationship, HTF context.
5. `MM_STRUCTURE` — price structure + patterns.
6. `MM_ELLIOTT` — primary/alternate Elliott count + invalidation.
7. `MM_SCENARIOS` — primary/alternate scenario, expected path, opportunities.
8. `MM_WAVE_REVISION` — preserved/invalidated previous anchors.
9. `MM_VIS_WAVES` — waves/structures/projections.
10. `MM_VIS_LEVELS_EVENTS` — levels, zones/FVG, events, scenario paths.
11. `MM_VIS_GEOMETRY` — trendlines, channels, pattern geometry.
12. `MM_FINAL_META` — chart comment + data quality.

После 12 этапов Python собирает **тот же canonical market-map contract**, который
понимает существующий V8.5.2 downstream-код, и прогоняет старую локальную
semantic/coordinate validation.

## Trade Decision — 7 последовательных этапов

Свежие H1/M30/M15/M5 raw candles отправляются только один раз — в `TD_CONTEXT`.
Полный market map также передаётся только этому первому execution stage.
Остальные этапы используют уже сохранённый execution context.

1. `TD_CONTEXT` — свежие H1/M30/M15/M5, entry/stop/target candidates, FVG context.
2. `TD_DECISION` — action, setup, order type, Entry/SL/TP/invalidation.
3. `TD_REASONING` — rationale, stop/target basis, invalidation, FVG role.
4. `TD_VIS_WAVES`.
5. `TD_VIS_LEVELS_EVENTS`.
6. `TD_VIS_GEOMETRY`.
7. `TD_FINAL_META` — chart comment + execution data quality.

После этого Python собирает прежний trade-decision contract. Только полностью
валидированный итог может попасть в Risk Manager и Executor.

## Восстановление оборванного ответа

`claude_stream_recovery.py` в V8.5.3 дополнительно сохраняет полностью закрытые
и локально валидные top-level JSON fields прямо во время SSE stream.

Например Claude успел передать:

```text
primary_scenario      OK
alternate_scenario    OK
expected_path         OK
invalidation          stream оборвался
next_opportunity      не пришёл
```

Первые три значения остаются в durable state. Следующий запрос создаётся только
для реально отсутствующих полей.

## Ошибки Anthropic

### Временные

Timeout, connection error, overload/5xx, обычный rate limit и другие retryable
ошибки получают bounded backoff. Повторяется только текущий маленький stage и
только его недостающие поля.

### Outcome unknown

Если stream оборвался после возможного начала тарифицируемой генерации, число
повторов жёстко ограничено. Это защита от повторной оплаты одного и того же
неизвестного результата.

### Невалидный/частичный ответ

Валидные поля сохраняются. Repair schema содержит только отсутствующие поля.
Число таких repair-попыток также ограничено; бесконечного платного цикла нет.

### Постоянная ошибка

При 400/401/402/403/404/413 и других классифицированных permanent errors
включается **глобальный circuit breaker**.

```text
PERMANENT_BLOCKED
ИИ-анализ не проводится.
Новые платные запросы автоматически остановлены.
```

Следующая H1 не обходит эту защиту новым cycle key. Перезапуск Windows тоже не
снимает блокировку.

После того как причина реально исправлена, breaker снимается явно:

```powershell
.\.venv\Scripts\python.exe -X utf8 .\reset_ai_breaker.py --yes --reason "исправлена причина permanent error"
```

Сохранённые микроэтапы при этом не удаляются.

## Что НЕ меняется

V8.5.3 не переписывает:

- Risk Manager;
- Trade State;
- live/pending Executor;
- FundingPips risk logic;
- правило настоящего `mt5.order_send()` на DEMO;
- блокировку REAL/CONTEST;
- H1/M30 scheduling;
- market data acquisition.

До отдельного lifecycle-теста исполнение не считается доказанным только потому,
что новый Claude pipeline прошёл unit/static tests.

## Web status

Windows публикует новый источник:

```text
claude_resilient_pipeline.json
```

В пакет входит `patch_web_v8_7_for_v853.py`. Он обновляет локальную копию
`waveframe_web_v8_7`, чтобы сайт явно показывал:

- `ИИ-анализ: ВОССТАНОВЛЕНИЕ`;
- `ИИ-анализ: ВРЕМЕННО НЕДОСТУПЕН`;
- `ИИ-анализ: НЕ ПРОВОДИТСЯ` при `PERMANENT_BLOCKED`.

Старую валидную рекомендацию можно показывать только как историческую/предыдущую.

## Установка на Windows Server

1. Распаковать upgrade-пакет, например в:

```text
C:\v853_upgrade
```

2. Выполнить:

```powershell
cd C:\v853_upgrade
py -3.11 .\upgrade_to_v8_5_3.py
```

Скрипт создаст новый проект:

```text
C:\elliot_robot_v8_5_3
```

`C:\elliot_robot_v8_5_2` не изменяется.

3. Создать новое Python environment и выполнить проверки:

```powershell
cd C:\elliot_robot_v8_5_3
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -U pip
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -X utf8 .\test_v853_resilience.py
.\.venv\Scripts\python.exe -X utf8 .\verify_v853_resilience.py
```

Можно использовать `setup_v853.ps1` как сокращённый вариант этих команд.

4. **Не переключать Scheduled Tasks и не отправлять тестовый DEMO-ордер**, пока
мы отдельно не проверим новый Claude lifecycle на сервере.

## State / secrets

Старые `state`, `analysis_archive`, `logs`, `debug` специально не копируются в
новый релиз, чтобы V8.5.2 permanent/exhausted state не отравлял V8.5.3.

Приватные локальные config-файлы копируются только локально
`C:\elliot_robot_v8_5_2 -> C:\elliot_robot_v8_5_3` на Windows Server. Они не
входят в этот ZIP и не загружаются в GitHub.

## Дополнение: ENTRY_CHECK, Scout и открытая позиция

V8.5.3 также переводит `ENTRY_CHECK` на тот же 7-stage trade-decision pipeline.
Его durable identity строится из source H1 + projection_id + trigger closed bar,
поэтому retry не может случайно переиспользовать решение другого trigger.

Глубокий `POSITION_REVIEW` разбит на отдельный последовательный micro-pipeline:

1. `PR_HTF` — D1/H4/H1 review открытой позиции.
2. `PR_LTF` — M30/M15/M5 child structure.
3. `PR_STATUS` — состояние исходной гипотезы.
4. `PR_MANAGEMENT_CORE` — тип protection plan.
5. `PR_MANAGEMENT_STOP` — доказательство stop anchor.
6. `PR_MANAGEMENT_TARGET` — Fibonacci target evidence.
7. `PR_VIS_WAVES`.
8. `PR_VIS_LEVELS_EVENTS`.
9. `PR_VIS_GEOMETRY`.
10. `PR_FINAL_META`.

Scout остаётся отдельным маленьким дешёвым stage: его schema уже компактна и
не является источником ошибки `compiled grammar is too large`. Но он также
подчиняется глобальному circuit breaker. Любой permanent error Scout включает
`PERMANENT_BLOCKED` и блокирует последующие платные вызовы.

## Самый простой способ собрать новый проект

После распаковки ZIP в `C:\v853_upgrade` можно выполнить один скрипт:

```powershell
cd C:\v853_upgrade
.\build_v853.ps1
```

Он:

- проверит, что источник — `C:\elliot_robot_v8_5_2` и `VERSION=8.5.2`;
- создаст отдельный `C:\elliot_robot_v8_5_3`;
- не изменит старый V8.5.2;
- не перенесёт старые `state/logs/debug/analysis_archive`;
- перенесёт локальные private config только между локальными папками сервера;
- создаст новый `.venv` Python 3.11;
- установит requirements;
- выполнит resilience/static verification.

Скрипт **не переключает Scheduled Tasks, не отправляет платный smoke-запрос
Claude и не вызывает `mt5.order_send()`**.
