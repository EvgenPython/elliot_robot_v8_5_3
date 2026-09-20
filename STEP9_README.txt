CLAUDE ROBOT — STEP 9
FULL INDEPENDENT RAW ANALYSIS + PROMPT CACHE
============================================================

СТАТУС: ИСТОРИЧЕСКИЙ ДОКУМЕНТ, НЕ ТЕКУЩАЯ ПОЛИТИКА.
Актуальная политика описана в STEP10_README.txt и
DAILY_FULL_POLICY_README.txt: один FULL в 08:00 FP, Scout на остальных H1,
Claude OFF при позиции, Sonnet 5 effort=medium. Указанные ниже effort=max,
96000 tokens, hourly FULL и prompt cache больше не используются.

ЦЕЛЬ
------------------------------------------------------------
Сохранить максимальное качество анализа Claude и убрать риск того,
что предыдущая гипотеза подменяет свежий анализ рынка.

КЛЮЧЕВАЯ ПОЛИТИКА
------------------------------------------------------------
1. Каждый разрешённый H1 Claude анализируется ЗАНОВО.
2. Источник истины — полный сырой MT5 market context D1/H4/H1/M15/M5.
3. Python НЕ рассчитывает для Claude:
   - EMA / ATR / RSI / MACD / ADX;
   - swing high / swing low;
   - support / resistance;
   - patterns;
   - Elliott waves;
   - market regime.
4. Python только доставляет raw bars и разделяет их по времени для cache.
5. Previous Claude analysis используется только как reference-only.
6. Если свежие raw data противоречат previous analysis, Claude обязан
   отбросить предыдущую гипотезу.

PROMPT CACHE
------------------------------------------------------------
В течение одного FP-day raw history ДО 00:00 остаётся одинаковой.
Она отправляется отдельным content block с explicit Anthropic 1h cache.

Каждая H1 получает:

    SYSTEM PROMPT
    +
    RAW HISTORICAL BASE до 00:00     <- cacheable
    +
    ВСЕ RAW БАРЫ текущих суток       <- fresh
    +
    текущие незакрытые D1/H4/H1/M15/M5
    +
    Bid / Ask / Spread
    +
    previous analysis reference      <- reference only, если свежий

Если cache hit не случился, качество НЕ меняется: Anthropic просто
заново обрабатывает тот же полный raw context.

CLAUDE POLICY
------------------------------------------------------------
Model:       claude-sonnet-5
Effort:      max
Max tokens:  96000
Transport:   SSE streaming
Retries:     0
Cache TTL:   1h

Финальный Structured Output должен быть концентрированным и без повторов,
но внутренний анализ Claude не ограничивается по глубине инструкцией.

НОВЫЙ STATE FILE
------------------------------------------------------------
state/claude_reference_state.json

Создаётся автоматически только после УСПЕШНО завершённого Claude-analysis.
Хранит один последний анализ как reference для следующей H1.
Не заменяет raw market data.

ВАЖНО ПРИ ОБНОВЛЕНИИ ТЕКУЩЕГО ПРОЕКТА
------------------------------------------------------------
НЕ удалять и НЕ заменять свои:

    config/account.json
    config/anthropic.json
    state/analysis_state.json
    state/trade_state.json
    state/fundingpips_risk_state.json

Архив специально НЕ содержит этих runtime/secrets файлов.

ПЕРВЫЙ ТЕСТ
------------------------------------------------------------
1. Остановить runner.py.
2. Скопировать файлы проекта поверх текущего D:\claude_robot.
3. НЕ удалять config/ и существующие state JSON.
4. Запустить:

       python test_max_market_context.py

   Этот тест Claude НЕ вызывает. Он пересоберёт:

       debug/claude_market_payload.json

   В новом raw/cache-partition формате.

5. Проверить в выводе:

       Python не рассчитывает торговые индикаторы...
       Claude каждый H1-анализ строит самостоятельно...

6. Только после этого, если нужен платный SAFE test:

       python test_professional_trader_analysis.py --confirm-paid-analysis

На первом Claude-вызове FP-day ожидается cache write.
На следующем H1-вызове при совпавшем cache prefix ожидается cache read.
Фактические значения видны в console usage:

       Cache write:
       Cache read:

============================================================
