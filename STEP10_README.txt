CLAUDE ROBOT — DAILY BASELINE FULL + EVENT-DRIVEN SCOUT
=======================================================

ЦЕЛЬ
----
Один раз в день создать глубокую структурную карту XAUUSD, а затем покупать
новый FULL только тогда, когда дешёвый Scout видит изменение рынка, возможный
setup или неопределённость.

РАБОЧЕЕ ОКНО
-------------
Используется только FundingPips Platform Time.
Понедельник-пятница, 08:00 <= time < 23:00.
Вне окна новые Claude cycles не запускаются. Технический контроль позиции,
pending и дневного FundingPips state продолжает работать независимо от окна.

ОДИН ОБЯЗАТЕЛЬНЫЙ ДНЕВНОЙ FULL
------------------------------
Единственный обязательный FULL запускается после закрытия H1 в 08:00 FP.
Это дневная базовая карта до открытия Лондона.

Других обязательных FULL в 12:00, 16:00 или 20:00 больше нет.

Если робот стартовал позже, дневной FULL завершился ошибкой или сделка была
открыта во время 08:00 FP, первый плоский H1 без reference выполняет
FULL_FALLBACK. Отсутствие карты считается системной неопределённостью: Scout
не может сравнить свежий рынок с несуществующим анализом.

SCOUT НА КАЖДОЙ ОСТАЛЬНОЙ H1
----------------------------
На каждой новой закрытой H1 внутри рабочего окна, когда робот без позиции и
без блокирующего active plan, запускается CHEAP SCOUT.

Model: отдельный scout_model из config/anthropic.json;
       default claude-haiku-4-5-20251001
Effort: для Haiku не передаётся (модель его не поддерживает);
        для совместимой override-модели default low
Max tokens: 6000
Prompt cache: OFF

Scout не имеет права создавать BUY/SELL, Entry, Stop Loss, Take Profit или
Trade Plan. Он только сравнивает дневной FULL-reference со свежим компактным
raw MT5 tape.

Внеплановый FULL обязателен, если Scout видит хотя бы одно:
- material structure change;
- новую/завершающуюся волну или инвалидацию прежней разметки;
- приближение, пробой, retest или rejection важного уровня;
- возможный торговый setup;
- изменение momentum/regime/характера движения;
- недостаток данных, low confidence или любую неопределённость.

SCOUT_NO_FULL разрешён только при высокой уверенности, что структура не
изменилась и возможного setup нет.

ДНЕВНОЙ REFERENCE
-----------------
Последний успешный FULL используется Scout до конца тех же FP-суток.
Технический предел возраста reference — 18 часов, что покрывает всё рабочее
окно после baseline в 08:00 FP. На следующий FP-день старый reference
автоматически отклоняется.

Reference не является источником истины. Scout получает свежий raw tape, а
каждый эскалированный FULL снова независимо анализирует полный raw context.

FULL ANALYSIS
-------------
Один FULL состоит из двух сохраняемых этапов одного frozen snapshot:

1. FULL_MAP — полный D1/H4/H1 market map, Elliott waves, structure, levels,
   zones и primary/alternate scenarios.
2. FULL_DECISION — validated map + H1/M15/M5 и прежний строгий торговый
   recommendation contract.

Claude Sonnet 5:
- FULL_MAP: effort=medium, max_tokens=64000;
- FULL_DECISION: effort=medium, max_tokens=48000.

Снижение effort не меняет raw данные, правила анализа, schema, Risk Manager
или Executor. Оно не позволяет adaptive thinking снова съесть весь output
budget без финального JSON.

ОТКРЫТАЯ ПОЗИЦИЯ
----------------
При managed position:
- Scout не запускается;
- scheduled FULL не запускается;
- event-driven FULL не запускается;
- Market Snapshot для нового анализа не собирается;
- робот выполняет только технический position/risk audit и ждёт SL/TP.

После подтверждённого закрытия позиции цикл сначала фиксирует результат сделки.
Затем, когда робот снова flat, анализ возобновляется по последней закрытой H1.
Если дневной reference существует — запускается Scout. Если reference нет —
выполняется FULL_FALLBACK.

ANALYSIS STATE
--------------
Одна закрытая H1 = один завершённый analysis cycle:
- SCOUT_NO_FULL;
- FULL_SCHEDULED — дневной baseline 08:00 FP;
- FULL_ESCALATED — Scout обнаружил событие;
- FULL_FALLBACK — дневного reference нет или Scout не дал надёжного ответа.

Пока позиция открыта, пропущенные H1 не догоняются и Claude не вызывается.
После выхода используется только последняя актуальная закрытая H1.

АРХИВ И АУДИТ
--------------
analysis_archive/YYYY-MM-DD хранит payload/result, тип цикла, Claude usage,
Scout decision, FULL map/decision и торговый результат. Для проверки политики:

python -X utf8 -u inspect_step10_week.py

Сравнивайте SCOUT_NO_FULL с ближайшим следующим FULL_ESCALATED/FULL_FALLBACK
или дневным FULL_SCHEDULED следующего дня.

ОБНОВЛЕНИЕ WINDOWS
------------------
Перед заменой файлов остановить runner.py. Не перезаписывать и не удалять:
- config/account.json;
- config/anthropic.json;
- config/web_export*.json;
- state/*.json;
- analysis_archive/*.

Проверка:

python -m py_compile analysis_schedule.py claude_reference_state.py main.py
python -m unittest discover -v

Ожидается: Ran 59 tests, OK.
