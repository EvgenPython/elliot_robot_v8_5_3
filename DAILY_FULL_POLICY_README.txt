ROBOT ELLIOT — DAILY FULL POLICY, 2026-08-22
============================================

Политика анализа
----------------
1. Единственный обязательный глубокий FULL: H1 close 08:00 FP.
2. Остальные новые H1 в рабочем окне: дешёвый Scout.
3. Scout запускает FULL при structure change, новой/завершённой волне,
   возможном setup, level event, low confidence или uncertainty.
4. Если дневного FULL-reference нет, выполняется FULL_FALLBACK.
5. Reference действует только в те же FP-сутки, максимум 18 часов.
6. При managed position Claude полностью выключен.
7. После подтверждённого закрытия позиции анализ возвращается на последней
   актуальной закрытой H1; старые H1 внутри сделки не догоняются.

Стоимость Scout
---------------
Scout имеет отдельную модель: по умолчанию claude-haiku-4-5-20251001.
FULL по-прежнему использует claude-sonnet-5. Локальный anthropic.json можно
дополнить ключом scout_model; пример находится в config/anthropic.example.json.

Что не изменено
---------------
- FundingPips Platform Time;
- рабочее окно и market freshness gates;
- raw MT5 market data;
- Elliott/multi-timeframe prompts и structured contracts;
- Risk Manager;
- Entry/SL/TP validation;
- Executor;
- HOLD until SL or TP;
- Windows -> Linux read-only export.

Дополнительная надёжность FULL
-----------------------------
Claude Sonnet 5 сохраняется, но FULL_MAP/FULL_DECISION используют bounded
effort=medium. Два реальных effort=max запроса ранее израсходовали все 48k и
128k output tokens только на thinking и не создали JSON.

Проверка
--------
python -m py_compile analysis_schedule.py claude_reference_state.py main.py
python -m unittest discover -v

Ожидается: Ran 59 tests ... OK.
