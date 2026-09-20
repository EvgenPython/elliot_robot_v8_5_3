ROBOT ELLIOT — BOUNDED EFFORT HOTFIX, 2026-08-22
================================================

Причина
-------
Два реальных FULL_MAP с Claude Sonnet 5 и effort=max использовали весь
доступный output budget только на thinking:

  47 998 из 48 000;
  127 998 из 128 000.

Финальный Structured JSON не был создан, хотя оба запроса тарифицировались.

Исправление
-----------
- Claude Sonnet 5 сохранён.
- Полные raw D1/H4/H1 и H1/M15/M5 сохранены.
- Аналитические правила, стратегия, schema и validators сохранены.
- FULL_MAP: effort=medium, max_tokens=64 000.
- FULL_DECISION: effort=medium, max_tokens=48 000.
- REPAIR: effort=medium, max_tokens=32 000/24 000.
- Платный web-тест использует новые journal stages WEB_TEST_V4.
- Известный stop_reason=max_tokens не вызывает повтор полного этапа.

Проверка
--------
python -m py_compile claude_staged_client.py run_web_full_test.py
python -m unittest discover -v

Ожидаемый результат текущей полной сборки: Ran 59 tests ... OK.

Платный тест запускать только после локальных тестов:

python -X utf8 -u run_web_full_test.py --confirm-paid-analysis

После успешного FULL:

python -X utf8 -u web_publisher.py --once
