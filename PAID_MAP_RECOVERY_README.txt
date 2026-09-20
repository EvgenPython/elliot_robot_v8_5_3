ROBOT ELLIOT — PAID MAP LOCAL RECOVERY, 2026-08-25
==================================================

Причина исправления
-------------------
24.08.2026 полный FULL_MAP завершился end_turn, но compact market_regime
содержал 6 строк вместо 7. Единственный FULL_MAP_REPAIR исправил market_regime,
но перечислил три data-quality issue отдельными элементами legacy-массива:

  ["true", issue1, issue2, issue3]

Старая локальная версия ожидала ровно ["true", "единая issues-строка"] и
отклонила уже оплаченный ответ. Рыночная карта, волны и координаты при этом
были полными.

Что исправлено
--------------
- Новый API wire contract передаёт data_quality как закрытый object:
  {"sufficient":"true","issues":"единая строка"}.
- Старые сохранённые массивы совместимы: первая ячейка остаётся sufficient,
  остальные issue-строки без потерь объединяются через "; ".
- Все другие positional rows по-прежнему требуют точную длину и fail closed.
- REPAIR prompt теперь прямо требует COMPACT WIRE schema.
- После невалидного REPAIR журнал пишет REPAIR EXHAUSTED и запрещает второй
  repair/повтор исходного этапа.
- Добавлен recover_paid_web_test.py: он не вызывает FULL_MAP/REPAIR и может
  после локальной валидации выполнить только отсутствующий FULL_DECISION.

Локальное бесплатное восстановление MAP
---------------------------------------
Из корня D:\robot_elliot_windows:

  python -X utf8 -u recover_paid_web_test.py ^
    --archive "analysis_archive\2026-08-24\110355_full_web_test_20260824_1000.json" ^
    --map-response "debug\claude_staged_attempts\full_map_repair_msg_011CeMEDJACRXirCUuFoRLdU.json"

Без --confirm-paid-decision скрипт только бесплатно восстановит и проверит
MAP, сохранит его в существующий frozen archive и завершится с BLOCKED перед
любым новым API-вызовом.

Продолжение только с платным FULL_DECISION
------------------------------------------
После успешного бесплатного recovery повторить команду с флагом:

  python -X utf8 -u recover_paid_web_test.py ^
    --archive "analysis_archive\2026-08-24\110355_full_web_test_20260824_1000.json" ^
    --map-response "debug\claude_staged_attempts\full_map_repair_msg_011CeMEDJACRXirCUuFoRLdU.json" ^
    --confirm-paid-decision

Эта команда:
- повторно локально валидирует сохранённый MAP;
- НЕ вызывает FULL_MAP и FULL_MAP_REPAIR;
- выполняет только ещё отсутствующий FULL_DECISION;
- при необходимости допускает прежний единственный DECISION_REPAIR;
- не вызывает Risk Manager, Trade State, Executor или mt5.order_send;
- сохраняет итоговый validated FULL в тот же archive.

Перед запуском
--------------
- сохранить исходный archive и оба msg JSON;
- остановить runner.py;
- убедиться, что выполняется только одна копия Python-проекта;
- сначала выполнить бесплатные тесты:

  python -m unittest discover -v

Новый платный FULL_MAP для snapshot 2026-08-24T10:00:00+03:00 не нужен.
