import json
from datetime import datetime, timedelta
from pathlib import Path

from prop_time import FUNDINGPIPS_TZ, now_fp
from instruments import symbol_state_path


# ============================================================
# PATH
# ============================================================

BASE_DIR = Path(__file__).resolve().parent
STATE_DIR = BASE_DIR / "state"
REFERENCE_STATE_PATH = symbol_state_path("claude_reference_state.json")


# ============================================================
# POLICY
# ============================================================

# Успешный дневной FULL используется Scout до конца тех же FP-суток.
# Он НИКОГДА не заменяет свежий raw market tape и на следующий FP-день не
# переносится. 18 часов покрывают всё рабочее окно 08:00-23:00 с запасом.
MAX_REFERENCE_AGE_HOURS = 18


# ============================================================
# HELPERS
# ============================================================

def _parse_fp(value) -> datetime | None:
    if not value:
        return None

    try:
        dt = datetime.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=FUNDINGPIPS_TZ)
    else:
        dt = dt.astimezone(FUNDINGPIPS_TZ)

    return dt


# ============================================================
# LOAD / SAVE
# ============================================================

def load_reference_state() -> dict | None:
    if not REFERENCE_STATE_PATH.exists():
        return None

    try:
        with open(
            REFERENCE_STATE_PATH,
            "r",
            encoding="utf-8",
        ) as file:
            value = json.load(file)
    except (OSError, json.JSONDecodeError):
        return None

    if not isinstance(value, dict):
        return None

    if not isinstance(value.get("analysis"), dict):
        return None

    return value


def save_reference_analysis(
    analysis: dict,
    snapshot: dict,
) -> dict:
    """
    Сохраняет только последний УСПЕШНО завершённый анализ Claude.

    Это reference для следующего H1, а не источник истины.
    """

    STATE_DIR.mkdir(parents=True, exist_ok=True)

    h1_closed_time = None

    try:
        closed = snapshot["timeframes"]["H1"]["closed_bars"]
        if closed is not None and len(closed) > 0:
            h1_closed_time = closed.iloc[-1]["time_fp"].isoformat()
    except Exception:
        h1_closed_time = None

    record = {
        "saved_at_fp": now_fp().isoformat(),
        "market_snapshot_time_fp": snapshot.get("generated_at_fp"),
        "h1_closed_bar_time_fp": h1_closed_time,
        "instrument": snapshot.get("instrument", "XAUUSD"),
        "analysis": analysis,
    }

    temp_path = REFERENCE_STATE_PATH.with_suffix(".tmp")

    with open(temp_path, "w", encoding="utf-8") as file:
        json.dump(
            record,
            file,
            ensure_ascii=False,
            indent=2,
        )

    temp_path.replace(REFERENCE_STATE_PATH)

    return record


# ============================================================
# SAFE REFERENCE FOR CURRENT REQUEST
# ============================================================

def get_fresh_reference_for_payload(
    payload: dict,
) -> dict | None:
    """
    Возвращает дневной FULL reference только если он пригоден для Scout.

    Дополнительная защита от anchoring:
    - только тот же инструмент;
    - те же FundingPips сутки;
    - максимум MAX_REFERENCE_AGE_HOURS, покрывающий рабочий день.

    Scout сравнивает reference со свежим компактным raw tape. Если Scout
    эскалирует цикл, FULL независимо анализирует полный raw market context.
    """

    state = load_reference_state()

    if state is None:
        return None

    if str(state.get("instrument")) != str(payload.get("instrument")):
        return None

    current_time = _parse_fp(payload.get("timestamp"))
    reference_time = _parse_fp(state.get("market_snapshot_time_fp"))

    if current_time is None or reference_time is None:
        return None

    if current_time.date() != reference_time.date():
        return None

    age = current_time - reference_time

    if age < timedelta(0):
        return None

    if age > timedelta(hours=MAX_REFERENCE_AGE_HOURS):
        return None

    return state


def get_fresh_reference_for_snapshot(
    snapshot: dict,
) -> dict | None:
    """
    Возвращает свежий последний FULL-reference напрямую для Market Snapshot.

    Используется дневным Scout, чтобы не строить тяжёлый FULL payload до того,
    как Scout решит, что полный анализ действительно нужен.
    """

    pseudo_payload = {
        "instrument": snapshot.get("instrument", "XAUUSD"),
        "timestamp": snapshot.get("generated_at_fp"),
    }

    return get_fresh_reference_for_payload(pseudo_payload)
