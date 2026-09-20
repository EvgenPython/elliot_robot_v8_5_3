from datetime import datetime, timedelta

from prop_time import FUNDINGPIPS_TZ
from trade_state import extract_latest_closed_h1_time


# ============================================================
# DAILY BASELINE FULL + EVENT-DRIVEN ANALYSIS SCHEDULE
# ============================================================

# Один обязательный глубокий анализ после закрытия H1 в 08:00 FP.
# Это дневная структурная база до открытия Лондона. На всех остальных
# закрытых H1 работает дешёвый Scout; дополнительный FULL разрешён только
# при изменении структуры/setup/неопределённости или отсутствии reference.
ANALYSIS_POLICY_VERSION = "daily_full_scout_h1_decision_v2"
DAILY_BASELINE_FULL_CLOSE_HOUR = 8
MANDATORY_FULL_CLOSE_HOURS = (DAILY_BASELINE_FULL_CLOSE_HOUR,)

CYCLE_FULL_SCHEDULED = "FULL_SCHEDULED"
CYCLE_SCOUT = "SCOUT"
CYCLE_FULL_FALLBACK = "FULL_FALLBACK"
CYCLE_FULL_ESCALATED = "FULL_ESCALATED"
CYCLE_H1_DECISION_REFRESH = "H1_DECISION_REFRESH"
CYCLE_M30_DECISION_REFRESH = "M30_DECISION_REFRESH"


def _as_fp_datetime(value) -> datetime:
    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.fromisoformat(str(value))

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=FUNDINGPIPS_TZ)
    else:
        dt = dt.astimezone(FUNDINGPIPS_TZ)

    return dt


def get_closed_h1_open_time(snapshot: dict) -> datetime:
    value = extract_latest_closed_h1_time(snapshot)

    if not value:
        raise RuntimeError(
            "Не удалось определить open-time последней закрытой H1."
        )

    return _as_fp_datetime(value)


def get_closed_h1_close_time(snapshot: dict) -> datetime:
    return get_closed_h1_open_time(snapshot) + timedelta(hours=1)


def is_mandatory_full_cycle(snapshot: dict) -> bool:
    close_time = get_closed_h1_close_time(snapshot)

    if close_time.weekday() >= 5:
        return False

    return close_time.hour in MANDATORY_FULL_CLOSE_HOURS


def inspect_analysis_schedule(snapshot: dict) -> dict:
    h1_open = get_closed_h1_open_time(snapshot)
    h1_close = h1_open + timedelta(hours=1)
    mandatory = is_mandatory_full_cycle(snapshot)

    return {
        "analysis_policy_version": ANALYSIS_POLICY_VERSION,
        "h1_open_time_fp": h1_open.isoformat(),
        "h1_close_time_fp": h1_close.isoformat(),
        "h1_close_hour": h1_close.hour,
        "mandatory_full": mandatory,
        "daily_baseline_full": mandatory,
        "cycle_mode": CYCLE_FULL_SCHEDULED if mandatory else CYCLE_SCOUT,
        "daily_baseline_full_close_hour": DAILY_BASELINE_FULL_CLOSE_HOUR,
        "mandatory_full_close_hours": list(MANDATORY_FULL_CLOSE_HOURS),
        "non_full_h1_decision_refresh": True,
    }


def print_analysis_schedule(schedule: dict):
    print()
    print("=" * 80)
    print("DAILY BASELINE FULL + SCOUT SCHEDULE")
    print("=" * 80)
    print(f"H1 open FP:       {schedule.get('h1_open_time_fp')}")
    print(f"H1 close FP:      {schedule.get('h1_close_time_fp')}")
    print(
        "Daily FULL:      "
        f"{'YES' if schedule.get('mandatory_full') else 'NO'}"
    )
    print(f"Cycle mode:       {schedule.get('cycle_mode')}")
    print(
        "Daily FULL time:  "
        + ", ".join(
            f"{hour:02d}:00"
            for hour in schedule.get("mandatory_full_close_hours", [])
        )
        + " FP"
    )
    print("=" * 80)
