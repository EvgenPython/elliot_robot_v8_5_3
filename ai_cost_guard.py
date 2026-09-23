from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent

STATE_PATH = (
    BASE_DIR
    / "state"
    / "ai_cost_budget.json"
)

FP_TZ = timezone(
    timedelta(hours=3)
)

TARGET_DAILY_USD = 0.65
SOFT_DAILY_USD = 0.80

# Separate reserve used only for management of an already-open position.
POSITION_RESERVE_USD = 0.25


def _now_fp() -> datetime:
    return datetime.now(
        timezone.utc
    ).astimezone(
        FP_TZ
    )


def _day_key() -> str:
    return _now_fp().date().isoformat()


def _empty_state() -> dict:
    return {
        "version": 1,
        "records": [],
    }


def _load() -> dict:

    if not STATE_PATH.exists():
        return _empty_state()

    try:
        value = json.loads(
            STATE_PATH.read_text(
                encoding="utf-8"
            )
        )
    except Exception:
        return _empty_state()

    if not isinstance(value, dict):
        return _empty_state()

    if not isinstance(
        value.get("records"),
        list,
    ):
        value["records"] = []

    return value


def _save(state: dict) -> None:

    STATE_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    tmp = STATE_PATH.with_suffix(
        ".json.tmp"
    )

    tmp.write_text(
        json.dumps(
            state,
            ensure_ascii=False,
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )

    tmp.replace(
        STATE_PATH
    )


def _cost_value(value) -> float:

    if isinstance(value, dict):
        value = value.get("usd")

    try:
        return max(
            0.0,
            float(value or 0.0),
        )
    except (TypeError, ValueError):
        return 0.0


def _record_identity(
    *,
    request_id,
    family,
    stage,
    usage,
    at_utc,
) -> str:

    request_id = str(
        request_id or ""
    ).strip()

    if request_id:
        return "request:" + request_id

    raw = json.dumps(
        {
            "family": family,
            "stage": stage,
            "usage": usage,
            "at_utc": at_utc,
        },
        ensure_ascii=False,
        sort_keys=True,
        default=str,
    )

    return (
        "fallback:"
        + hashlib.sha256(
            raw.encode("utf-8")
        ).hexdigest()
    )


def record_pipeline_usage(
    *,
    family: str,
    stage: str,
    record: dict,
) -> bool:
    """
    Persist one already-completed/billed request.

    This never estimates a request before it happens.
    It records the same estimated_cost already produced by
    claude_resilient_pipeline.
    """

    if not isinstance(record, dict):
        return False

    cost = _cost_value(
        record.get("estimated_cost")
    )

    usage = record.get("usage")

    if (
        cost <= 0
        and not isinstance(usage, dict)
    ):
        return False

    category = (
        "position"
        if str(family).startswith(
            "POSITION_REVIEW"
        )
        else "analysis"
    )

    identity = _record_identity(
        request_id=record.get(
            "request_id"
        ),
        family=family,
        stage=stage,
        usage=usage,
        at_utc=record.get(
            "at_utc"
        ),
    )

    state = _load()

    for old in state["records"]:

        if (
            isinstance(old, dict)
            and old.get("identity")
            == identity
        ):
            return False

    state["records"].append(
        {
            "identity": identity,
            "fp_day": _day_key(),
            "recorded_at_fp": (
                _now_fp().isoformat()
            ),
            "category": category,
            "family": str(family),
            "stage": str(stage),
            "model": record.get(
                "model"
            ),
            "request_id": record.get(
                "request_id"
            ),
            "usage": usage,
            "estimated_cost_usd": cost,
        }
    )

    # Keep history bounded.
    state["records"] = (
        state["records"][-2000:]
    )

    _save(
        state
    )

    return True


def budget_snapshot() -> dict:

    state = _load()
    today = _day_key()

    analysis = 0.0
    position = 0.0
    requests = 0

    for record in state["records"]:

        if not isinstance(
            record,
            dict,
        ):
            continue

        if record.get(
            "fp_day"
        ) != today:
            continue

        cost = _cost_value(
            record.get(
                "estimated_cost_usd"
            )
        )

        requests += 1

        if (
            record.get("category")
            == "position"
        ):
            position += cost

        else:
            analysis += cost

    return {
        "fp_day": today,
        "analysis_spent_usd": round(
            analysis,
            6,
        ),
        "position_spent_usd": round(
            position,
            6,
        ),
        "total_spent_usd": round(
            analysis + position,
            6,
        ),
        "requests": requests,
        "target_daily_usd": (
            TARGET_DAILY_USD
        ),
        "soft_daily_usd": (
            SOFT_DAILY_USD
        ),
        "position_reserve_usd": (
            POSITION_RESERVE_USD
        ),
    }


def inspect_cycle_budget(
    cycle: str,
) -> dict:

    cycle = str(
        cycle or ""
    ).upper()

    snap = budget_snapshot()

    analysis = float(
        snap["analysis_spent_usd"]
    )

    position = float(
        snap["position_spent_usd"]
    )

    allowed = True
    reason = "allowed"


    # One daily baseline FULL must be allowed to complete.
    if cycle in {
        "FULL",
        "FULL_SCHEDULED",
        "FULL_FALLBACK",
        "MARKET_MAP",
        "FULL_DECISION",
    }:
        allowed = True
        reason = "daily_full_may_complete"


    elif cycle in {
        "H1_DECISION",
        "H1_DECISION_REFRESH",
    }:

        allowed = (
            analysis
            < TARGET_DAILY_USD
        )

        reason = (
            "below_target_budget"
            if allowed
            else "target_budget_reached"
        )


    elif cycle == "ENTRY_CHECK":

        allowed = (
            analysis
            < SOFT_DAILY_USD
        )

        reason = (
            "below_soft_ceiling"
            if allowed
            else "soft_ceiling_reached"
        )


    elif cycle in {
        "M30",
        "M30_DECISION",
        "M30_DECISION_REFRESH",
    }:

        allowed = (
            paid_m30_enabled()
            and
            analysis
            < TARGET_DAILY_USD
        )

        reason = (
            "explicit_m30_override"
            if allowed
            else "paid_m30_disabled_by_cost_policy"
        )


    elif cycle in {
        "POSITION_REVIEW",
        "POSITION_SCOUT",
    }:

        allowed = (
            position
            < POSITION_RESERVE_USD
        )

        reason = (
            "position_reserve_available"
            if allowed
            else "position_reserve_reached"
        )


    elif cycle in {
        "FULL_ESCALATED",
    }:

        allowed = (
            analysis
            < SOFT_DAILY_USD
        )

        reason = (
            "critical_escalation_budget_available"
            if allowed
            else "soft_ceiling_reached"
        )


    return {
        **snap,
        "cycle": cycle,
        "allowed": bool(allowed),
        "reason": reason,
    }


def paid_m30_enabled() -> bool:

    return str(
        os.getenv(
            "ROBOT_ENABLE_PAID_M30",
            "",
        )
    ).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }

# ============================================================================
# V8.5.3 EVENT-DRIVEN TRADING BUDGET
# ============================================================================
#
# The daily FULL is a baseline analytical cost.
# It must not consume the only opportunity to evaluate a genuine H1 setup.
#
# Rules:
#   - max 1 new H1 trade-decision pipeline per FP day;
#   - max 1 new ENTRY_CHECK pipeline per FP day;
#   - an emergency ceiling still blocks new paid event work after abnormal cost;
#   - existing FULL / POSITION rules remain unchanged.
#
# A TD_CONTEXT usage record is used as the conservative marker that a paid
# trade-decision pipeline was already started. Retries therefore never create
# extra budget freedom.
# ============================================================================

EVENT_HARD_CEILING_USD = 2.50
MAX_H1_EVENT_DECISIONS_PER_DAY = 1
MAX_ENTRY_CHECKS_PER_DAY = 1


_COST_EVENT_ORIGINAL_INSPECT_CYCLE_BUDGET = (
    inspect_cycle_budget
)


def _event_pipeline_count_today(
    family_name: str,
) -> int:

    state = _load()
    today = _day_key()

    count = 0

    for record in (
        state.get("records")
        or []
    ):

        if not isinstance(
            record,
            dict,
        ):
            continue

        if (
            record.get("fp_day")
            != today
        ):
            continue

        if (
            str(
                record.get("family")
                or ""
            )
            != family_name
        ):
            continue

        # Every new TRADE_DECISION pipeline starts with TD_CONTEXT.
        # Counting this stage prevents a second event pipeline from being
        # purchased after the first one has already started.
        if (
            str(
                record.get("stage")
                or ""
            )
            == "TD_CONTEXT"
        ):
            count += 1

    return count


def inspect_cycle_budget(
    cycle: str,
) -> dict:

    result = (
        _COST_EVENT_ORIGINAL_INSPECT_CYCLE_BUDGET(
            cycle
        )
    )

    normalized = str(
        cycle or ""
    ).upper()

    analysis_spent = float(
        result.get(
            "analysis_spent_usd"
        )
        or 0.0
    )


    if normalized in {
        "H1_DECISION",
        "H1_DECISION_REFRESH",
    }:

        used = _event_pipeline_count_today(
            "TRADE_DECISION:H1_DECISION"
        )

        slot_available = (
            used
            < MAX_H1_EVENT_DECISIONS_PER_DAY
        )

        under_hard_ceiling = (
            analysis_spent
            < EVENT_HARD_CEILING_USD
        )

        result[
            "event_decisions_used"
        ] = used

        result[
            "event_decisions_limit"
        ] = (
            MAX_H1_EVENT_DECISIONS_PER_DAY
        )

        result[
            "event_hard_ceiling_usd"
        ] = EVENT_HARD_CEILING_USD

        result[
            "allowed"
        ] = bool(
            slot_available
            and under_hard_ceiling
        )

        if not under_hard_ceiling:
            result[
                "reason"
            ] = "event_hard_ceiling_reached"

        elif not slot_available:
            result[
                "reason"
            ] = "daily_h1_event_slot_used"

        else:
            result[
                "reason"
            ] = "daily_h1_event_slot_available"


    elif normalized == "ENTRY_CHECK":

        used = _event_pipeline_count_today(
            "TRADE_DECISION:ENTRY_CHECK"
        )

        slot_available = (
            used
            < MAX_ENTRY_CHECKS_PER_DAY
        )

        under_hard_ceiling = (
            analysis_spent
            < EVENT_HARD_CEILING_USD
        )

        result[
            "entry_checks_used"
        ] = used

        result[
            "entry_checks_limit"
        ] = (
            MAX_ENTRY_CHECKS_PER_DAY
        )

        result[
            "event_hard_ceiling_usd"
        ] = EVENT_HARD_CEILING_USD

        result[
            "allowed"
        ] = bool(
            slot_available
            and under_hard_ceiling
        )

        if not under_hard_ceiling:
            result[
                "reason"
            ] = "event_hard_ceiling_reached"

        elif not slot_available:
            result[
                "reason"
            ] = "daily_entry_check_slot_used"

        else:
            result[
                "reason"
            ] = "daily_entry_check_slot_available"


    return result

# ============================================================================
# V8.5.3 M30 PRIMARY RUNTIME BUDGET
# ============================================================================

M30_PRIMARY_MODE = True

# Initial DEMO policy:
# at most two genuinely-triggered paid M30 decision pipelines per FP day.
MAX_M30_EVENT_DECISIONS_PER_DAY = 2


_M30_PRIMARY_PREVIOUS_INSPECT_CYCLE_BUDGET = (
    inspect_cycle_budget
)


def paid_m30_enabled() -> bool:
    """
    M30-primary is ON by default.

    Explicit environment override may still disable it:
    ROBOT_ENABLE_PAID_M30=0/false/no/off
    """

    raw = str(
        os.getenv(
            "ROBOT_ENABLE_PAID_M30",
            "",
        )
    ).strip().lower()


    if not raw:
        return True


    return raw in {
        "1",
        "true",
        "yes",
        "on",
    }


def inspect_cycle_budget(
    cycle: str,
) -> dict:

    result = (
        _M30_PRIMARY_PREVIOUS_INSPECT_CYCLE_BUDGET(
            cycle
        )
    )


    normalized = str(
        cycle or ""
    ).upper()


    analysis_spent = float(
        result.get(
            "analysis_spent_usd"
        )
        or 0.0
    )


    # H1 now owns structural validation only.
    # New entry opportunities belong to M30-primary.
    if normalized in {
        "H1_DECISION",
        "H1_DECISION_REFRESH",
    }:

        result["allowed"] = False

        result[
            "reason"
        ] = "m30_primary_h1_entry_disabled"

        result[
            "m30_primary_mode"
        ] = True

        return result


    if normalized in {
        "M30",
        "M30_DECISION",
        "M30_DECISION_REFRESH",
    }:

        used = (
            _event_pipeline_count_today(
                "TRADE_DECISION:M30_DECISION"
            )
        )


        enabled = (
            paid_m30_enabled()
        )


        slot_available = (
            used
            < MAX_M30_EVENT_DECISIONS_PER_DAY
        )


        under_hard_ceiling = (
            analysis_spent
            < EVENT_HARD_CEILING_USD
        )


        result[
            "m30_primary_mode"
        ] = True

        result[
            "m30_decisions_used"
        ] = used

        result[
            "m30_decisions_limit"
        ] = (
            MAX_M30_EVENT_DECISIONS_PER_DAY
        )

        result[
            "event_hard_ceiling_usd"
        ] = EVENT_HARD_CEILING_USD


        result[
            "allowed"
        ] = bool(
            enabled
            and slot_available
            and under_hard_ceiling
        )


        if not enabled:

            result[
                "reason"
            ] = "paid_m30_explicitly_disabled"

        elif not under_hard_ceiling:

            result[
                "reason"
            ] = "event_hard_ceiling_reached"

        elif not slot_available:

            result[
                "reason"
            ] = "daily_m30_event_slots_used"

        else:

            result[
                "reason"
            ] = "daily_m30_event_slot_available"


    return result
