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
