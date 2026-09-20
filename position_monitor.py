"""Nested-wave monitoring and protected SL/TP management for a position.

The monitor uses a cheap M15 Scout and escalates material events to a deep
Sonnet POSITION_REVIEW. Every closed H1 receives a deep review unconditionally.
Only ``position_protection.py`` may translate a verified review into an SL/TP
request; opening, adding, reversing and automatic closing remain forbidden.
"""

from __future__ import annotations
from observability import observe, emit

import copy
import json
import os
from datetime import datetime
from pathlib import Path

import MetaTrader5 as mt5

from analysis_archive import save_analysis_archive, safe_update_analysis_archive
from claude_payload import build_claude_payload
from claude_reference_state import load_reference_state
from claude_resilient_pipeline import run_position_review_pipeline

from claude_staged_client import (
    analyze_position_review,
    get_last_stage_diagnostics,
    get_last_stage_usage,
)
from instruments import active_instrument, symbol_state_path
from main import (
    _run_api_with_retries,
    build_hold_risk_context,
    inspect_position_gate,
)
from market_data import (
    TIMEFRAMES,
    get_market_snapshot,
    mt5_timestamp_to_fp,
)
from prop_time import now_fp
from scout_client import analyze_scout
from scout_payload import build_scout_payload
from trade_state import get_managed_positions
from position_protection import (
    PositionProtectionError,
    execute_position_protection,
)


SYMBOL = active_instrument()
BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / "config" / "position_monitor.json"
STATE_PATH = symbol_state_path("position_monitor_state.json")

POSITION_MONITOR_VERSION = "nested_wave_management_v3_exact_previous_wave_stop"
STATE_VERSION = 1

DEFAULT_CONFIG = {
    "enabled": True,
    "m15_scout_enabled": True,
    "deep_review_on_every_closed_h1": True,
    "deep_review_on_m15_event": True,
    "retry_after_error_seconds": 300,
    "automatic_trade_changes": True,
    "minimum_confidence": "high",
}


def load_position_monitor_config() -> dict:
    config = dict(DEFAULT_CONFIG)
    if CONFIG_PATH.exists():
        raw = json.loads(CONFIG_PATH.read_text(encoding="utf-8-sig"))
        if not isinstance(raw, dict):
            raise RuntimeError(
                "config/position_monitor.json должен содержать JSON object."
            )
        config.update(raw)

    for name in (
        "enabled",
        "m15_scout_enabled",
        "deep_review_on_every_closed_h1",
        "deep_review_on_m15_event",
        "automatic_trade_changes",
    ):
        if not isinstance(config.get(name), bool):
            raise RuntimeError(f"position_monitor.{name} должен быть true/false.")

    if str(config.get("minimum_confidence", "high")) != "high":
        raise RuntimeError(
            "position_monitor.minimum_confidence должен быть high."
        )

    try:
        retry_seconds = int(config.get("retry_after_error_seconds", 300))
    except (TypeError, ValueError) as error:
        raise RuntimeError(
            "position_monitor.retry_after_error_seconds должен быть целым."
        ) from error
    config["retry_after_error_seconds"] = max(60, min(3600, retry_seconds))
    return config


def _empty_state() -> dict:
    return {
        "version": STATE_VERSION,
        "monitor_version": POSITION_MONITOR_VERSION,
        "updated_at_fp": None,
        "position_key": None,
        "last_processed_h1": None,
        "last_processed_m15": None,
        "last_attempt_key": None,
        "last_attempt_at_fp": None,
        "last_attempt_ok": None,
        "status": "idle",
        "last_scout": None,
        "last_review": None,
        "last_error": None,
    }


def load_position_monitor_state() -> dict:
    try:
        value = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return _empty_state()
    if not isinstance(value, dict):
        return _empty_state()
    result = _empty_state()
    result.update(value)
    result["version"] = STATE_VERSION
    result["monitor_version"] = POSITION_MONITOR_VERSION
    return result


def _save_state(state: dict) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    state = copy.deepcopy(state)
    state["version"] = STATE_VERSION
    state["monitor_version"] = POSITION_MONITOR_VERSION
    state["updated_at_fp"] = now_fp().isoformat()
    temporary_path = STATE_PATH.with_suffix(STATE_PATH.suffix + ".tmp")
    with open(temporary_path, "w", encoding="utf-8") as file:
        json.dump(state, file, ensure_ascii=False, indent=2)
        file.flush()
        os.fsync(file.fileno())
    os.replace(temporary_path, STATE_PATH)


def _position_key(managed_positions: list[dict]) -> str:
    tickets = sorted(
        str(item.get("position_ticket") or item.get("plan_id") or "unknown")
        for item in managed_positions
        if isinstance(item, dict)
    )
    return "|".join(tickets)


def _latest_closed_bar_time(symbol: str, timeframe_name: str) -> str:
    rates = mt5.copy_rates_from_pos(
        str(symbol),
        TIMEFRAMES[timeframe_name],
        1,
        1,
    )
    if rates is None or len(rates) == 0:
        raise RuntimeError(
            f"Не удалось получить последнюю закрытую {timeframe_name}: "
            f"{mt5.last_error()}"
        )
    return mt5_timestamp_to_fp(int(rates[-1]["time"])).isoformat()


def _seconds_since(value) -> float:
    if not value:
        return float("inf")
    try:
        timestamp = datetime.fromisoformat(str(value))
    except (TypeError, ValueError):
        return float("inf")
    if timestamp.tzinfo is None:
        return float("inf")
    return max(0.0, (now_fp() - timestamp).total_seconds())


def inspect_position_monitor_due(
    symbol: str = SYMBOL,
    managed_positions: list[dict] | None = None,
    *,
    tick_fresh: bool = True,
) -> dict:
    """Return the next monitor action without making any API request."""
    config = load_position_monitor_config()
    positions = (
        managed_positions
        if isinstance(managed_positions, list)
        else get_managed_positions()
    )
    if not config["enabled"]:
        return {"due": False, "reason": "disabled"}
    if not positions:
        return {"due": False, "reason": "no_managed_position"}
    if not tick_fresh:
        return {"due": False, "reason": "market_not_fresh"}

    h1_time = _latest_closed_bar_time(symbol, "H1")
    m15_time = _latest_closed_bar_time(symbol, "M15")
    position_key = _position_key(positions)
    state = load_position_monitor_state()

    if state.get("position_key") != position_key:
        trigger = "position_start"
        action = "deep_review"
        event_time = m15_time
    elif (
        config["deep_review_on_every_closed_h1"]
        and state.get("last_processed_h1") != h1_time
    ):
        trigger = "h1_close"
        action = "deep_review"
        event_time = h1_time
    elif (
        config["m15_scout_enabled"]
        and state.get("last_processed_m15") != m15_time
    ):
        trigger = "m15_close"
        action = "m15_scout"
        event_time = m15_time
    else:
        return {
            "due": False,
            "reason": "already_processed",
            "h1_time": h1_time,
            "m15_time": m15_time,
            "position_key": position_key,
        }

    attempt_key = f"{position_key}|{action}|{event_time}"
    if (
        state.get("last_attempt_key") == attempt_key
        and state.get("last_attempt_ok") is False
        and _seconds_since(state.get("last_attempt_at_fp"))
        < config["retry_after_error_seconds"]
    ):
        return {
            "due": False,
            "reason": "retry_cooldown",
            "retry_after_seconds": config["retry_after_error_seconds"],
            "attempt_key": attempt_key,
        }

    return {
        "due": True,
        "action": action,
        "trigger": trigger,
        "event_time": event_time,
        "h1_time": h1_time,
        "m15_time": m15_time,
        "position_key": position_key,
        "attempt_key": attempt_key,
        "config": config,
    }


def _stage_suffix(value: str) -> str:
    return "".join(character for character in str(value) if character.isdigit())[:12]


def _position_context(position_gate: dict) -> dict:
    managed_plans = get_managed_positions()
    live_positions = copy.deepcopy(position_gate.get("live_managed") or [])
    primary_ticket = None
    if live_positions:
        primary_ticket = live_positions[0].get("position_ticket")
    return {
        "primary_position_ticket": primary_ticket,
        "live_positions": live_positions,
        "managed_trade_plans": copy.deepcopy(managed_plans),
        "risk_context": build_hold_risk_context(),
    }


def _build_position_scout_payload(
    snapshot: dict,
    previous_reference: dict,
    position_context: dict,
    previous_review: dict | None,
) -> dict:
    payload = build_scout_payload(snapshot, previous_reference)
    payload["task"] = "POSITION_SCOUT"
    payload["policy"].update(
        {
            "may_open_trade": False,
            "may_define_entry_sl_tp": False,
            "analysis_suspended_while_position_open": False,
            "position_monitor_only": True,
            "goal": (
                "Detect a new M15 child wave, structure/pattern/level/FVG "
                "event, thesis risk or target approach that requires a deep "
                "POSITION_REVIEW and deterministic protection validation."
            ),
        }
    )
    payload["open_position_context"] = copy.deepcopy(position_context)
    payload["previous_position_review"] = copy.deepcopy(
        previous_review if isinstance(previous_review, dict) else {}
    )
    return payload


def _mark_attempt(state: dict, due: dict) -> dict:
    state = copy.deepcopy(state)
    state["position_key"] = due["position_key"]
    state["last_attempt_key"] = due["attempt_key"]
    state["last_attempt_at_fp"] = now_fp().isoformat()
    state["last_attempt_ok"] = False
    state["status"] = "running"
    state["last_error"] = None
    _save_state(state)
    return state


def _mark_failure(state: dict, error) -> dict:
    state = copy.deepcopy(state)
    state["last_attempt_ok"] = False
    state["status"] = "error"
    state["last_error"] = str(error)
    _save_state(state)
    return state


def _mark_scout_success(state: dict, due: dict, scout_result: dict) -> dict:
    state = copy.deepcopy(state)
    state["last_attempt_ok"] = True
    state["last_processed_m15"] = due["m15_time"]
    state["last_scout"] = copy.deepcopy(scout_result)
    state["status"] = "scout_complete"
    state["last_error"] = None
    _save_state(state)
    return state


def _mark_review_success(state: dict, due: dict, review_result: dict) -> dict:
    state = copy.deepcopy(state)
    state["last_attempt_ok"] = True
    state["last_processed_h1"] = due["h1_time"]
    state["last_processed_m15"] = due["m15_time"]
    state["last_review"] = copy.deepcopy(review_result)
    state["status"] = str(review_result.get("position_status") or "reviewed")
    state["last_error"] = None
    _save_state(state)
    return state


def _print_review_summary(result: dict) -> None:
    summary = str(result.get("summary") or "")
    russian = summary.split("\nRU:", 1)[-1].strip() if "\nRU:" in summary else summary
    print()
    print("=" * 80)
    print("POSITION REVIEW — ВОЛНЫ И СОПРОВОЖДЕНИЕ")
    print("=" * 80)
    print(f"Position status: {result.get('position_status')}")
    print(f"Advisory:        {result.get('advisory_action')}")
    print(f"Confidence:      {result.get('confidence')}")
    print(f"Кратко:          {' '.join(russian.split())}")
    management = result.get("position_management") or {}
    print(f"Protection plan: {management.get('action')}")
    print("=" * 80)


@observe("position")
def run_position_monitor(symbol: str, due: dict) -> dict:
    """Run one due Scout/review and validate any protection proposal."""
    if not isinstance(due, dict) or not due.get("due"):
        return {"ok": False, "reason": "monitor_not_due"}

    state = _mark_attempt(load_position_monitor_state(), due)
    try:
        gate = inspect_position_gate(symbol=symbol)
        if not gate.get("live_managed"):
            raise RuntimeError("Управляемая открытая позиция больше не найдена.")
        protection_blocked = bool(gate.get("unresolved_positions") or gate.get("unmanaged_positions"))
        if protection_blocked:
            emit("position", "execution_quarantine_analysis_continues", data={"gate": gate})

        snapshot = get_market_snapshot(symbol=symbol)
        position_context = _position_context(gate)
        previous_reference = load_reference_state()
        previous_review = state.get("last_review")
        deep_trigger = None
        scout_result = None

        if due["action"] == "m15_scout":
            if not isinstance(previous_reference, dict):
                # Without a validated parent map a cheap relative-change Scout
                # has no trustworthy baseline. Escalate the closed M15 instead.
                deep_trigger = "m15_event"
            else:
                scout_payload = _build_position_scout_payload(
                    snapshot,
                    previous_reference,
                    position_context,
                    previous_review,
                )
                scout_archive = save_analysis_archive(
                    snapshot=snapshot,
                    cycle_type="POSITION_SCOUT",
                    payload=scout_payload,
                    result=None,
                    previous_reference=previous_reference,
                    note=(
                        "M15 POSITION_SCOUT payload сохранён до платного вызова. "
                        "Scout не имеет торговых полномочий."
                    ),
                )
                scout_stage = "POSITION_SCOUT_" + _stage_suffix(due["event_time"])
                scout_run = _run_api_with_retries(
                    snapshot=snapshot,
                    api_stage=scout_stage,
                    cycle_type="POSITION_SCOUT",
                    payload_timestamp=scout_payload.get("timestamp"),
                    archive_path=scout_archive,
                    api_call=lambda on_preflight, on_response: analyze_scout(
                        scout_payload,
                        on_preflight=on_preflight,
                        on_response=on_response,
                    ),
                    result_archive_key="position_scout_result",
                    usage_archive_key="position_scout_usage",
                )
                if not scout_run.get("ok"):
                    raise RuntimeError(
                        "POSITION_SCOUT не завершён: "
                        f"{scout_run.get('error') or 'unknown error'}"
                    )
                scout_result = scout_run["result"]
                safe_update_analysis_archive(
                    scout_archive,
                    position_scout_result=scout_result,
                    position_monitor_version=POSITION_MONITOR_VERSION,
                )
                if not scout_result.get("full_analysis_required"):
                    state = _mark_scout_success(state, due, scout_result)
                    print(
                        "[POSITION SCOUT] M15 обновлена; нового структурного "
                        "события для глубокого POSITION_REVIEW нет."
                    )
                    return {
                        "ok": True,
                        "kind": "scout_only",
                        "scout_result": scout_result,
                    }
                if due["config"].get("deep_review_on_m15_event"):
                    deep_trigger = "m15_event"
                else:
                    state = _mark_scout_success(state, due, scout_result)
                    return {
                        "ok": True,
                        "kind": "scout_escalation_disabled",
                        "scout_result": scout_result,
                    }
        elif due["trigger"] == "position_start":
            deep_trigger = "position_start"
        else:
            deep_trigger = "h1_close"

        if deep_trigger is None:
            deep_trigger = "m15_event"

        full_payload = build_claude_payload(
            snapshot,
            previous_reference=previous_reference,
        )
        review_archive = save_analysis_archive(
            snapshot=snapshot,
            cycle_type="POSITION_REVIEW",
            payload=full_payload,
            result=None,
            scout_result=scout_result,
            previous_reference=previous_reference,
            note=(
                "Глубокий POSITION_REVIEW payload сохранён до платного "
                "вызова. Claude не вызывает Executor; отдельный Python gate "
                "проверяет protection plan после ответа."
            ),
        )
        review_stage = "POSITION_REVIEW_" + _stage_suffix(due["event_time"])
        review_identity = (
            f"{due.get('position_key')}|{due.get('attempt_key')}|{review_stage}"
        )
        review_run = run_position_review_pipeline(
            full_payload,
            review_trigger=deep_trigger,
            position_context=position_context,
            previous_reference=previous_reference,
            previous_monitor_result=previous_review,
            pipeline_identity=review_identity,
        )
        if not review_run.get("ok"):
            raise RuntimeError(
                "POSITION_REVIEW micro-pipeline не завершён: "
                f"{review_run.get('error') or 'unknown error'}"
            )

        review_result = review_run["result"]
        emit("position", "review_decision", data={"due": due, "review": review_result,
             "position_context": position_context, "archive_path": str(review_archive)})
        protection_result = None
        try:
            if protection_blocked:
                raise PositionProtectionError("Position reconciliation requires resolution; analysis completed, protection unchanged.")
            protection_result = execute_position_protection(
                review=review_result,
                snapshot=snapshot,
                position_context=position_context,
                config=due["config"],
                event_key=due["attempt_key"],
            )
        except PositionProtectionError as error:
            protection_result = {
                "ok": False,
                "status": "validation_blocked",
                "order_send_called": False,
                "error": str(error),
            }

        safe_update_analysis_archive(
            review_archive,
            position_monitor_result=review_result,
            position_monitor_version=POSITION_MONITOR_VERSION,
            automatic_trade_changes=bool(
                due["config"].get("automatic_trade_changes", False)
            ),
            position_protection_result=protection_result,
        )
        _mark_review_success(state, due, review_result)
        _print_review_summary(review_result)
        if protection_result:
            print(
                "[POSITION PROTECTION] "
                f"status={protection_result.get('status')}; "
                f"order_send={protection_result.get('order_send_called', False)}"
            )
            if protection_result.get("error"):
                print(f"[POSITION PROTECTION] {protection_result['error']}")
        return {
            "ok": True,
            "kind": "deep_review",
            "scout_result": scout_result,
            "review_result": review_result,
            "protection_result": protection_result,
        }
    except Exception as error:
        _mark_failure(state, f"{type(error).__name__}: {error}")
        print(
            "[POSITION MONITOR ERROR] "
            f"{type(error).__name__}: {error}. "
            "Проверьте журнал protection и фактический SL/TP в MT5."
        )
        return {"ok": False, "reason": f"{type(error).__name__}: {error}"}
