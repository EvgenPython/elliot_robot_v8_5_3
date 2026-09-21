from observability import observe, emit, context
from datetime import datetime, timedelta, timezone
import time

import MetaTrader5 as mt5

from mt5_client import (
    connect_mt5,
    disconnect_mt5,
)

from market_data import (
    get_market_snapshot,
    print_market_snapshot,
    mt5_timestamp_to_fp,
)

from claude_payload import (
    build_claude_payload,
    print_payload_stats,
    save_debug_payload,
)

from claude_client import (
    ClaudeInvalidResponseError,
    ClaudeTransientRequestError,
    claude_api_retry_context,
    get_api_retry_policy,
    get_error_failure_class,
    get_error_request_id,
    get_error_retry_after_seconds,
    get_last_attempt_diagnostics,
    get_last_usage_stats,
    is_outcome_unknown_error,
    is_repairable_claude_error,
    is_retryable_claude_error,
)

from claude_staged_client import (
    STAGED_ANALYSIS_VERSION,
    analyze_market_map,
    analyze_m30_decision_refresh,
    analyze_trade_decision,
    analyze_h1_decision_refresh,
    assemble_staged_analysis,
    build_previous_confirmed_anchor_reference,
    combine_stage_usage,
    get_last_stage_diagnostics,
    get_last_stage_usage,
    repair_market_map,
    repair_trade_decision,
    validate_market_map_result,
)

from claude_client import (
    print_analysis_summary,
    save_debug_response,
)

from claude_request_guard import (
    begin_api_attempt,
    get_api_cycle,
    mark_api_attempt,
    update_api_attempt,
)

from claude_resilient_pipeline import (
    run_market_map_pipeline,
    run_trade_decision_pipeline,
)

from claude_reference_state import (
    get_fresh_reference_for_payload,
    get_fresh_reference_for_snapshot,
    save_reference_analysis,
)

from risk_manager import (
    evaluate_trade,
    print_risk_report,
    get_account_info,
    get_positions,
    get_daily_state,
    calculate_fundingpips_limits,
    calculate_open_positions_risk,
    enrich_limits,
    now_fp,
)

from trade_state import (
    register_trade_decision,
    print_trade_state_result,
    get_managed_positions,
    get_active_plan,
    mark_managed_position_closed,
    extract_latest_closed_h1_time,
)

from trade_executor import (
    print_executor_report,
)

from live_executor import (
    execute_active_plan,
    print_live_execution_report,
    print_send_intent_reconciliation,
    reconcile_active_send_intent,
)

from pending_executor import (
    reconcile_active_pending_execution,
    print_pending_startup_reconciliation,
)

from analysis_state import (
    GATE_ALREADY_ANALYZED,
    check_analysis_gate,
    print_analysis_gate,
    register_completed_analysis,
    print_analysis_registration,
    register_uncertain_api_attempt,
    print_uncertain_api_registration,
    load_analysis_state,
    save_analysis_state,
)

from execution_control import (
    inspect_execution_safety_gate,
    print_execution_safety_gate,
    inspect_pre_claude_gate,
    print_pre_claude_gate,
)

from runtime_policy import (
    inspect_market_runtime_gate,
    print_market_runtime_gate,
)

from analysis_schedule import (
    CYCLE_FULL_ESCALATED,
    CYCLE_FULL_FALLBACK,
    CYCLE_FULL_SCHEDULED,
    CYCLE_H1_DECISION_REFRESH,
    CYCLE_M30_DECISION_REFRESH,
    inspect_analysis_schedule,
    print_analysis_schedule,
)

from scout_payload import (
    build_scout_payload,
    print_scout_payload_stats,
    save_debug_scout_payload,
)

from scout_client import (
    analyze_scout,
)

from analysis_archive import (
    load_analysis_archive,
    safe_update_analysis_archive,
    save_analysis_archive,
)

from web_market_snapshot import (
    WEB_MARKET_SNAPSHOT_PATH,
    save_web_market_snapshot,
)
from entry_watch import refresh_entry_watch
from entry_check_cycle import assemble_entry_refresh_analysis


# ============================================================
# ОСНОВНЫЕ НАСТРОЙКИ
# ============================================================

from instruments import active_instrument

SYMBOL = active_instrument()

PROJECT_MODE = (
    "FUNDINGPIPS_2_STEP_STANDARD_EVALUATION_ONLY"
)


# ============================================================
# ЛОГИКА СТРАТЕГИИ
# ============================================================

# Основное правило открытой позиции:
#
#     ENTRY
#       ↓
#     POSITION OPEN
#       ↓
#     NESTED H4/H1/M30/M15/M5 MONITOR
#       ↓
#     SL tightening / Fibonacci TP / broker exit
#
# После открытия позиции этот основной ENTRY pipeline больше не вызывается.
# Отдельный position_monitor.py продолжает анализ H4/H1/M30/M15/M5. Он может
# передать только доказательный SL/TP plan в position_protection.py. Стоп
# разрешено двигать исключительно в сторону уменьшения риска. Новые входы,
# доливки, развороты и автоматическое закрытие позиции запрещены.
#
# Technical reconciliation is separate from runner M15/H1 position analysis.


# ============================================================
# H1 ANALYSIS POLICY
# ============================================================

# Одна закрытая H1-свеча = один завершённый analysis cycle.
#
# Cycle может быть:
#   - SCOUT -> NO FULL;
#   - SCOUT -> FULL;
#   - один обязательный DAILY BASELINE FULL в 08:00 FP;
#   - FULL FALLBACK, если дневной reference отсутствует.
#
# После завершения cycle та же H1 повторно Claude не отправляется.
# Executor при необходимости может повторно обработать уже созданный plan.
ONE_ANALYSIS_CYCLE_PER_CLOSED_H1 = True


# ============================================================
# CONTROLLED CLAUDE RETRIES + SINGLE VALIDATED WINNER
# ============================================================

def _stage_attempt_archive_sections(
    api_stage: str,
    attempt: dict | None,
    cycle: dict | None,
) -> dict:
    """Keeps every stage audit while retaining legacy latest-stage fields."""
    prefix = "".join(
        character.lower() if character.isalnum() else "_"
        for character in str(api_stage)
    ).strip("_")
    attempts = (cycle or {}).get("attempts", [])
    return {
        "api_attempt": attempt,
        "api_attempts": attempts,
        "api_cycle": cycle,
        f"{prefix}_api_attempt": attempt,
        f"{prefix}_api_attempts": attempts,
        f"{prefix}_api_cycle": cycle,
    }

def _begin_guarded_api_attempt(
    snapshot: dict,
    api_stage: str,
    cycle_type: str,
    payload_timestamp,
    archive_path,
    max_attempts: int,
) -> dict:
    h1_time = extract_latest_closed_h1_time(snapshot)
    result = begin_api_attempt(
        h1_closed_bar_time=h1_time,
        api_stage=api_stage,
        cycle_type=cycle_type,
        payload_timestamp=payload_timestamp,
        archive_path=archive_path,
        max_attempts=max_attempts,
    )

    cycle = result.get("cycle") or get_api_cycle(h1_time, api_stage)
    safe_update_analysis_archive(
        archive_path,
        **_stage_attempt_archive_sections(
            api_stage,
            result.get("attempt"),
            cycle,
        ),
    )
    return result


def _update_guarded_api_attempt(
    snapshot: dict,
    api_stage: str,
    archive_path,
    attempt_id: str,
    diagnostics: dict,
) -> None:
    h1_time = extract_latest_closed_h1_time(snapshot)
    attempt = update_api_attempt(
        h1_closed_bar_time=h1_time,
        api_stage=api_stage,
        attempt_id=attempt_id,
        **diagnostics,
    )
    cycle = get_api_cycle(h1_time, api_stage)
    safe_update_analysis_archive(
        archive_path,
        **_stage_attempt_archive_sections(api_stage, attempt, cycle),
    )


def _register_guarded_api_failure(
    snapshot: dict,
    api_stage: str,
    archive_path,
    attempt_id: str,
    error: Exception,
    diagnostics: dict | None = None,
) -> dict:
    h1_time = extract_latest_closed_h1_time(snapshot)
    diagnostics = dict(diagnostics or {})
    error_diagnostics = getattr(error, "diagnostics", None)
    if isinstance(error_diagnostics, dict):
        diagnostics.update(error_diagnostics)
    retryable = is_retryable_claude_error(error)
    failure_class = get_error_failure_class(error)
    if is_outcome_unknown_error(error):
        cycle_before = get_api_cycle(h1_time, api_stage) or {}
        prior_unknown_count = sum(
            1
            for item in cycle_before.get("attempts", [])
            if isinstance(item, dict)
            and item.get("outcome_unknown")
            and item.get("attempt_id") != attempt_id
        )
        # Hard safety boundary: configuration cannot re-enable a third
        # possibly billed generation for the same stage/frozen snapshot.
        if prior_unknown_count >= 1:
            retryable = False
            failure_class = "OUTCOME_UNKNOWN_LIMIT"
    status = "FAILED_RETRYABLE" if retryable else "FAILED_PERMANENT"

    attempt = mark_api_attempt(
        h1_closed_bar_time=h1_time,
        api_stage=api_stage,
        attempt_id=attempt_id,
        status=status,
        request_id=(
            diagnostics.get("request_id") or get_error_request_id(error)
        ),
        usage=diagnostics.get("usage"),
        error=error,
        retryable=retryable,
        outcome_unknown=is_outcome_unknown_error(error),
        failure_class=failure_class,
        billing_status=(
            diagnostics.get("billing_status")
            or (
                "UNKNOWN_MAY_BE_BILLED"
                if is_outcome_unknown_error(error)
                else "USAGE_AVAILABLE"
                if isinstance(diagnostics.get("usage"), dict)
                else "NO_COMPLETED_RESPONSE_REPORTED"
            )
        ),
        delivery_recovered=diagnostics.get("delivery_recovered"),
    )
    cycle = get_api_cycle(h1_time, api_stage)

    safe_update_analysis_archive(
        archive_path,
        **_stage_attempt_archive_sections(api_stage, attempt, cycle),
        note=(
            f"{api_stage} attempt {attempt.get('attempt_number')} не дал "
            "валидного результата. "
            f"Failure class: {attempt.get('failure_class')}; "
            f"billing: {attempt.get('billing_status')}; "
            f"cycle status: {attempt.get('cycle_status')}."
        ),
    )
    return attempt


def _complete_guarded_api_attempt(
    snapshot: dict,
    api_stage: str,
    archive_path,
    attempt_id: str,
    usage: dict | None = None,
    diagnostics: dict | None = None,
) -> dict:
    h1_time = extract_latest_closed_h1_time(snapshot)
    diagnostics = diagnostics or {}
    attempt = mark_api_attempt(
        h1_closed_bar_time=h1_time,
        api_stage=api_stage,
        attempt_id=attempt_id,
        status="VALIDATED",
        request_id=diagnostics.get("request_id"),
        usage=usage,
        failure_class=(
            "DELIVERY_RECOVERED"
            if diagnostics.get("delivery_recovered")
            else "VALIDATED_RESPONSE"
        ),
        billing_status=(
            diagnostics.get("billing_status")
            or ("USAGE_AVAILABLE" if isinstance(usage, dict) else "UNKNOWN")
        ),
        delivery_recovered=diagnostics.get("delivery_recovered", False),
    )
    cycle = get_api_cycle(h1_time, api_stage)
    safe_update_analysis_archive(
        archive_path,
        **_stage_attempt_archive_sections(api_stage, attempt, cycle),
    )
    return attempt


def _load_validated_archive_result(
    archive_path,
    result_archive_key: str = "result",
) -> dict | None:
    try:
        record = load_analysis_archive(archive_path)
    except Exception as error:
        print(
            "[API RECOVERY ERROR] Validated response есть в journal, но "
            "архив прочитать не удалось: "
            f"{type(error).__name__}: {error}"
        )
        return None

    result = record.get(str(result_archive_key))
    return result if isinstance(result, dict) else None


def _build_known_api_cost_audit(archive_path) -> dict:
    """Aggregate only usage Anthropic actually returned, without guessing."""
    record = load_analysis_archive(archive_path)
    attempts = []
    seen = set()
    for name, value in record.items():
        if name != "api_attempts" and not str(name).endswith("_api_attempts"):
            continue
        if not isinstance(value, list):
            continue
        for attempt in value:
            if not isinstance(attempt, dict):
                continue
            attempt_id = str(attempt.get("attempt_id") or "")
            if attempt_id and attempt_id in seen:
                continue
            if attempt_id:
                seen.add(attempt_id)
            attempts.append(attempt)

    fields = {
        "input_tokens",
        "output_tokens",
        "cache_creation_input_tokens",
        "cache_read_input_tokens",
        "thinking_tokens",
    }
    totals = {field: 0 for field in sorted(fields)}
    usage_attempts = 0
    for attempt in attempts:
        usage = attempt.get("usage")
        if not isinstance(usage, dict):
            continue
        usage_attempts += 1
        for field in fields:
            totals[field] += int(usage.get(field, 0) or 0)

    return {
        "known_usage_attempts": usage_attempts,
        "billing_unknown_attempts": sum(
            attempt.get("billing_status") == "UNKNOWN_MAY_BE_BILLED"
            for attempt in attempts
        ),
        "delivery_recovered_attempts": sum(
            bool(attempt.get("delivery_recovered")) for attempt in attempts
        ),
        "known_usage_totals": totals,
        "note": (
            "Totals include only attempts with API usage. "
            "UNKNOWN_MAY_BE_BILLED attempts are counted separately and "
            "never estimated locally."
        ),
    }


def _load_resumable_api_archive(
    snapshot: dict,
    api_stage: str,
) -> tuple[object, dict] | None:
    """Returns the original frozen payload after a process/Windows restart."""
    h1_time = extract_latest_closed_h1_time(snapshot)
    cycle = get_api_cycle(h1_time, api_stage)
    if not cycle or cycle.get("status") not in {
        "IN_PROGRESS",
        "WAITING_RETRY",
    }:
        return None

    archive_path = cycle.get("archive_path")
    if not archive_path:
        return None

    try:
        record = load_analysis_archive(archive_path)
    except Exception as error:
        print(
            "[API RESUME ERROR] Исходный frozen archive недоступен: "
            f"{type(error).__name__}: {error}"
        )
        return None

    if not isinstance(record.get("payload"), dict):
        print("[API RESUME ERROR] Frozen archive не содержит payload.")
        return None

    return archive_path, record


def _load_resumable_staged_full_archive(
    snapshot: dict,
) -> tuple[object, dict] | None:
    """Recovers one frozen FULL snapshot across both persisted stages.

    A validated FULL_MAP is deliberately considered resumable: after a
    restart FULL_DECISION must use that exact map/payload instead of buying a
    new map for a newer snapshot of the same closed H1.
    """
    h1_time = extract_latest_closed_h1_time(snapshot)
    candidates = []
    for api_stage in (
        "FULL_DECISION_REPAIR",
        "FULL_DECISION",
        "FULL_MAP_REPAIR",
        "FULL_MAP",
    ):
        cycle = get_api_cycle(h1_time, api_stage)
        if not cycle or cycle.get("status") not in {
            "IN_PROGRESS",
            "WAITING_RETRY",
            "VALIDATED",
            "EXHAUSTED",
        }:
            continue
        archive_path = cycle.get("archive_path")
        if archive_path:
            candidates.append((archive_path, cycle))

    for archive_path, cycle in candidates:
        try:
            record = load_analysis_archive(archive_path)
        except Exception as error:
            print(
                "[STAGED RESUME ERROR] Frozen archive недоступен: "
                f"{type(error).__name__}: {error}"
            )
            continue
        if not isinstance(record.get("payload"), dict):
            print(
                "[STAGED RESUME ERROR] Frozen archive не содержит FULL payload."
            )
            continue
        print(
            "[STAGED RESUME] Найден сохранённый stage "
            f"{cycle.get('api_stage')} status={cycle.get('status')}."
        )
        return archive_path, record

    return None


def _load_h1_decision_archive(
    snapshot: dict,
) -> tuple[object, dict] | None:
    """Recover the exact H1 decision payload after any process interruption."""
    h1_time = extract_latest_closed_h1_time(snapshot)
    for stage in ("H1_DECISION_REPAIR", "H1_DECISION"):
        cycle = get_api_cycle(h1_time, stage)
        if not cycle or cycle.get("status") not in {
            "IN_PROGRESS", "WAITING_RETRY", "VALIDATED", "EXHAUSTED"
        }:
            continue
        archive_path = cycle.get("archive_path")
        if not archive_path:
            continue
        try:
            record = load_analysis_archive(archive_path)
        except Exception as error:
            print(
                "[H1 DECISION RESUME ERROR] Frozen archive недоступен: "
                f"{type(error).__name__}: {error}"
            )
            continue
        if isinstance(record.get("payload"), dict):
            return archive_path, record
    return None


def _load_m30_decision_archive(
    snapshot: dict,
) -> tuple[object, dict] | None:
    """Recover the original M30 frozen payload across retry rounds/restarts."""

    h1_time = extract_latest_closed_h1_time(snapshot)
    stages = []
    for base in ("M30_DECISION_REPAIR", "M30_DECISION"):
        stages.append(base)
        stages.extend(
            _api_retry_round_stage(base, round_number)
            for round_number in range(2, MAX_API_RETRY_ROUNDS_PER_SNAPSHOT + 1)
        )
    for stage in reversed(stages):
        cycle = get_api_cycle(h1_time, stage)
        if not isinstance(cycle, dict):
            continue
        archive_path = cycle.get("archive_path")
        if not archive_path:
            continue
        try:
            record = load_analysis_archive(archive_path)
        except Exception:
            continue
        if isinstance(record.get("payload"), dict):
            return archive_path, record
    return None


API_RETRY_ROUND_COOLDOWN_SECONDS = 300
MAX_API_RETRY_ROUNDS_PER_SNAPSHOT = 100


def _api_retry_round_stage(api_stage: str, round_number: int) -> str:
    base = str(api_stage).strip().upper()
    if int(round_number) <= 1:
        return base
    return f"{base}__RECOVERY_ROUND_{int(round_number)}"


def _cycle_retry_wait_seconds(cycle: dict) -> float:
    value = cycle.get("updated_at_utc")
    if not value:
        return 0.0
    try:
        updated = datetime.fromisoformat(str(value))
        if updated.tzinfo is None:
            updated = updated.replace(tzinfo=timezone.utc)
        elapsed = (datetime.now(timezone.utc) - updated.astimezone(timezone.utc)).total_seconds()
    except (TypeError, ValueError):
        return 0.0
    return max(0.0, API_RETRY_ROUND_COOLDOWN_SECONDS - elapsed)


def _select_api_journal_stage(snapshot: dict, api_stage: str) -> dict:
    """Pick one persistent sequential retry round for an unchanged snapshot.

    Known provider refusal, incomplete output and transient failures do not
    consume the market bar forever.  A completed five-attempt round is cooled
    down and the next runner pass opens a new journal namespace while keeping
    the same frozen archive/payload.  Permanent auth/input errors remain
    blocked until configuration is fixed.
    """

    h1_time = extract_latest_closed_h1_time(snapshot)
    inherited_failure_class = None
    for round_number in range(1, MAX_API_RETRY_ROUNDS_PER_SNAPSHOT + 1):
        journal_stage = _api_retry_round_stage(api_stage, round_number)
        cycle = get_api_cycle(h1_time, journal_stage)
        if not isinstance(cycle, dict):
            return {
                "journal_stage": journal_stage,
                "round_number": round_number,
                "previous_failure_class": inherited_failure_class,
                "deferred": False,
            }

        attempts = [item for item in (cycle.get("attempts") or []) if isinstance(item, dict)]
        last_attempt = attempts[-1] if attempts else {}
        status = str(cycle.get("status") or "").upper()
        previous_failure = str(last_attempt.get("failure_class") or "").upper() or None

        if status != "EXHAUSTED":
            return {
                "journal_stage": journal_stage,
                "round_number": round_number,
                "previous_failure_class": previous_failure or inherited_failure_class,
                "deferred": False,
            }

        retryable = bool(last_attempt.get("retryable"))
        hard_unknown_limit = previous_failure in {
            "OUTCOME_UNKNOWN_LIMIT",
            "PERMANENT_REQUEST",
        }
        if not retryable or hard_unknown_limit:
            return {
                "journal_stage": journal_stage,
                "round_number": round_number,
                "previous_failure_class": previous_failure,
                "deferred": False,
                "permanent_exhaustion": True,
            }

        wait_seconds = _cycle_retry_wait_seconds(cycle)
        if wait_seconds > 0:
            return {
                "journal_stage": journal_stage,
                "round_number": round_number,
                "previous_failure_class": previous_failure,
                "deferred": True,
                "retry_after_seconds": wait_seconds,
            }
        inherited_failure_class = previous_failure

    return {
        "journal_stage": _api_retry_round_stage(
            api_stage, MAX_API_RETRY_ROUNDS_PER_SNAPSHOT
        ),
        "round_number": MAX_API_RETRY_ROUNDS_PER_SNAPSHOT,
        "previous_failure_class": inherited_failure_class,
        "deferred": False,
        "permanent_exhaustion": True,
    }


@observe("analysis")
def _run_api_with_retries(
    snapshot: dict,
    api_stage: str,
    cycle_type: str,
    payload_timestamp,
    archive_path,
    api_call,
    result_archive_key: str = "result",
    usage_archive_key: str = "api_usage",
    result_validator=None,
    diagnostics_fallback=None,
    usage_fallback=None,
) -> dict:
    """Runs bounded attempts against one frozen market snapshot."""
    selection = _select_api_journal_stage(snapshot, api_stage)
    if selection.get("deferred"):
        retry_after = float(selection.get("retry_after_seconds") or 0.0)
        error = ClaudeTransientRequestError(
            f"{api_stage}: предыдущий последовательный retry-round завершён; "
            f"следующий разрешён через {retry_after:.0f} sec.",
            retry_after_seconds=retry_after,
        )
        print(
            f"[CLAUDE DEFERRED] {api_stage}: новый платный запрос сейчас не "
            f"отправлен; cooldown {retry_after:.0f} sec."
        )
        return {
            "ok": False,
            "error": error,
            "deferred": True,
            "retry_after_seconds": retry_after,
            "exhausted": False,
        }

    journal_stage = str(selection["journal_stage"])
    retry_round = int(selection.get("round_number") or 1)
    inherited_failure_class = selection.get("previous_failure_class")
    policy = get_api_retry_policy(api_stage)
    max_attempts = int(policy["max_attempts"])
    delays = list(policy["retry_delays_seconds"])
    outcome_unknown_min_delay = float(
        policy.get("outcome_unknown_min_delay_seconds", 90) or 90
    )

    print()
    print(
        f"[RELIABILITY V3] {api_stage}: максимум {max_attempts} попыток; "
        f"задержки {delays} sec."
    )
    print(
        f"[RELIABILITY V4] Sequential retry round={retry_round}; "
        f"journal_stage={journal_stage}."
    )
    print(
        "[RELIABILITY V3] Frozen snapshot неизменен; при outcome-unknown "
        "разрешён максимум один повтор этапа. Invalid response не вызывает "
        "повтор полного этапа — для него предусмотрен отдельный REPAIR."
    )

    while True:
        try:
            begin_result = _begin_guarded_api_attempt(
                snapshot=snapshot,
                api_stage=journal_stage,
                cycle_type=cycle_type,
                payload_timestamp=payload_timestamp,
                archive_path=archive_path,
                max_attempts=max_attempts,
            )
        except Exception as error:
            print()
            print(
                "[API JOURNAL BLOCK] Попытку нельзя безопасно записать; "
                "платный запрос НЕ отправлен: "
                f"{type(error).__name__}: {error}"
            )
            return {"ok": False, "error": error, "exhausted": True}

        if not begin_result.get("allowed"):
            reason = begin_result.get("reason")
            cycle = begin_result.get("cycle") or {}
            if reason in {
                "VALIDATED_RESPONSE_ALREADY_EXISTS",
                "MAX_ATTEMPTS_EXHAUSTED",
                "CYCLE_EXHAUSTED",
            }:
                saved_path = cycle.get("archive_path") or archive_path
                recovered = _load_validated_archive_result(
                    saved_path,
                    result_archive_key=result_archive_key,
                )
                if recovered is not None:
                    if callable(result_validator):
                        try:
                            result_validator(recovered)
                        except Exception as validation_error:
                            error = RuntimeError(
                                f"Сохранённый {api_stage} result больше не "
                                "проходит локальную проверку: "
                                f"{type(validation_error).__name__}: "
                                f"{validation_error}"
                            )
                            return {
                                "ok": False,
                                "error": error,
                                "exhausted": True,
                            }
                    print(
                        "[API RECOVERY] Используем уже сохранённый validated "
                        "response (включая возможный REPAIR winner); новый "
                        "платный запрос НЕ нужен."
                    )
                    return {
                        "ok": True,
                        "result": recovered,
                        "usage": load_analysis_archive(saved_path).get(
                            usage_archive_key
                        ),
                        "recovered": True,
                    }

            error = RuntimeError(
                f"{api_stage} API cycle недоступен: {reason}; "
                f"status={cycle.get('status')}."
            )
            return {"ok": False, "error": error, "exhausted": True}

        attempt = begin_result["attempt"]
        attempt_id = attempt["attempt_id"]
        attempt_number = int(attempt["attempt_number"])
        diagnostics = {}

        print()
        print(
            f"[CLAUDE ATTEMPT] {api_stage} "
            f"{attempt_number}/{max_attempts}; id={attempt_id}"
        )

        def record_diagnostics(values: dict):
            diagnostics.update(values)
            if values.get("usage"):
                from api_costs import estimate_cost
                emit("api_usage", "reported", data={"attempt_id": attempt_id, "api_stage": api_stage,
                     "model": values.get("model"), "usage": values["usage"],
                     "estimated_cost": estimate_cost(values.get("model"), values["usage"]),
                     "billing_status": values.get("billing_status"), "archive_path": str(archive_path)})
            _update_guarded_api_attempt(
                snapshot=snapshot,
                api_stage=journal_stage,
                archive_path=archive_path,
                attempt_id=attempt_id,
                diagnostics=values,
            )

        try:
            prior_attempts = list((begin_result.get("cycle") or {}).get("attempts") or [])
            previous_failure_class = inherited_failure_class
            if len(prior_attempts) >= 2:
                previous_failure_class = (
                    prior_attempts[-2].get("failure_class")
                    or previous_failure_class
                )
            with context(attempt_id=attempt_id, api_stage=api_stage, archive_path=str(archive_path),
                         cycle_type=cycle_type, snapshot_time=snapshot.get("generated_at_fp")):
                with claude_api_retry_context(
                    previous_failure_class=previous_failure_class,
                    retry_round=retry_round,
                    attempt_number=attempt_number,
                ):
                    result = api_call(record_diagnostics, record_diagnostics)
            if not isinstance(result, dict) or not result:
                raise ClaudeInvalidResponseError(
                    f"{api_stage} вернул пустой/невалидный result."
                )
            if callable(result_validator):
                try:
                    validated_result = result_validator(result)
                    if isinstance(validated_result, dict):
                        result = validated_result
                except ClaudeInvalidResponseError:
                    raise
                except Exception as validation_error:
                    raise ClaudeInvalidResponseError(
                        f"{api_stage} response не прошёл локальную stage "
                        "валидацию: "
                        f"{type(validation_error).__name__}: "
                        f"{validation_error}",
                        invalid_result=result,
                        validation_error=(
                            f"{type(validation_error).__name__}: "
                            f"{validation_error}"
                        ),
                    ) from validation_error
        except Exception as error:
            error_diagnostics = getattr(error, "diagnostics", None)
            if isinstance(error_diagnostics, dict):
                diagnostics.update(error_diagnostics)
            fallback_diagnostics = None
            if callable(diagnostics_fallback):
                fallback_diagnostics = diagnostics_fallback()
            elif str(api_stage).upper() == "FULL":
                fallback_diagnostics = get_last_attempt_diagnostics()
            if fallback_diagnostics is not None:
                if isinstance(fallback_diagnostics, dict):
                    diagnostics.update(fallback_diagnostics)

            try:
                failed_attempt = _register_guarded_api_failure(
                    snapshot=snapshot,
                    api_stage=journal_stage,
                    archive_path=archive_path,
                    attempt_id=attempt_id,
                    error=error,
                    diagnostics=diagnostics,
                )
            except Exception as journal_error:
                print()
                print(
                    "[API JOURNAL BLOCK] Не удалось завершить запись "
                    "неудачной попытки; новый retry запрещён: "
                    f"{type(journal_error).__name__}: {journal_error}"
                )
                return {
                    "ok": False,
                    "error": error,
                    "journal_error": journal_error,
                    "exhausted": True,
                }

            print()
            print(
                f"[CLAUDE ATTEMPT FAILED] {api_stage} "
                f"{attempt_number}/{max_attempts}: "
                f"{type(error).__name__}: {error}"
            )

            if is_repairable_claude_error(error):
                if str(api_stage).upper().endswith("_REPAIR"):
                    print(
                        f"[REPAIR EXHAUSTED] {api_stage}: единственный "
                        "REPAIR завершён невалидным ответом. Второй REPAIR "
                        "и повтор исходного платного этапа запрещены."
                    )
                    return {
                        "ok": False,
                        "error": error,
                        "attempt": failed_attempt,
                        "repairable": False,
                        "invalid_result": dict(error.invalid_result),
                        "validation_error": (
                            error.validation_error or str(error)
                        ),
                        "exhausted": True,
                    }
                print(
                    f"[REPAIR REQUIRED] {api_stage}: полный этап повторно "
                    "не отправляется. Разрешён один отдельно журналируемый "
                    "маленький REPAIR-запрос."
                )
                return {
                    "ok": False,
                    "error": error,
                    "attempt": failed_attempt,
                    "repairable": True,
                    "invalid_result": dict(error.invalid_result),
                    "validation_error": (
                        error.validation_error or str(error)
                    ),
                    "exhausted": True,
                }

            if failed_attempt.get("cycle_status") == "WAITING_RETRY":
                delay_index = max(0, attempt_number - 1)
                delay = delays[delay_index] if delay_index < len(delays) else 0
                retry_after = get_error_retry_after_seconds(error)
                if retry_after is not None:
                    delay = max(delay, retry_after)
                if is_outcome_unknown_error(error):
                    delay = max(delay, outcome_unknown_min_delay)
                    print(
                        "[BILLING WARNING] Предыдущая генерация могла быть "
                        "тарифицирована. Reliability V3 разрешает только "
                        "один повтор этого этапа."
                    )
                print(
                    f"[RETRY] Повтор {api_stage} через {delay:.0f} sec."
                )
                if delay > 0:
                    time.sleep(delay)
                continue

            return {
                "ok": False,
                "error": error,
                "attempt": failed_attempt,
                "alert_required": bool(
                    failed_attempt.get("alert_required")
                    or is_outcome_unknown_error(error)
                ),
                "exhausted": True,
            }

        usage = diagnostics.get("usage")
        if not isinstance(usage, dict):
            usage = result.get("usage")
        if not isinstance(usage, dict) and callable(usage_fallback):
            usage = usage_fallback()
        if not isinstance(usage, dict):
            usage = get_last_usage_stats()

        # Save the validated business result before declaring a winner. This
        # allows a restarted runner to recover it without another API call.
        archive_sections = {
            str(result_archive_key): result,
            str(usage_archive_key): usage,
            "note": (
                f"{api_stage} validated response сохранён; "
                f"winner attempt {attempt_number}/{max_attempts}."
            ),
        }
        safe_update_analysis_archive(archive_path, **archive_sections)
        completed_attempt = _complete_guarded_api_attempt(
            snapshot=snapshot,
            api_stage=journal_stage,
            archive_path=archive_path,
            attempt_id=attempt_id,
            usage=usage,
            diagnostics=diagnostics,
        )
        print(
            f"[CLAUDE WINNER] {api_stage} validated; "
            f"attempt_id={completed_attempt.get('attempt_id')}."
        )
        return {
            "ok": True,
            "result": result,
            "usage": usage,
            "attempt": completed_attempt,
            "recovered": False,
        }


def _register_api_retries_exhausted(
    snapshot: dict,
    api_stage: str,
    error: Exception,
) -> None:
    # A missing/refused/incomplete provider answer is not a completed market
    # analysis.  Never advance last_analyzed_h1 here: runner must revisit the
    # same frozen payload after the controlled cooldown or after credentials
    # are repaired.
    emit(
        "api",
        "stage_not_completed",
        level="ERROR",
        data={
            "api_stage": str(api_stage),
            "h1_closed_bar_time": extract_latest_closed_h1_time(snapshot),
            "error_type": type(error).__name__,
            "error": str(error),
            "h1_consumed": False,
        },
    )
    print()
    print("=" * 80)
    print("CLAUDE STAGE НЕ ЗАВЕРШЁН — H1 НЕ ПОМЕЧЕНА ОБРАБОТАННОЙ")
    print("=" * 80)
    print(f"Stage: {api_stage}")
    print(f"Error: {type(error).__name__}: {error}")
    print("Runner повторит тот же сохранённый payload последовательно.")
    print("=" * 80)


def _emit_trade_funnel(
    *, snapshot: dict, cycle_type: str, analysis: dict, risk_report: dict,
    state_result: dict, execution_report: dict,
) -> None:
    """One compact, searchable explanation of what happened to an H1 idea."""
    recommendation = analysis.get("recommendation") or {}
    record = {
        "h1_closed_bar_time": extract_latest_closed_h1_time(snapshot),
        "cycle_type": cycle_type,
        "claude_action": recommendation.get("action"),
        "setup_type": recommendation.get("setup_type"),
        "setup_quality": recommendation.get("setup_quality"),
        "entry_quality": recommendation.get("entry_quality"),
        "confidence": recommendation.get("confidence"),
        "risk_decision": risk_report.get("decision"),
        "risk_reasons": list(risk_report.get("reasons") or []),
        "trade_state_action": state_result.get("state_action"),
        "plan_id": (state_result.get("active_plan") or {}).get("plan_id"),
        "execution_decision": execution_report.get("decision"),
        "order_send_called": bool(execution_report.get("order_send_called")),
    }
    emit("trade_funnel", "h1_result", data=record)
    print()
    print("=" * 80)
    print("TRADE FUNNEL — ИТОГ H1")
    print("=" * 80)
    print(f"Claude:       {record['claude_action']}")
    print(f"Risk:         {record['risk_decision']}")
    print(f"Trade State:  {record['trade_state_action']}")
    print(f"Execution:    {record['execution_decision']}")
    if record["risk_reasons"]:
        for reason in record["risk_reasons"]:
            print(f"- {reason}")
    print("=" * 80)


def _run_h1_decision_refresh(
    *,
    snapshot: dict,
    previous_reference: dict,
    scout_result: dict,
    scout_archive,
    execution_observation_only: bool,
    execution_block_reasons: list[str],
) -> None:
    """Evaluate a fresh trade decision on every non-FULL closed H1.

    D1/H4 and the parent Elliott map remain frozen from the most recent
    validated FULL.  Claude receives current closed H1/M30/M15/M5 data and may
    create a current entry or a new conditional projection.  This closes the
    old blind spot where SCOUT_NO_FULL meant that no trade decision existed.
    """
    reference_analysis = previous_reference.get("analysis")
    if not isinstance(reference_analysis, dict):
        raise RuntimeError("H1 decision refresh: FULL reference analysis отсутствует.")

    print()
    print("=" * 80)
    print("H1 DECISION REFRESH — СВЕЖАЯ ПРОВЕРКА ВХОДА")
    print("=" * 80)
    print("Старшая D1/H4/H1 карта сохраняется; H1/M30/M15/M5 проверяются заново.")

    refresh_resume = _load_h1_decision_archive(snapshot)
    if refresh_resume is not None:
        refresh_archive, refresh_record = refresh_resume
        payload = refresh_record["payload"]
        archived_reference = refresh_record.get("previous_reference")
        if isinstance(archived_reference, dict):
            previous_reference = archived_reference
            reference_analysis = previous_reference.get("analysis") or reference_analysis
        print(f"[API RESUME] H1_DECISION продолжает frozen payload из {refresh_archive}.")
    else:
        payload = build_claude_payload(snapshot, previous_reference=previous_reference)
        try:
            refresh_archive = save_analysis_archive(
                snapshot=snapshot,
                cycle_type=CYCLE_H1_DECISION_REFRESH,
                payload=payload,
                result=None,
                scout_result=scout_result,
                previous_reference=previous_reference,
                api_attempt={
                    "status": "PAYLOAD_SAVED_BEFORE_API",
                    "retry_policy": "CONTROLLED_BOUNDED",
                },
                note=(
                    "Scout не потребовал перестройки карты; сохранён отдельный "
                    "H1 decision payload до платного API-вызова."
                ),
            )
        except Exception as error:
            print(
                "[COST SAFETY] Не удалось сохранить H1 decision payload; "
                f"Claude API НЕ вызывается: {type(error).__name__}: {error}"
            )
            return

    decision_run = run_trade_decision_pipeline(
        payload,
        market_map=reference_analysis,
        previous_reference=previous_reference,
        snapshot=snapshot,
        cycle_type=CYCLE_H1_DECISION_REFRESH,
        archive_path=refresh_archive,
        decision_family="H1_DECISION",
    )

    if not decision_run.get("ok"):
        error = decision_run.get("error") or RuntimeError(
            "H1_DECISION micro-pipeline не завершён."
        )
        _register_api_retries_exhausted(
            snapshot=snapshot, api_stage="H1_DECISION_MICRO", error=error
        )
        print(
            "[BLOCKED] Свежая H1 проверка входа не завершена; "
            "полученные поля сохранены, ордер не создаётся."
        )
        return

    analysis = assemble_entry_refresh_analysis(
        reference_analysis=reference_analysis,
        decision=decision_run["result"],
        payload=payload,
    )
    save_debug_response(analysis)
    print_analysis_summary(analysis)
    safe_update_analysis_archive(
        refresh_archive,
        result=analysis,
        api_usage=decision_run.get("usage"),
        staged_analysis_version=STAGED_ANALYSIS_VERSION,
        note=(
            "H1 decision refresh validated against the last FULL map; "
            "fresh H1/M30/M15/M5 recommendation is ready for local gates."
        ),
    )

    if execution_observation_only:
        reason_text = "; ".join(dict.fromkeys(
            item for item in execution_block_reasons if item
        )) or "Execution lifecycle временно запрещает новые ордера."
        risk_report = {
            "decision": "NO_TRADE", "approved": False,
            "reasons": [reason_text], "observation_only": True,
        }
        state_result = {
            "state_action": "OBSERVATION_ONLY_EXECUTION_BLOCKED",
            "active_plan": None, "plan_changed": False,
            "observation_only": True, "reason": reason_text,
        }
        execution_report = {
            "decision": "OBSERVATION_ONLY", "order_send_called": False,
            "reason": reason_text,
        }
    else:
        watch_state = refresh_entry_watch(
            analysis,
            source_h1=extract_latest_closed_h1_time(snapshot),
        )
        print(
            "[ENTRY WATCH] "
            + ("Новый M15/M5 trigger поставлен на контроль."
               if watch_state.get("status") == "watching"
               else "Условный M15/M5 trigger не задан.")
        )
        risk_report = evaluate_trade(analysis=analysis, symbol=SYMBOL)
        print_risk_report(risk_report)
        state_result = register_trade_decision(
            analysis=analysis, risk_report=risk_report, snapshot=snapshot
        )
        print_trade_state_result(state_result)
        execution_report = None

    registration = register_completed_analysis(
        snapshot=snapshot,
        analysis=analysis,
        risk_report=risk_report,
        state_result=state_result,
        symbol=SYMBOL,
        cycle_type=CYCLE_H1_DECISION_REFRESH,
        scout_result=scout_result,
    )
    print_analysis_registration(registration)

    if execution_report is None:
        execution_report = execute_active_plan(snapshot=snapshot, symbol=SYMBOL)
        validation_report = execution_report.get("validation_report")
        if isinstance(validation_report, dict):
            print_executor_report(validation_report)
        print_live_execution_report(execution_report)

    safe_update_analysis_archive(
        refresh_archive,
        risk_report=risk_report,
        trade_state_result=state_result,
        analysis_registration=registration,
        execution_report=execution_report,
    )
    safe_update_analysis_archive(
        scout_archive,
        h1_decision_refresh_archive=str(refresh_archive),
        h1_decision_refresh_result={
            "claude_action": (analysis.get("recommendation") or {}).get("action"),
            "risk_decision": risk_report.get("decision"),
            "execution_decision": execution_report.get("decision"),
        },
    )
    _emit_trade_funnel(
        snapshot=snapshot,
        cycle_type=CYCLE_H1_DECISION_REFRESH,
        analysis=analysis,
        risk_report=risk_report,
        state_result=state_result,
        execution_report=execution_report,
    )


@observe("analysis")
def run_m30_decision_cycle(
    *,
    manage_connection: bool = False,
    analysis_only: bool = False,
) -> dict:
    """Run one additional entry decision on the xx:30 closed M30 bar.

    This cycle never rewrites the D1/H4/H1 map and never advances the H1
    analysis gate.  It may create a normal market/pending plan only after the
    same local contract, risk, Trade State and live executor gates used by the
    hourly decision.
    """


    # COST OPTIMIZATION V8.5.3  PHASE 2A M30 GATE
    from ai_cost_guard import paid_m30_enabled

    if not paid_m30_enabled():

        print()
        print(
            "[COST POLICY] Платный M30 DECISION отключён."
        )
        print(
            "[COST POLICY] M30/M15/M5 остаются частью "
            "event-driven H1/ENTRY_CHECK, но отдельный "
            "Claude-вызов на xx:30 не покупается."
        )

        return {
            "ok": True,
            "reason": "COST_POLICY_M30_DISABLED",
            "order_send_called": False,
        }

    connected = False
    try:
        if manage_connection:
            connected = connect_mt5()
            if not connected:
                return {"ok": False, "reason": "MT5_CONNECTION_FAILED"}

        snapshot = get_market_snapshot(SYMBOL)
        try:
            save_web_market_snapshot(snapshot)
        except Exception as error:
            print(f"[WEB SNAPSHOT WARNING] M30 snapshot не сохранён: {error}")

        previous_reference = get_fresh_reference_for_snapshot(snapshot)
        if not isinstance(previous_reference, dict):
            return {
                "ok": False,
                "reason": "NO_CURRENT_DAY_FULL_REFERENCE",
            }
        print()
        print("=" * 80)
        print("M30 DECISION REFRESH — ПРОМЕЖУТОЧНАЯ ПРОВЕРКА ВХОДА")
        print("=" * 80)
        print(
            "D1/H4/H1 карта сохранена; закрытая M30 проверяет setup, "
            "M15 подтверждает структуру, M5 уточняет trigger."
        )

        resume = _load_m30_decision_archive(snapshot)
        if resume is not None:
            archive_path, archived_record = resume
            payload = archived_record["payload"]
            archived_reference = archived_record.get("previous_reference")
            if isinstance(archived_reference, dict):
                previous_reference = archived_reference
            print(
                f"[API RESUME] M30_DECISION продолжает frozen payload из "
                f"{archive_path}."
            )
        else:
            payload = build_claude_payload(
                snapshot,
                previous_reference=previous_reference,
            )
            try:
                archive_path = save_analysis_archive(
                    snapshot=snapshot,
                    cycle_type=CYCLE_M30_DECISION_REFRESH,
                    payload=payload,
                    result=None,
                    previous_reference=previous_reference,
                    api_attempt={
                        "status": "PAYLOAD_SAVED_BEFORE_API",
                        "retry_policy": "SEQUENTIAL_DURABLE_RECOVERY",
                    },
                    note=(
                        "M30 decision frozen payload сохранён до платного "
                        "API-вызова; H1 analysis gate не изменяется."
                    ),
                )
            except Exception as error:
                return {
                    "ok": False,
                    "reason": "ARCHIVE_SAVE_FAILED",
                    "error": f"{type(error).__name__}: {error}",
                }

        reference_analysis = previous_reference.get("analysis")
        if not isinstance(reference_analysis, dict):
            return {"ok": False, "reason": "REFERENCE_ANALYSIS_MISSING"}

        decision_run = run_trade_decision_pipeline(
            payload,
            market_map=reference_analysis,
            previous_reference=previous_reference,
            snapshot=snapshot,
            cycle_type=CYCLE_M30_DECISION_REFRESH,
            archive_path=archive_path,
            decision_family="M30_DECISION",
        )

        if not decision_run.get("ok"):
            error = decision_run.get("error") or RuntimeError(
                "M30_DECISION micro-pipeline не завершён."
            )
            _register_api_retries_exhausted(
                snapshot=snapshot,
                api_stage="M30_DECISION_MICRO",
                error=error,
            )
            return {
                "ok": False,
                "reason": (
                    "AI_PERMANENT_BLOCKED"
                    if decision_run.get("permanent")
                    else "CLAUDE_RESPONSE_NOT_VALIDATED"
                ),
                "error": str(error),
                "archive_path": str(archive_path),
            }

        analysis = assemble_entry_refresh_analysis(
            reference_analysis=reference_analysis,
            decision=decision_run["result"],
            payload=payload,
        )
        save_debug_response(analysis)
        print_analysis_summary(analysis)
        safe_update_analysis_archive(
            archive_path,
            result=analysis,
            api_usage=decision_run.get("usage"),
            staged_analysis_version=STAGED_ANALYSIS_VERSION,
            note=(
                "M30 decision validated against current-day FULL map; "
                "H1 analysis state intentionally unchanged."
            ),
        )

        if analysis_only:
            risk_report = {
                "decision": "NO_TRADE",
                "approved": False,
                "reasons": ["M30 cycle запущен в observation-only."],
                "observation_only": True,
            }
            state_result = {
                "state_action": "OBSERVATION_ONLY_EXECUTION_BLOCKED",
                "active_plan": None,
                "plan_changed": False,
            }
            execution_report = {
                "decision": "OBSERVATION_ONLY",
                "order_send_called": False,
            }
        else:
            refresh_entry_watch(
                analysis,
                source_h1=extract_latest_closed_h1_time(snapshot),
            )
            risk_report = evaluate_trade(analysis=analysis, symbol=SYMBOL)
            print_risk_report(risk_report)
            state_result = register_trade_decision(
                analysis=analysis,
                risk_report=risk_report,
                snapshot=snapshot,
            )
            print_trade_state_result(state_result)
            execution_report = execute_active_plan(
                snapshot=snapshot,
                symbol=SYMBOL,
            )
            validation_report = execution_report.get("validation_report")
            if isinstance(validation_report, dict):
                print_executor_report(validation_report)
            print_live_execution_report(execution_report)

        safe_update_analysis_archive(
            archive_path,
            risk_report=risk_report,
            trade_state_result=state_result,
            execution_report=execution_report,
        )
        _emit_trade_funnel(
            snapshot=snapshot,
            cycle_type=CYCLE_M30_DECISION_REFRESH,
            analysis=analysis,
            risk_report=risk_report,
            state_result=state_result,
            execution_report=execution_report,
        )
        return {
            "ok": True,
            "archive_path": str(archive_path),
            "claude_action": (analysis.get("recommendation") or {}).get("action"),
            "risk_decision": risk_report.get("decision"),
            "execution_decision": execution_report.get("decision"),
            "order_send_called": bool(execution_report.get("order_send_called")),
        }
    finally:
        if manage_connection and connected:
            disconnect_mt5()


# ============================================================
# LIVE POSITION
# ============================================================

def get_live_position_by_ticket(
    ticket: int,
):
    """
    Получает открытую MT5-позицию
    по ticket.
    """

    positions = mt5.positions_get(
        ticket=int(
            ticket
        )
    )

    if positions is None:

        raise RuntimeError(
            "Не удалось выполнить "
            f"positions_get(ticket={ticket}). "
            f"MT5 error: {mt5.last_error()}"
        )

    if len(
        positions
    ) == 0:

        return None

    return positions[
        0
    ]


# ============================================================
# ИСТОРИЯ ЗАКРЫТИЯ ПОЗИЦИИ
# ============================================================

def _positive_int(value) -> int | None:
    """Возвращает положительный int или None."""

    try:
        result = int(
            value
        )
    except (
        TypeError,
        ValueError,
    ):
        return None

    if result <= 0:
        return None

    return result


def _append_unique_int(
    target: list[int],
    value,
):
    """Добавляет положительный int без дублей."""

    normalized = _positive_int(
        value
    )

    if (
        normalized is not None
        and
        normalized not in target
    ):
        target.append(
            normalized
        )


def recover_position_identifiers(
    managed_plan: dict,
) -> dict:
    """
    Восстанавливает стабильный MT5 POSITION_IDENTIFIER.

    ВАЖНО:

    POSITION_TICKET и POSITION_IDENTIFIER — не одно и то же.
    POSITION_TICKET может измениться из-за серверных сервисных
    операций, а POSITION_IDENTIFIER остаётся неизменным и
    записывается в DEAL_POSITION_ID / ORDER_POSITION_ID.

    Для старых Trade State, где identifier ещё не сохранялся,
    пытаемся восстановить его через исходный order ticket.
    """

    identifiers: list[int] = []
    order_tickets: list[int] = []
    diagnostics = []

    # Уже сохранённый identifier — самый сильный источник.
    _append_unique_int(
        identifiers,
        managed_plan.get(
            "position_identifier"
        ),
    )

    # Старый код мог хранить identifier в position_ticket,
    # поэтому оставляем его как fallback-кандидат.
    _append_unique_int(
        identifiers,
        managed_plan.get(
            "position_ticket"
        ),
    )

    # Тикеты ордеров, через которые можно получить
    # ORDER_POSITION_ID / DEAL_POSITION_ID.
    for key in (
        "mt5_order_ticket",
        "source_pending_ticket",
    ):
        _append_unique_int(
            order_tickets,
            managed_plan.get(
                key
            ),
        )

    # Для pending fill POSITION_TICKET часто совпадает
    # с исходным order ticket. Это только дополнительный fallback.
    _append_unique_int(
        order_tickets,
        managed_plan.get(
            "position_ticket"
        ),
    )

    # --------------------------------------------------------
    # ВОССТАНОВЛЕНИЕ ИЗ HISTORY ORDERS
    # --------------------------------------------------------

    for order_ticket in order_tickets:
        orders = mt5.history_orders_get(
            ticket=int(
                order_ticket
            )
        )

        if orders is None:
            diagnostics.append(
                "history_orders_get(ticket="
                f"{order_ticket}) -> None; "
                f"MT5 error: {mt5.last_error()}"
            )
        else:
            for order in orders:
                _append_unique_int(
                    identifiers,
                    getattr(
                        order,
                        "position_id",
                        None,
                    ),
                )

        # ----------------------------------------------------
        # ВОССТАНОВЛЕНИЕ ИЗ DEAL, СОЗДАННОГО ЭТИМ ORDER
        # ----------------------------------------------------

        deals_by_order = mt5.history_deals_get(
            ticket=int(
                order_ticket
            )
        )

        if deals_by_order is None:
            ticket_error = mt5.last_error()
            # Some MT5 Python/terminal builds reject the documented ticket=
            # form with (-2, "Invalid params").  Range lookup plus an exact
            # DEAL_ORDER filter is the compatible, ownership-safe fallback.
            range_deals = mt5.history_deals_get(
                datetime.now(timezone.utc) - timedelta(days=30),
                datetime.now(timezone.utc) + timedelta(minutes=5),
            )
            if range_deals is None:
                diagnostics.append(
                    "history_deals_get(ticket="
                    f"{order_ticket}) -> None ({ticket_error}); "
                    "range fallback -> None; "
                    f"MT5 error: {mt5.last_error()}"
                )
                deals_by_order = ()
            else:
                deals_by_order = tuple(
                    deal for deal in range_deals
                    if int(getattr(deal, "order", 0) or 0)
                    == int(order_ticket)
                )
                diagnostics.append(
                    "history_deals_get(ticket="
                    f"{order_ticket}) unsupported ({ticket_error}); "
                    "used range fallback with exact DEAL_ORDER filter."
                )

        for deal in deals_by_order:
            _append_unique_int(
                identifiers,
                getattr(
                    deal,
                    "position_id",
                    None,
                ),
            )

    return {
        "identifiers": identifiers,
        "order_tickets": order_tickets,
        "diagnostics": diagnostics,
    }


def _find_exit_deals(
    deals,
    action: str | None,
) -> tuple[list, str]:
    """
    Находит closing deals.

    Основной путь — DEAL_ENTRY_OUT/OUT_BY/INOUT.
    Дополнительный fallback нужен для брокерских историй,
    где entry может быть представлен нетипично: при нашей
    политике без partial close и reversal закрытие LONG должно
    быть SELL deal, а закрытие SHORT — BUY deal.
    """

    deal_entry_out = int(
        getattr(
            mt5,
            "DEAL_ENTRY_OUT",
            1,
        )
    )

    deal_entry_out_by = int(
        getattr(
            mt5,
            "DEAL_ENTRY_OUT_BY",
            3,
        )
    )

    deal_entry_inout = int(
        getattr(
            mt5,
            "DEAL_ENTRY_INOUT",
            2,
        )
    )

    closing_entries = {
        deal_entry_out,
        deal_entry_out_by,
        deal_entry_inout,
    }

    exit_deals = [
        deal
        for deal in deals
        if int(
            getattr(
                deal,
                "entry",
                -1,
            )
        )
        in closing_entries
    ]

    if exit_deals:
        return (
            exit_deals,
            "DEAL_ENTRY",
        )

    # --------------------------------------------------------
    # FALLBACK ПО НАПРАВЛЕНИЮ DEAL
    # --------------------------------------------------------

    expected_close_type = None

    if action == "enter_long":
        expected_close_type = int(
            mt5.DEAL_TYPE_SELL
        )
    elif action == "enter_short":
        expected_close_type = int(
            mt5.DEAL_TYPE_BUY
        )

    if expected_close_type is None:
        return (
            [],
            "NONE",
        )

    directional_matches = [
        deal
        for deal in deals
        if int(
            getattr(
                deal,
                "type",
                -1,
            )
        )
        == expected_close_type
        and float(
            getattr(
                deal,
                "volume",
                0.0,
            )
        )
        > 0
    ]

    if directional_matches:
        return (
            directional_matches,
            "DEAL_DIRECTION_FALLBACK",
        )

    return (
        [],
        "NONE",
    )


def get_position_close_info(
    managed_plan: dict,
) -> dict:
    """
    Если managed position больше нет среди открытых позиций MT5,
    подтверждает её закрытие через историю MT5.

    Ключевой принцип:

        POSITION_TICKET используется для open-position lookup;
        POSITION_IDENTIFIER / DEAL_POSITION_ID используется для
        восстановления полного жизненного цикла позиции.

    Поддерживает старые Trade State, где position_identifier
    ещё не сохранялся.
    """

    position_ticket = _positive_int(
        managed_plan.get(
            "position_ticket"
        )
    )

    plan_id = managed_plan.get(
        "plan_id"
    )

    recovery = recover_position_identifiers(
        managed_plan
    )

    candidate_identifiers = recovery[
        "identifiers"
    ]

    diagnostics = list(
        recovery[
            "diagnostics"
        ]
    )

    if not candidate_identifiers:
        return {
            "confirmed_closed": False,
            "close_reason": "UNKNOWN",
            "close_price": None,
            "close_time_fp": None,
            "net_result": None,
            "deal_ticket": None,
            "raw_reason": None,
            "position_identifier": None,
            "lookup_method": None,
            "deals_count": 0,
            "error": (
                "Не удалось восстановить POSITION_IDENTIFIER "
                f"для Plan ID {plan_id}, position ticket "
                f"#{position_ticket}."
            ),
            "diagnostics": diagnostics,
        }

    selected_deals = None
    selected_identifier = None
    selected_exit_deals = None
    selected_exit_method = None

    # ========================================================
    # ИЩЕМ DATASET, В КОТОРОМ ЕСТЬ CLOSING DEAL
    # ========================================================

    for position_identifier in candidate_identifiers:
        deals = mt5.history_deals_get(
            position=int(
                position_identifier
            )
        )

        if deals is None:
            position_error = mt5.last_error()
            range_deals = mt5.history_deals_get(
                datetime.now(timezone.utc) - timedelta(days=90),
                datetime.now(timezone.utc) + timedelta(minutes=5),
            )
            if range_deals is None:
                diagnostics.append(
                    "history_deals_get(position="
                    f"{position_identifier}) -> None ({position_error}); "
                    "range fallback -> None; "
                    f"MT5 error: {mt5.last_error()}"
                )
                continue
            deals = tuple(
                deal for deal in range_deals
                if int(getattr(deal, "position_id", 0) or 0)
                == int(position_identifier)
            )
            diagnostics.append(
                "history_deals_get(position="
                f"{position_identifier}) unsupported ({position_error}); "
                "used range fallback with exact DEAL_POSITION_ID filter."
            )

        if len(
            deals
        ) == 0:
            diagnostics.append(
                "history_deals_get(position="
                f"{position_identifier}) -> 0 deals."
            )
            continue

        exit_deals, exit_method = _find_exit_deals(
            deals=deals,
            action=managed_plan.get(
                "action"
            ),
        )

        if exit_deals:
            selected_deals = deals
            selected_identifier = int(
                position_identifier
            )
            selected_exit_deals = exit_deals
            selected_exit_method = exit_method
            break

        diagnostics.append(
            "Для POSITION_IDENTIFIER "
            f"{position_identifier} найдено {len(deals)} deals, "
            "но closing deal среди них не определён."
        )

    if (
        selected_deals is None
        or
        selected_exit_deals is None
    ):
        return {
            "confirmed_closed": False,
            "close_reason": "UNKNOWN",
            "close_price": None,
            "close_time_fp": None,
            "net_result": None,
            "deal_ticket": None,
            "raw_reason": None,
            "position_identifier": None,
            "lookup_method": None,
            "deals_count": 0,
            "error": (
                "Позиция отсутствует среди открытых, но "
                "закрывающий deal не найден после проверки "
                "POSITION_IDENTIFIER. Candidates: "
                f"{candidate_identifiers}."
            ),
            "diagnostics": diagnostics,
        }

    # ========================================================
    # ПОСЛЕДНИЙ EXIT DEAL
    # ========================================================

    last_exit = max(
        selected_exit_deals,
        key=lambda deal: int(
            getattr(
                deal,
                "time_msc",
                0,
            )
            or (
                int(
                    getattr(
                        deal,
                        "time",
                        0,
                    )
                )
                * 1000
            )
        ),
    )

    raw_reason = int(
        getattr(
            last_exit,
            "reason",
            -1,
        )
    )

    # ========================================================
    # ПРИЧИНА ЗАКРЫТИЯ
    # ========================================================

    reason_sl = int(
        getattr(
            mt5,
            "DEAL_REASON_SL",
            -1001,
        )
    )

    reason_tp = int(
        getattr(
            mt5,
            "DEAL_REASON_TP",
            -1002,
        )
    )

    reason_so = int(
        getattr(
            mt5,
            "DEAL_REASON_SO",
            -1003,
        )
    )

    if raw_reason == reason_sl:
        close_reason = "STOP_LOSS"
    elif raw_reason == reason_tp:
        close_reason = "TAKE_PROFIT"
    elif raw_reason == reason_so:
        close_reason = "STOP_OUT"
    else:
        close_reason = "OTHER"

    # ========================================================
    # ФИНАНСОВЫЙ РЕЗУЛЬТАТ
    # ========================================================

    net_result = 0.0

    for deal in selected_deals:
        net_result += float(
            getattr(
                deal,
                "profit",
                0.0,
            )
        )
        net_result += float(
            getattr(
                deal,
                "commission",
                0.0,
            )
        )
        net_result += float(
            getattr(
                deal,
                "swap",
                0.0,
            )
        )
        net_result += float(
            getattr(
                deal,
                "fee",
                0.0,
            )
        )

    # ========================================================
    # FUNDINGPIPS TIME
    # ========================================================

    close_time_fp = None

    try:
        close_time_fp = mt5_timestamp_to_fp(
            int(
                last_exit.time
            )
        ).isoformat()
    except Exception:
        close_time_fp = None

    # Определяем, как identifier был получен.
    stored_identifier = _positive_int(
        managed_plan.get(
            "position_identifier"
        )
    )

    if (
        stored_identifier is not None
        and
        stored_identifier == selected_identifier
    ):
        identifier_source = "TRADE_STATE"
    else:
        identifier_source = "RECOVERED_FROM_MT5_HISTORY"

    return {
        "confirmed_closed": True,
        "close_reason": close_reason,
        "close_price": float(
            last_exit.price
        ),
        "close_time_fp": close_time_fp,
        "net_result": net_result,
        "deal_ticket": int(
            last_exit.ticket
        ),
        "raw_reason": raw_reason,
        "position_identifier": selected_identifier,
        "identifier_source": identifier_source,
        "lookup_method": selected_exit_method,
        "deals_count": len(
            selected_deals
        ),
        "error": None,
        "diagnostics": diagnostics,
    }


# ============================================================
# ПРОВЕРКА ОТКРЫТОЙ MANAGED POSITION
# ============================================================

def inspect_live_managed_position(
    managed_plan: dict,
    live_position,
) -> dict:
    """
    Сверяет реальную позицию MT5
    с Trade State.

    Ничего не изменяет автоматически.
    """

    errors = []

    warnings = []

    plan_id = managed_plan.get(
        "plan_id"
    )

    position_ticket = int(
        managed_plan[
            "position_ticket"
        ]
    )

    expected_symbol = str(
        managed_plan.get(
            "symbol",
            SYMBOL,
        )
    )

    expected_action = (
        managed_plan.get(
            "action"
        )
    )

    expected_volume = float(
        managed_plan.get(
            "volume",
            0.0,
        )
    )

    expected_sl = float(
        managed_plan.get(
            "stop_loss",
            0.0,
        )
    )

    expected_tp = float(
        managed_plan.get(
            "take_profit",
            0.0,
        )
    )

    actual_symbol = str(
        live_position.symbol
    )

    actual_volume = float(
        live_position.volume
    )

    actual_sl = float(
        live_position.sl
    )

    actual_tp = float(
        live_position.tp
    )

    actual_open_price = float(
        live_position.price_open
    )

    actual_profit = float(
        live_position.profit
    )

    actual_type = int(
        live_position.type
    )

    # ========================================================
    # SYMBOL
    # ========================================================

    if (
        actual_symbol
        != expected_symbol
    ):

        errors.append(
            "Symbol позиции не совпадает "
            "с Trade State: "
            f"{actual_symbol} != "
            f"{expected_symbol}."
        )

    # ========================================================
    # DIRECTION
    # ========================================================

    if (
        expected_action
        == "enter_long"
    ):

        expected_type = (
            mt5.POSITION_TYPE_BUY
        )

    elif (
        expected_action
        == "enter_short"
    ):

        expected_type = (
            mt5.POSITION_TYPE_SELL
        )

    else:

        expected_type = None

        errors.append(
            "Trade State содержит "
            "неизвестный action: "
            f"{expected_action}."
        )

    if (
        expected_type is not None
        and
        actual_type
        != expected_type
    ):

        errors.append(
            "Направление реальной позиции "
            "не совпадает с Trade State."
        )

    # ========================================================
    # SYMBOL INFO
    # ========================================================

    info = mt5.symbol_info(
        actual_symbol
    )

    if info is None:

        errors.append(
            "Не удалось получить "
            f"symbol_info({actual_symbol})."
        )

        price_tolerance = (
            0.00001
        )

        volume_tolerance = (
            0.00000001
        )

    else:

        price_tolerance = max(
            float(
                info.point
            )
            * 2.0,
            0.00001,
        )

        volume_tolerance = max(
            float(
                info.volume_step
            )
            / 10.0,
            0.00000001,
        )

    # ========================================================
    # VOLUME
    # ========================================================

    if (
        abs(
            actual_volume
            - expected_volume
        )
        > volume_tolerance
    ):

        errors.append(
            "Объём реальной позиции "
            "не совпадает с Trade State: "
            f"{actual_volume} != "
            f"{expected_volume}."
        )

    # ========================================================
    # STOP LOSS
    # ========================================================

    if (
        actual_sl
        <= 0
    ):

        errors.append(
            "КРИТИЧЕСКИ: у открытой позиции "
            "отсутствует Stop Loss."
        )

    elif (
        abs(
            actual_sl
            - expected_sl
        )
        > price_tolerance
    ):

        errors.append(
            "Stop Loss реальной позиции "
            "изменён: "
            f"MT5={actual_sl}, "
            f"Trade State={expected_sl}."
        )

    # ========================================================
    # TAKE PROFIT
    # ========================================================

    if (
        actual_tp
        <= 0
    ):

        errors.append(
            "КРИТИЧЕСКИ: у открытой позиции "
            "отсутствует Take Profit."
        )

    elif (
        abs(
            actual_tp
            - expected_tp
        )
        > price_tolerance
    ):

        errors.append(
            "Take Profit реальной позиции "
            "изменён: "
            f"MT5={actual_tp}, "
            f"Trade State={expected_tp}."
        )

    return {

        "plan_id": (
            plan_id
        ),

        "position_ticket": (
            position_ticket
        ),

        "position_identifier": int(
            getattr(
                live_position,
                "identifier",
                position_ticket,
            )
        ),

        "symbol": (
            actual_symbol
        ),

        "action": (
            expected_action
        ),

        "volume": (
            actual_volume
        ),

        "open_price": (
            actual_open_price
        ),

        "stop_loss": (
            actual_sl
        ),

        "take_profit": (
            actual_tp
        ),

        "floating_profit": (
            actual_profit
        ),

        "errors": (
            errors
        ),

        "warnings": (
            warnings
        ),
    }


# ============================================================
# POSITION GATE
# ============================================================

@observe("analysis")
def inspect_position_gate(
    symbol: str = SYMBOL,
) -> dict:
    """
    Выполняется раньше Claude.

    Если позиция уже открыта:
        новый ENTRY/FULL pipeline Claude запрещён. Отдельный
        POSITION_REVIEW запускается runner-ом через position_monitor.py.

    Если позиция только что закрылась:
        lifecycle завершается,
        но новый Claude-анализ в этом же цикле
        не выполняется.

    Если состояние MT5 не совпадает
    с Trade State:
        Claude запрещён.
    """

    managed_positions = (
        get_managed_positions()
    )

    live_managed = []

    closed_positions = []

    unresolved_positions = []

    managed_live_tickets = set()

    # ========================================================
    # MANAGED POSITIONS
    # ========================================================

    for managed_plan in managed_positions:

        position_ticket = (
            managed_plan.get(
                "position_ticket"
            )
        )

        if position_ticket is None:

            unresolved_positions.append({

                "plan_id": (
                    managed_plan.get(
                        "plan_id"
                    )
                ),

                "position_ticket": None,

                "error": (
                    "Managed position не содержит "
                    "position_ticket."
                ),
            })

            continue

        position_ticket = int(
            position_ticket
        )

        live_position = (
            get_live_position_by_ticket(
                position_ticket
            )
        )

        # ====================================================
        # ПОЗИЦИЯ ЕЩЁ ЖИВА
        # ====================================================

        if live_position is not None:

            managed_live_tickets.add(
                int(
                    live_position.ticket
                )
            )

            live_managed.append(
                inspect_live_managed_position(
                    managed_plan=managed_plan,
                    live_position=live_position,
                )
            )

            continue

        # ====================================================
        # ПОЗИЦИЯ ПРОПАЛА ИЗ OPEN POSITIONS
        # ====================================================

        close_info = (
            get_position_close_info(
                managed_plan
            )
        )

        if not close_info[
            "confirmed_closed"
        ]:

            unresolved_positions.append({

                "plan_id": (
                    managed_plan.get(
                        "plan_id"
                    )
                ),

                "position_ticket": (
                    position_ticket
                ),

                "error": (
                    close_info[
                        "error"
                    ]
                ),

                "diagnostics": (
                    close_info.get(
                        "diagnostics",
                        [],
                    )
                ),
            })

            continue

        # ====================================================
        # ПОДТВЕРЖДЁННОЕ ЗАКРЫТИЕ
        # ====================================================

        close_reason = (
            close_info[
                "close_reason"
            ]
        )

        state_reason = (
            f"MT5 подтвердил закрытие позиции "
            f"#{position_ticket}. "
            f"Причина: {close_reason}. "
            f"Цена: {close_info['close_price']}. "
            f"Net result: "
            f"{close_info['net_result']:.2f}."
        )

        mark_managed_position_closed(
            reason=state_reason,
            position_ticket=(
                position_ticket
            ),
            close_info=(
                close_info
            ),
        )

        closed_positions.append({

            "plan_id": (
                managed_plan.get(
                    "plan_id"
                )
            ),

            "position_ticket": (
                position_ticket
            ),

            **close_info,
        })

    # ========================================================
    # ВСЕ РЕАЛЬНЫЕ XAUUSD POSITIONS
    # ========================================================

    real_positions = mt5.positions_get(
        symbol=symbol
    )

    if real_positions is None:

        raise RuntimeError(
            "Не удалось выполнить "
            f"positions_get(symbol={symbol}). "
            f"MT5 error: {mt5.last_error()}"
        )

    unmanaged_positions = []

    for position in real_positions:

        ticket = int(
            position.ticket
        )

        if (
            ticket
            not in managed_live_tickets
        ):

            unmanaged_positions.append({

                "ticket": (
                    ticket
                ),

                "symbol": str(
                    position.symbol
                ),

                "volume": float(
                    position.volume
                ),

                "price_open": float(
                    position.price_open
                ),

                "sl": float(
                    position.sl
                ),

                "tp": float(
                    position.tp
                ),

                "profit": float(
                    position.profit
                ),
            })

    # ========================================================
    # BLOCK CONDITIONS
    # ========================================================

    has_live_managed = (
        len(
            live_managed
        )
        > 0
    )

    has_unresolved = (
        len(
            unresolved_positions
        )
        > 0
    )

    has_unmanaged = (
        len(
            unmanaged_positions
        )
        > 0
    )

    position_just_closed = (
        len(
            closed_positions
        )
        > 0
    )

    block_claude = (
        has_live_managed
        or
        has_unresolved
        or
        has_unmanaged
        or
        position_just_closed
    )

    return {

        "block_claude": (
            block_claude
        ),

        "position_just_closed": (
            position_just_closed
        ),

        "live_managed": (
            live_managed
        ),

        "closed_positions": (
            closed_positions
        ),

        "unresolved_positions": (
            unresolved_positions
        ),

        "unmanaged_positions": (
            unmanaged_positions
        ),
    }


# ============================================================
# HOLD RISK CONTEXT
# ============================================================

def build_hold_risk_context() -> dict:
    """
    Формирует фактический risk context для технического audit и отдельного
    POSITION_REVIEW.
    """

    account = get_account_info()

    positions = get_positions()

    daily_state = get_daily_state(
        account,
        positions,
    )

    base_limits = (
        calculate_fundingpips_limits(
            daily_state
        )
    )

    open_risk = (
        calculate_open_positions_risk()
    )

    balance = float(
        account.balance
    )

    equity = float(
        account.equity
    )

    free_margin = float(
        account.margin_free
    )

    existing_positions_risk = float(
        open_risk[
            "total_remaining_risk"
        ]
    )

    limits = enrich_limits(
        limits=base_limits,
        balance=balance,
        equity=equity,
        existing_positions_risk=(
            existing_positions_risk
        ),
    )

    return {

        "account": {

            "balance": (
                balance
            ),

            "equity": (
                equity
            ),

            "free_margin": (
                free_margin
            ),
        },

        "daily_state": (
            daily_state
        ),

        "limits": (
            limits
        ),

        "open_risk": (
            open_risk
        ),
    }


# ============================================================
# HOLD REPORT
# ============================================================

def print_position_hold_report(
    gate: dict,
    risk_context: dict,
):
    """
    Выводит POSITION HOLD MODE.
    """

    print()
    print("=" * 80)
    print(
        "POSITION HOLD MODE"
    )
    print("=" * 80)

    print(
        f"Project mode:    "
        f"{PROJECT_MODE}"
    )

    print("Strategy:        ENTRY -> MONITOR -> SL / TP")

    print("New entry Claude: DISABLED")
    print("Position review:  H4/H1/M30/M15/M5 + protected SL/TP management")

    # ========================================================
    # LIVE MANAGED POSITIONS
    # ========================================================

    live_managed = gate[
        "live_managed"
    ]

    print()
    print(
        "УПРАВЛЯЕМЫЕ ОТКРЫТЫЕ ПОЗИЦИИ"
    )
    print("-" * 80)

    if not live_managed:

        print(
            "Нет."
        )

    else:

        for index, position in enumerate(
            live_managed,
            start=1,
        ):

            print()
            print(
                f"[{index}]"
            )

            print(
                f"Plan ID:         "
                f"{position['plan_id']}"
            )

            print(
                f"Position ticket: "
                f"{position['position_ticket']}"
            )

            print(
                f"Position ID:     "
                f"{position['position_identifier']}"
            )

            print(
                f"Symbol:          "
                f"{position['symbol']}"
            )

            print(
                f"Action:          "
                f"{position['action']}"
            )

            print(
                f"Volume:          "
                f"{position['volume']}"
            )

            print(
                f"Open price:      "
                f"{position['open_price']}"
            )

            print(
                f"Stop Loss:       "
                f"{position['stop_loss']}"
            )

            print(
                f"Take Profit:     "
                f"{position['take_profit']}"
            )

            print(
                f"Floating P/L:    "
                f"{position['floating_profit']:.2f}"
            )

            if position[
                "errors"
            ]:

                print()
                print(
                    "ТЕХНИЧЕСКИЕ ПРОБЛЕМЫ:"
                )

                for error in position[
                    "errors"
                ]:

                    print(
                        f"- {error}"
                    )

    # ========================================================
    # JUST CLOSED
    # ========================================================

    if gate[
        "closed_positions"
    ]:

        print()
        print(
            "ПОЗИЦИИ, ЗАКРЫТЫЕ НА ЭТОМ ЦИКЛЕ"
        )
        print("-" * 80)

        for position in gate[
            "closed_positions"
        ]:

            print()
            print(
                f"Plan ID:         "
                f"{position['plan_id']}"
            )

            print(
                f"Position ticket: "
                f"{position['position_ticket']}"
            )

            print(
                f"Position ID:     "
                f"{position.get('position_identifier')}"
            )

            print(
                f"Close deal:      "
                f"{position.get('deal_ticket')}"
            )

            print(
                f"History lookup:  "
                f"{position.get('identifier_source')} / "
                f"{position.get('lookup_method')}"
            )

            print(
                f"Close reason:    "
                f"{position['close_reason']}"
            )

            print(
                f"Close price:     "
                f"{position['close_price']}"
            )

            print(
                f"Close FP time:   "
                f"{position['close_time_fp']}"
            )

            print(
                f"Net result:      "
                f"{position['net_result']:.2f}"
            )

    # ========================================================
    # RECONCILIATION
    # ========================================================

    if gate[
        "unresolved_positions"
    ]:

        print()
        print(
            "RECONCILIATION ERRORS"
        )
        print("-" * 80)

        for position in gate[
            "unresolved_positions"
        ]:

            print(
                f"- Plan "
                f"{position['plan_id']}, "
                f"ticket "
                f"{position['position_ticket']}: "
                f"{position['error']}"
            )

            for diagnostic in position.get(
                "diagnostics",
                [],
            ):
                print(
                    f"  * {diagnostic}"
                )

    # ========================================================
    # UNKNOWN POSITIONS
    # ========================================================

    if gate[
        "unmanaged_positions"
    ]:

        print()
        print(
            "НЕИЗВЕСТНЫЕ ПОЗИЦИИ В MT5"
        )
        print("-" * 80)

        for position in gate[
            "unmanaged_positions"
        ]:

            print()

            print(
                f"Ticket:          "
                f"{position['ticket']}"
            )

            print(
                f"Symbol:          "
                f"{position['symbol']}"
            )

            print(
                f"Volume:          "
                f"{position['volume']}"
            )

            print(
                f"Open price:      "
                f"{position['price_open']}"
            )

            print(
                f"SL:              "
                f"{position['sl']}"
            )

            print(
                f"TP:              "
                f"{position['tp']}"
            )

            print(
                f"Floating P/L:    "
                f"{position['profit']:.2f}"
            )

    # ========================================================
    # ACCOUNT
    # ========================================================

    account = risk_context[
        "account"
    ]

    print()
    print(
        "СЧЁТ"
    )
    print("-" * 80)

    print(
        f"Balance:         "
        f"{account['balance']:.2f}"
    )

    print(
        f"Equity:          "
        f"{account['equity']:.2f}"
    )

    print(
        f"Free Margin:     "
        f"{account['free_margin']:.2f}"
    )

    # ========================================================
    # FUNDINGPIPS DAY
    # ========================================================

    daily_state = (
        risk_context[
            "daily_state"
        ]
    )

    print()
    print(
        "FUNDINGPIPS DAY"
    )
    print("-" * 80)

    print(
        f"Day:             "
        f"{daily_state['fp_day']}"
    )

    print(
        f"Opening Balance: "
        f"{daily_state['opening_balance']:.2f}"
    )

    print(
        f"Opening Equity:  "
        f"{daily_state['opening_equity']:.2f}"
    )

    print(
        f"Daily baseline:  "
        f"{daily_state['daily_baseline']:.2f}"
    )

    print(
        f"Trusted:         "
        f"{daily_state['trusted']}"
    )

    # ========================================================
    # LIMITS
    # ========================================================

    limits = (
        risk_context[
            "limits"
        ]
    )

    print()
    print(
        "FUNDINGPIPS LIMITS"
    )
    print("-" * 80)

    print(
        f"Daily Floor:     "
        f"{limits['daily_floor']:.2f}"
    )

    print(
        f"Daily Room:      "
        f"{limits['current_daily_room']:.2f}"
    )

    print(
        f"Max Floor:       "
        f"{limits['max_loss_floor']:.2f}"
    )

    print(
        f"Max Room:        "
        f"{limits['current_max_room']:.2f}"
    )

    # ========================================================
    # OPEN RISK
    # ========================================================

    open_risk = (
        risk_context[
            "open_risk"
        ]
    )

    print()
    print(
        "OPEN POSITION RISK"
    )
    print("-" * 80)

    print(
        f"Positions:       "
        f"{open_risk['positions_count']}"
    )

    print(
        f"Risk known:      "
        f"{open_risk['risk_known']}"
    )

    print(
        f"Risk to SL:      "
        f"{open_risk['total_remaining_risk']:.2f}"
    )

    if open_risk[
        "problems"
    ]:

        print()

        for problem in open_risk[
            "problems"
        ]:

            print(
                f"- {problem}"
            )

    # ========================================================
    # FINAL
    # ========================================================

    print()
    print("=" * 80)

    if gate[
        "live_managed"
    ]:

        print(
            "[HOLD] Позиция открыта."
        )

        print(
            "[POSITION AUDIT] Этот технический проход не вызывает Claude; M15/H1 анализ запускает runner."
        )

        print(
            "[POSITION AUDIT] Этот проход не меняет SL/TP; проверенный план исполняет position_protection."
        )

        print(
            "[HOLD] Ждём Stop Loss "
            "или Take Profit."
        )

    elif gate[
        "closed_positions"
    ]:

        print(
            "[POSITION CLOSED] "
            "MT5 подтвердил закрытие позиции."
        )

        print(
            "[POSITION CLOSED] "
            "Trade State обновлён."
        )

        print(
            "[WAIT] Новый поиск входа "
            "начнётся на следующем цикле."
        )

    elif gate[
        "unresolved_positions"
    ]:

        print(
            "[BLOCKED] "
            "Состояние позиции "
            "не удалось согласовать."
        )

        print(
            "[BLOCKED] "
            "Claude API НЕ вызывается."
        )

    elif gate[
        "unmanaged_positions"
    ]:

        print(
            "[BLOCKED] "
            "В MT5 существует позиция, "
            "которой нет в Trade State."
        )

        print(
            "[BLOCKED] "
            "Claude API НЕ вызывается."
        )

    print("=" * 80)


# ============================================================
# BOOTSTRAP ANALYSIS STATE
# ============================================================

def bootstrap_analysis_state_from_existing_plan(
    snapshot: dict,
    symbol: str = SYMBOL,
) -> dict:
    """
    Нужен для перехода со старой версии проекта
    на H1 Analysis Gate.

    Если Trade State уже содержит активный план
    для текущей закрытой H1, значит:

        Claude уже был вызван;
        Risk Manager уже завершён;
        Trade State уже создан.

    Поэтому повторно платить за анализ Claude
    той же H1 нельзя.

    В этом случае Analysis State
    восстанавливается из существующего плана.
    """

    current_h1 = (
        extract_latest_closed_h1_time(
            snapshot
        )
    )

    if not current_h1:

        return {

            "bootstrapped": False,

            "reason": (
                "Текущая закрытая H1 "
                "не определена."
            ),

            "h1": None,

            "plan_id": None,
        }

    state = (
        load_analysis_state()
    )

    last_analyzed_h1 = (
        state.get(
            "last_analyzed_h1"
        )
    )

    # ========================================================
    # УЖЕ ЗАРЕГИСТРИРОВАНА
    # ========================================================

    if (
        last_analyzed_h1 is not None
        and
        str(
            last_analyzed_h1
        )
        ==
        str(
            current_h1
        )
    ):

        return {

            "bootstrapped": False,

            "reason": (
                "Текущая H1 уже есть "
                "в Analysis State."
            ),

            "h1": (
                current_h1
            ),

            "plan_id": (
                state.get(
                    "last_plan_id"
                )
            ),
        }

    # ========================================================
    # ACTIVE PLAN
    # ========================================================

    active_plan = (
        get_active_plan()
    )

    if active_plan is None:

        return {

            "bootstrapped": False,

            "reason": (
                "Активного Trade Plan нет."
            ),

            "h1": (
                current_h1
            ),

            "plan_id": None,
        }

    plan_h1 = (
        active_plan.get(
            "source_h1_closed_bar_time"
        )
    )

    if not plan_h1:

        return {

            "bootstrapped": False,

            "reason": (
                "Active Plan не содержит "
                "source_h1_closed_bar_time."
            ),

            "h1": (
                current_h1
            ),

            "plan_id": (
                active_plan.get(
                    "plan_id"
                )
            ),
        }

    # ========================================================
    # PLAN ОТНОСИТСЯ К ДРУГОЙ H1
    # ========================================================

    if (
        str(
            plan_h1
        )
        !=
        str(
            current_h1
        )
    ):

        return {

            "bootstrapped": False,

            "reason": (
                "Active Plan относится "
                "к другой закрытой H1."
            ),

            "h1": (
                current_h1
            ),

            "plan_id": (
                active_plan.get(
                    "plan_id"
                )
            ),
        }

    # ========================================================
    # ПРОВЕРКА HISTORY
    # ========================================================

    history = state.setdefault(
        "history",
        [],
    )

    existing_record = None

    for record in history:

        if (
            str(
                record.get(
                    "h1_closed_bar_time"
                )
            )
            ==
            str(
                current_h1
            )
        ):

            existing_record = (
                record
            )

            break

    analysis_time = (
        active_plan.get(
            "source_analysis_time"
        )
        or
        active_plan.get(
            "created_at_fp"
        )
        or
        now_fp().isoformat()
    )

    # ========================================================
    # CREATE HISTORY RECORD
    # ========================================================

    if existing_record is None:

        history_record = {

            "h1_closed_bar_time": (
                current_h1
            ),

            "analysis_time_fp": (
                analysis_time
            ),

            "registered_at_fp": (
                now_fp().isoformat()
            ),

            "symbol": (
                symbol
            ),

            "risk_decision": (
                "APPROVED"
            ),

            "risk_approved": True,

            "claude_action": (
                active_plan.get(
                    "action"
                )
            ),

            "order_type": (
                active_plan.get(
                    "order_type"
                )
            ),

            "confidence": (
                active_plan.get(
                    "confidence"
                )
            ),

            "trade_state_action": (
                "BOOTSTRAP_EXISTING_PLAN"
            ),

            "plan_id": (
                active_plan.get(
                    "plan_id"
                )
            ),
        }

        history.append(
            history_record
        )

    # ========================================================
    # UPDATE CURRENT ANALYSIS STATE
    # ========================================================

    state[
        "last_analyzed_h1"
    ] = current_h1

    state[
        "last_analysis_time_fp"
    ] = analysis_time

    state[
        "last_risk_decision"
    ] = "APPROVED"

    state[
        "last_claude_action"
    ] = (
        active_plan.get(
            "action"
        )
    )

    state[
        "last_order_type"
    ] = (
        active_plan.get(
            "order_type"
        )
    )

    state[
        "last_confidence"
    ] = (
        active_plan.get(
            "confidence"
        )
    )

    state[
        "last_trade_state_action"
    ] = (
        "BOOTSTRAP_EXISTING_PLAN"
    )

    state[
        "last_plan_id"
    ] = (
        active_plan.get(
            "plan_id"
        )
    )

    state[
        "symbol"
    ] = (
        symbol
    )

    save_analysis_state(
        state
    )

    return {

        "bootstrapped": True,

        "reason": (
            "Analysis State восстановлен "
            "из существующего Active Plan."
        ),

        "h1": (
            current_h1
        ),

        "plan_id": (
            active_plan.get(
                "plan_id"
            )
        ),

        "analysis_time": (
            analysis_time
        ),
    }


# ============================================================
# PRINT BOOTSTRAP
# ============================================================

def print_analysis_bootstrap(
    result: dict,
):
    """
    Выводит результат bootstrap.
    """

    if not result[
        "bootstrapped"
    ]:

        return

    print()
    print("=" * 80)
    print(
        "H1 ANALYSIS STATE BOOTSTRAP"
    )
    print("=" * 80)

    print(
        f"H1:              "
        f"{result['h1']}"
    )

    print(
        f"Plan ID:         "
        f"{result['plan_id']}"
    )

    print(
        f"Analysis time:   "
        f"{result['analysis_time']}"
    )

    print()

    print(
        "[OK] Существующий Trade Plan "
        "признан результатом уже "
        "выполненного Claude-анализа."
    )

    print(
        "[OK] Повторный вызов Claude "
        "для этой H1 запрещён."
    )

    print("=" * 80)


# ============================================================
# EXECUTOR FINAL SUMMARY
# ============================================================

def print_executor_final_summary(
    execution_report: dict,
    source: str,
):
    """
    Короткий итог Executor после validation
    и, если разрешено, LIVE execution.
    """

    validation_report = (
        execution_report.get(
            "validation_report"
        )
        or {}
    )

    validation_decision = (
        validation_report.get(
            "decision"
        )
    )

    execution_decision = (
        execution_report.get(
            "decision"
        )
    )

    mode = (
        execution_report.get(
            "mode"
        )
    )

    order_send_called = bool(
        execution_report.get(
            "order_send_called",
            False,
        )
    )

    position_ticket = (
        execution_report.get(
            "position_ticket"
        )
    )

    pending_ticket = (
        execution_report.get(
            "pending_ticket"
        )
    )

    print()
    print("=" * 80)
    print(
        "EXECUTION SUMMARY"
    )
    print("=" * 80)

    print(
        f"Source:             "
        f"{source}"
    )

    print(
        f"Mode:               "
        f"{mode}"
    )

    print(
        f"Validation:         "
        f"{validation_decision}"
    )

    print(
        f"Execution:          "
        f"{execution_decision}"
    )

    print(
        f"order_send called:  "
        f"{order_send_called}"
    )

    print(
        f"Position ticket:    "
        f"{position_ticket}"
    )

    print(
        f"Pending ticket:     "
        f"{pending_ticket}"
    )

    print()

    if (
        execution_decision
        == "ORDER_SEND_SUCCESS"
    ):

        print(
            "[DEMO LIVE OK] "
            "Market order отправлен и "
            "реальная DEMO-позиция подтверждена."
        )

        print(
            "[DEMO LIVE OK] "
            "Trade State перенесён "
            "в managed_positions."
        )

    elif (
        execution_decision
        == "RECOVERED_EXISTING_EXECUTION"
    ):

        print(
            "[RECOVERED] "
            "Исполнение уже существовало в MT5."
        )

        print(
            "[RECOVERED] "
            "Повторный order_send НЕ выполнялся."
        )

    elif (
        execution_decision
        == "DRY_RUN_ONLY"
    ):

        print(
            "[DRY RUN] "
            "MT5 request только проверен."
        )

        print(
            "[DRY RUN] "
            "order_send() не вызывался."
        )

    elif (
        execution_decision
        == "EXECUTION_SAFETY_BLOCKED"
    ):

        print(
            "[BLOCKED] "
            "Execution Safety Gate "
            "не разрешил order_send()."
        )

    elif (
        execution_decision
        in (
            "ORDER_SEND_STATE_UNKNOWN",
            "PARTIAL_FILL_UNRESOLVED",
        )
    ):

        print(
            "[FAIL CLOSED] "
            "Результат order_send неоднозначен."
        )

        print(
            "[FAIL CLOSED] "
            "SEND_INTENT сохранён. "
            "Повторный order_send запрещён."
        )

    elif (
        execution_decision
        == "ORDER_SEND_REJECTED"
    ):

        print(
            "[REJECTED] "
            "MT5 однозначно отклонил order_send."
        )

        print(
            "[REJECTED] "
            "План завершён без автоматического повтора."
        )

    elif (
        execution_decision
        == "PENDING_ORDER_PLACED"
    ):

        print(
            "[DEMO LIVE OK] "
            "Pending order выставлен в MT5."
        )

        print(
            "[DEMO LIVE OK] "
            f"Pending ticket: {pending_ticket}."
        )

    elif (
        execution_decision
        == "PENDING_ACTIVE"
    ):

        print(
            "[PENDING ACTIVE] "
            "Ордер остаётся активным до fill "
            "или появления следующей закрытой H1."
        )

    elif (
        execution_decision
        in (
            "PENDING_FILLED",
            "PENDING_FILLED_IMMEDIATELY",
            "PENDING_FILLED_BEFORE_CANCEL",
        )
    ):

        print(
            "[DEMO LIVE OK] "
            "Pending исполнился в позицию."
        )

    elif (
        execution_decision
        in (
            "PENDING_CANCELLED",
            "PENDING_EXPIRED",
            "PENDING_REJECTED",
            "PENDING_PLAN_EXPIRED_BEFORE_SEND",
        )
    ):

        print(
            "[PENDING CLOSED] "
            "Старый pending lifecycle завершён. "
            "Claude в этом же цикле не запускается."
        )

    elif (
        execution_decision
        in (
            "PENDING_ORDER_SEND_STATE_UNKNOWN",
            "PENDING_CANCEL_STATE_UNKNOWN",
            "PENDING_RECONCILIATION_BLOCKED",
            "PENDING_CANCEL_RECONCILIATION_BLOCKED",
            "PENDING_PARTIAL_FILL_UNRESOLVED",
        )
    ):

        print(
            "[FAIL CLOSED] "
            "Pending lifecycle неоднозначен. "
            "Новые сделки запрещены до reconciliation."
        )

    elif (
        validation_decision
        == "PLAN_EXPIRED"
    ):

        print(
            "[PLAN EXPIRED] "
            "Market-plan истёк и больше "
            "не может быть исполнен."
        )

    elif (
        validation_decision
        == "NO_ACTIVE_PLAN"
    ):

        print(
            "[NO ACTIVE PLAN] "
            "Исполнять нечего."
        )

    else:

        print(
            "[NO SEND] "
            "Executor не подтвердил "
            "реальное DEMO-исполнение."
        )

    print("=" * 80)


# ============================================================
# PRE-CLAUDE FUNDINGPIPS DAILY STATE GATE
# ============================================================

def inspect_pre_claude_daily_state_gate() -> dict:
    """
    Проверяет, можно ли вообще тратить Claude API на новую H1.

    Ключевой принцип:

        если daily baseline FundingPips не был надёжно
        зафиксирован в начале текущего Platform Day,
        новая торговая идея всё равно не может быть безопасно
        одобрена Risk Manager.

    Поэтому при trusted=False Claude НЕ вызывается, а H1
    остаётся незаблокированной в Analysis State. После
    восстановления доверенного daily baseline следующий цикл
    сможет снова рассмотреть эту же закрытую H1.
    """

    account = get_account_info()
    positions = get_positions()

    daily_state = get_daily_state(
        account,
        positions,
    )

    trusted = bool(
        daily_state.get(
            "trusted",
            False,
        )
    )

    return {
        "allowed": trusted,
        "fp_day": daily_state.get(
            "fp_day"
        ),
        "opening_balance": float(
            daily_state.get(
                "opening_balance",
                0.0,
            )
        ),
        "opening_equity": float(
            daily_state.get(
                "opening_equity",
                0.0,
            )
        ),
        "daily_baseline": float(
            daily_state.get(
                "daily_baseline",
                0.0,
            )
        ),
        "trusted": trusted,
        "capture_method": daily_state.get(
            "capture_method"
        ),
        "captured_at_fp": daily_state.get(
            "captured_at_fp"
        ),
    }


def print_pre_claude_daily_state_gate(
    gate: dict,
):
    """Печатает pre-Claude FundingPips daily-state gate."""

    print()
    print("=" * 80)
    print(
        "PRE-CLAUDE FUNDINGPIPS DAILY STATE GATE"
    )
    print("=" * 80)

    print(
        f"Day:                  "
        f"{gate.get('fp_day')}"
    )

    print(
        f"Opening Balance:      "
        f"{gate.get('opening_balance', 0.0):.2f}"
    )

    print(
        f"Opening Equity:       "
        f"{gate.get('opening_equity', 0.0):.2f}"
    )

    print(
        f"Daily baseline:       "
        f"{gate.get('daily_baseline', 0.0):.2f}"
    )

    print(
        f"Trusted:              "
        f"{gate.get('trusted')}"
    )

    print(
        f"Capture method:       "
        f"{gate.get('capture_method')}"
    )

    print(
        f"Captured at FP:       "
        f"{gate.get('captured_at_fp')}"
    )

    print(
        f"Claude allowed:       "
        f"{gate.get('allowed')}"
    )

    if gate.get(
        "allowed",
        False,
    ):
        print()
        print(
            "[OK] Daily baseline FundingPips "
            "считается надёжно зафиксированным."
        )
    else:
        print()
        print(
            "[BLOCKED] Daily baseline FundingPips "
            "не является доверенным."
        )
        print(
            "[BLOCKED] Claude API НЕ вызывается, "
            "потому что новая торговая идея всё равно "
            "не может быть безопасно одобрена."
        )
        print(
            "[WAIT] Analysis State для текущей H1 "
            "НЕ изменяется."
        )

    print("=" * 80)


# ============================================================
# ГЛАВНАЯ ФУНКЦИЯ
# ============================================================

@observe("analysis")
def main(
    manage_connection: bool = True,
    analysis_only: bool = False,
):
    """
    Основной цикл системы.

    ПРИОРИТЕТЫ:

    1. Проверка открытой позиции.
    2. Если позиция есть, этот технический цикл возвращается в runner;
       отдельный Position Monitor выполняет M15 Scout и H1 deep review.

    3. Если позиции нет:
           получить Market Snapshot.

    4. H1 Analysis Gate.

    5. Если H1 уже анализировалась:
           Entry analysis OFF; separate M15/H1 position review remains active.
           Executor можно запустить повторно.

    6. Если H1 новая:
           Claude.
           Risk Manager.
           Trade State.
           Analysis State.
           Executor.

    В режиме LIVE разрешены реальные
    market / limit / stop order_send() только после
    всех safety gates и durable SEND_INTENT.

    Дополнительно действует:

        - глобальный Execution Safety Gate;
        - pre-Claude execution fail-closed gate;
        - pre-Claude FundingPips daily-state gate;
        - market runtime / freshness gate.

    manage_connection=True
        обычный ручной запуск main.py: функция сама
        подключается и отключается от MT5.

    manage_connection=False
        runner уже держит постоянное MT5-соединение;
        main выполняет один торговый цикл без shutdown().

    analysis_only=True
        рыночный Scout/FULL и архив продолжаются без создания нового
        торгового плана и отправки ордеров. Сверка старого плана с MT5
        может обновить его фактический статус. Этот режим
        применяется, когда execution lifecycle временно неоднозначен:
        проблема исполнения блокирует новые ордера, а не наблюдение рынка.
    """

    connected = False
    execution_observation_only = bool(analysis_only)
    execution_block_reasons: list[str] = []

    try:

        # ====================================================
        # 1. CONNECT MT5
        # ====================================================

        if manage_connection:
            connected = (
                connect_mt5()
            )
        else:
            terminal = mt5.terminal_info()
            account = mt5.account_info()

            connected = bool(
                terminal is not None
                and account is not None
                and getattr(
                    terminal,
                    "connected",
                    False,
                )
            )

        if not connected:

            print()
            print(
                "[STOP] "
                "Подключение к MT5 не удалось "
                "или runner потерял соединение."
            )

            return

        print()
        if manage_connection:
            print(
                "[OK] MT5 подключён."
            )
        else:
            print(
                "[OK] Используем постоянное MT5-соединение runner."
            )

        print()
        print(
            f"[MODE] "
            f"{PROJECT_MODE}"
        )

        # ====================================================
        # EXECUTION SAFETY GATE
        # ====================================================

        execution_safety = (
            inspect_execution_safety_gate()
        )

        print_execution_safety_gate(
            execution_safety
        )

        if not execution_safety[
            "configuration_valid"
        ]:

            print()
            print(
                "[WARNING] "
                "Execution Safety Gate сейчас "
                "не разрешает реальный order_send()."
            )

            print(
                "[WARNING] "
                "Мониторинг и reconciliation продолжаются, "
                "но новый DEMO entry будет заблокирован."
            )

        # ====================================================
        # 1A. PENDING RECONCILIATION
        # ====================================================

        pending_reconciliation = (
            reconcile_active_pending_execution(
                symbol=SYMBOL
            )
        )

        print_pending_startup_reconciliation(
            pending_reconciliation
        )

        if pending_reconciliation.get(
            "blocked",
            False,
        ):

            print()
            print(
                "[FAIL CLOSED] "
                "Pending lifecycle не удалось "
                "однозначно восстановить из MT5."
            )

            execution_observation_only = True
            execution_block_reasons.extend(
                str(item)
                for item in pending_reconciliation.get("errors", [])
            )
            print(
                "[OBSERVATION ONLY] Новые ордера запрещены, "
                "но анализ новой H1 продолжится."
            )

        # ====================================================
        # 1B. MARKET SEND-INTENT RECONCILIATION
        # ====================================================

        send_intent_reconciliation = (
            reconcile_active_send_intent(
                symbol=SYMBOL
            )
        )

        print_send_intent_reconciliation(
            send_intent_reconciliation
        )

        if send_intent_reconciliation.get(
            "blocked",
            False,
        ):

            print()
            print(
                "[FAIL CLOSED] "
                "Есть неразрешённый SEND_INTENT."
            )

            execution_observation_only = True
            execution_block_reasons.extend(
                str(item)
                for item in send_intent_reconciliation.get("errors", [])
            )
            print(
                "[OBSERVATION ONLY] SEND_INTENT не разрешён; "
                "рыночный анализ продолжится без исполнения."
            )

        # ====================================================
        # 2. POSITION GATE
        # ====================================================

        print()
        print(
            "[INFO] Проверяем "
            "состояние открытых позиций..."
        )

        position_gate = (
            inspect_position_gate(
                symbol=SYMBOL
            )
        )

        # ====================================================
        # POSITION OPEN / CLOSED / RECONCILIATION
        # ====================================================

        if position_gate[
            "block_claude"
        ]:

            risk_context = (
                build_hold_risk_context()
            )

            print_position_hold_report(
                gate=position_gate,
                risk_context=risk_context,
            )

            return

        # ====================================================
        # 3. FLAT MODE
        # ====================================================

        print()
        print(
            "[FLAT MODE] "
            "Открытых позиций нет."
        )

        # ====================================================
        # 3A. MARKET RUNTIME / FRESHNESS GATE
        # ====================================================

        market_runtime_gate = (
            inspect_market_runtime_gate(
                symbol=SYMBOL
            )
        )

        print_market_runtime_gate(
            market_runtime_gate
        )

        active_plan_before_snapshot = (
            get_active_plan()
        )

        # Если active_plan нет, stale/closed/out-of-window рынок
        # не должен даже создавать тяжёлый Market Snapshot.
        #
        # Если active_plan ЕСТЬ, snapshot всё ещё нужен для
        # технического lifecycle старого market/pending плана
        # (например, отмены pending при появлении новой H1).
        if (
            not market_runtime_gate.get(
                "allowed",
                False,
            )
            and
            active_plan_before_snapshot is None
        ):

            print()
            print(
                "[BLOCKED] "
                "Рынок/рабочее окно не разрешают новую "
                "торговую идею Claude."
            )
            print(
                "[BLOCKED] "
                "Market Snapshot и Claude API не запускаются."
            )
            return

        if (
            not market_runtime_gate.get(
                "allowed",
                False,
            )
            and
            active_plan_before_snapshot is not None
        ):
            print()
            print(
                "[INFO] Runtime gate блокирует НОВУЮ идею, "
                "но active_plan существует."
            )
            print(
                "[INFO] Продолжаем только технический lifecycle "
                "существующего плана; Claude не будет запущен."
            )

        # ====================================================
        # 4. MARKET SNAPSHOT
        # ====================================================

        print()
        print(
            "[INFO] Собираем "
            "рыночные данные..."
        )

        snapshot = (
            get_market_snapshot(
                symbol=SYMBOL
            )
        )

        print_market_snapshot(
            snapshot
        )

        print()
        print(
            "[OK] Market snapshot "
            "успешно сформирован."
        )

        # ====================================================
        # 5. BOOTSTRAP ANALYSIS STATE
        # ====================================================

        bootstrap_result = (
            bootstrap_analysis_state_from_existing_plan(
                snapshot=snapshot,
                symbol=SYMBOL,
            )
        )

        print_analysis_bootstrap(
            bootstrap_result
        )

        # ====================================================
        # 6. H1 ANALYSIS GATE
        # ====================================================

        analysis_gate = (
            check_analysis_gate(
                snapshot=snapshot,
                symbol=SYMBOL,
            )
        )

        print_analysis_gate(
            analysis_gate
        )

        # Read-only telemetry only.  The exact raw candles used by the current
        # H1 cycle are persisted before any Claude stage.  A web/export failure
        # is contained inside save_web_market_snapshot and cannot change the
        # strategy, Risk Manager or Executor.
        if (
            analysis_gate.get("should_analyze", False)
            or not WEB_MARKET_SNAPSHOT_PATH.exists()
        ):
            save_web_market_snapshot(snapshot)

        # ====================================================
        # 7. ЭТА H1 УЖЕ АНАЛИЗИРОВАЛАСЬ
        # ====================================================

        if not analysis_gate[
            "should_analyze"
        ]:

            # =================================================
            # ALREADY ANALYZED
            # =================================================

            if (
                analysis_gate[
                    "decision"
                ]
                == GATE_ALREADY_ANALYZED
            ):

                print()
                print(
                    "[H1 GATE] "
                    "Эта закрытая H1 "
                    "уже обработана analysis cycle."
                )

                print(
                    "[H1 GATE] "
                    "Claude API НЕ вызывается."
                )

                if execution_observation_only:
                    print(
                        "[OBSERVATION ONLY] Эта H1 уже сохранена; "
                        "Executor не запускается."
                    )
                    return

                print()
                print(
                    "[INFO] "
                    "Проверяем, осталось ли "
                    "что-то для исполнения..."
                )

                # =============================================
                # EXECUTOR МОЖНО ПОВТОРИТЬ
                # =============================================

                execution_report = (
                    execute_active_plan(
                        snapshot=snapshot,
                        symbol=SYMBOL,
                    )
                )

                print_executor_report(
                    execution_report[
                        "validation_report"
                    ]
                )

                print_live_execution_report(
                    execution_report
                )

                print_executor_final_summary(
                    execution_report=(
                        execution_report
                    ),
                    source=(
                        "EXISTING_H1_ANALYSIS"
                    ),
                )

                return

            # =================================================
            # ДРУГАЯ БЛОКИРОВКА GATE
            # =================================================

            print()
            print(
                "[BLOCKED] "
                "H1 Analysis Gate "
                "не разрешил запуск Claude."
            )

            print(
                f"[BLOCKED] "
                f"{analysis_gate['reason']}"
            )

            print()
            print(
                "[SAFE MODE] "
                "Claude API НЕ вызывается."
            )

            print(
                "[SAFE MODE] "
                "Новая торговая идея "
                "не создаётся."
            )

            return

        # ====================================================
        # 8. NEW H1
        # ====================================================

        print()
        print(
            "[H1 GATE] "
            "Обнаружена новая "
            "закрытая H1."
        )

        print(
            "[H1 GATE] "
            "Claude-анализ разрешён."
        )

        # ====================================================
        # 8A. LIVE AVAILABILITY
        # ====================================================

        # Готовность order_send не должна выключать наблюдение рынка.
        # Если исполнение недоступно, H1 всё равно анализируется и
        # публикуется, но ниже не меняются Trade State/entry watch и
        # гарантированно не запускается Executor.
        if (
            execution_safety.get(
                "mode"
            )
            == "LIVE"
            and
            not execution_safety.get(
                "order_send_allowed",
                False,
            )
        ):

            print()
            print(
                "[BLOCKED] "
                "Режим LIVE включён, "
                "но Execution Safety Gate "
                "не разрешает order_send()."
            )

            execution_observation_only = True
            execution_block_reasons.extend(
                str(item)
                for item in execution_safety.get("errors", [])
            )

            print()
            print(
                "[OBSERVATION ONLY] Claude-анализ будет сохранён, "
                "но торговое исполнение этой H1 запрещено."
            )

        # ====================================================
        # 8B. PRE-CLAUDE EXECUTION GATE
        # ====================================================

        pre_claude_gate = (
            inspect_pre_claude_gate(
                symbol=SYMBOL
            )
        )

        print_pre_claude_gate(
            pre_claude_gate
        )

        if not pre_claude_gate[
            "allowed"
        ]:

            print()
            print(
                "[BLOCKED] "
                "Новая H1 есть, но Claude НЕ вызывается, "
                "потому что торговое состояние "
                "ещё не полностью разрешено."
            )

            if execution_observation_only:
                execution_block_reasons.extend(
                    str(item)
                    for item in pre_claude_gate.get("blockers", [])
                )
                print(
                    "[OBSERVATION ONLY] Блокирующее торговое состояние "
                    "не мешает анализу, но запрещает любое исполнение."
                )
            # Если остался старый active_plan и это обычный торговый цикл,
            # разрешаем Executor один раз обработать его lifecycle (TTL).
            elif pre_claude_gate[
                "trade_state"
            ][
                "active_plan_present"
            ]:

                print()
                print(
                    "[INFO] "
                    "Обнаружен старый active_plan. "
                    "Запускаем Executor для его безопасной обработки."
                )

                execution_report = (
                    execute_active_plan(
                        snapshot=snapshot,
                        symbol=SYMBOL,
                    )
                )

                print_executor_report(
                    execution_report[
                        "validation_report"
                    ]
                )

                print_live_execution_report(
                    execution_report
                )

                print_executor_final_summary(
                    execution_report=(
                        execution_report
                    ),
                    source=(
                        "PRE_CLAUDE_BLOCKING_STATE"
                    ),
                )

            if not execution_observation_only:
                print()
                print(
                    "[WAIT] "
                    "После устранения блокирующего состояния "
                    "следующий запуск снова проверит эту H1."
                )

                print(
                    "[WAIT] "
                    "Analysis State для этой H1 НЕ изменён, "
                    "поэтому Claude не потерян."
                )

                return

        # ====================================================
        # 8C. PRE-CLAUDE FUNDINGPIPS DAILY STATE GATE
        # ====================================================

        daily_state_gate = (
            inspect_pre_claude_daily_state_gate()
        )

        print_pre_claude_daily_state_gate(
            daily_state_gate
        )

        if not daily_state_gate["allowed"]:

            print()
            print(
                "[BLOCKED] "
                "FundingPips daily baseline не подтверждён."
            )

            print(
                "[COST GUARD] Claude API НЕ вызывается: результат всё равно "
                "не мог бы дойти до Risk Manager и order_send()."
            )
            print(
                "[WAIT] Эта H1 не помечается обработанной. После восстановления "
                "дневной базы runner повторит цикл."
            )
            return

        # Runtime gate проверяется ещё раз непосредственно
        # перед Claude. Это важно, если выше существовал
        # active_plan и цикл продолжался только ради lifecycle.
        if not market_runtime_gate.get(
            "allowed",
            False,
        ):
            print()
            print(
                "[BLOCKED] "
                "Market Runtime Gate запрещает новый Claude-анализ."
            )
            print(
                "[WAIT] Analysis State для этой H1 НЕ изменяется."
            )
            return

        # ====================================================
        # 9. STEP 10 ANALYSIS SCHEDULE
        # ====================================================

        schedule = inspect_analysis_schedule(
            snapshot
        )

        print_analysis_schedule(
            schedule
        )

        scout_result = None
        analysis_cycle_type = None

        previous_reference = (
            get_fresh_reference_for_snapshot(
                snapshot
            )
        )

        full_required = bool(
            schedule.get(
                "mandatory_full",
                False,
            )
        )

        if full_required:
            analysis_cycle_type = CYCLE_FULL_SCHEDULED

            print()
            print(
                "[DAILY BASELINE] Эта H1 закрылась в 08:00 FP. "
                "Выполняем единственный обязательный дневной FULL; "
                "Scout пропускается."
            )

        else:
            # Scout сравнивает новые raw данные с последним успешным FULL
            # текущих FP-суток. Reference действует до конца рабочего дня.
            # Если дневной FULL отсутствует (ошибка baseline, поздний старт,
            # только что закрывшаяся длинная позиция), Scout не имеет базы для
            # сравнения: это системная неопределённость и безопасный FULL
            # FALLBACK выполняется напрямую.
            if previous_reference is None:
                full_required = True
                analysis_cycle_type = CYCLE_FULL_FALLBACK

                print()
                print(
                    "[DAILY REFERENCE MISSING] Успешного FULL-reference "
                    "текущих FP-суток нет."
                )
                print(
                    "[FULL FALLBACK] Scout не может сравнить рынок без "
                    "дневной карты. Выполняем глубокий FULL."
                )

            else:
                # COST OPTIMIZATION V8.5.3  PHASE 2A LOCAL H1 EVENT GATE
                #
                # No paid Scout here. Python does NOT decide direction,
                # Entry, SL or TP. It only decides whether new market data
                # justify purchasing a fresh Claude decision.

                from ai_local_event_gate import (
                    inspect_h1_event,
                    mark_h1_evaluated,
                    was_h1_evaluated,
                )
                from ai_cost_guard import inspect_cycle_budget

                h1_event_key = str(
                    extract_latest_closed_h1_time(
                        snapshot
                    )
                )

                if was_h1_evaluated(
                    h1_event_key
                ):

                    print()
                    print(
                        "[LOCAL EVENT GATE] Эта H1 уже была "
                        "бесплатно проверена и не требовала Claude."
                    )
                    print(
                        "[LOCAL EVENT GATE] Новых платных "
                        "запросов нет."
                    )
                    return


                scout_result = inspect_h1_event(
                    snapshot,
                    previous_reference,
                )

                print()
                print("=" * 80)
                print(
                    "LOCAL H1 EVENT GATE  $0"
                )
                print("=" * 80)
                print(
                    "Material change: "
                    f"{scout_result.get('material_change')}"
                )
                print(
                    "Possible setup:  "
                    f"{scout_result.get('possible_setup')}"
                )
                print(
                    "FULL required:   "
                    f"{scout_result.get('full_analysis_required')}"
                )
                print(
                    "Trigger:         "
                    f"{scout_result.get('trigger_kind')}"
                )
                print(
                    "Claude cost:     $0"
                )
                print("=" * 80)


                # Save the free gate result before any possible paid H1 call.
                scout_payload = build_scout_payload(
                    snapshot=snapshot,
                    previous_reference=previous_reference,
                )

                try:

                    scout_archive = save_analysis_archive(
                        snapshot=snapshot,
                        cycle_type="LOCAL_EVENT_GATE",
                        payload=scout_payload,
                        result=scout_result,
                        previous_reference=previous_reference,
                        note=(
                            "Deterministic local H1 event gate. "
                            "No Claude API request was made."
                        ),
                    )

                except Exception as error:

                    print()
                    print(
                        "[COST SAFETY] Не удалось сохранить "
                        "LOCAL EVENT GATE; платный H1 Claude "
                        "НЕ запускается: "
                        f"{type(error).__name__}: {error}"
                    )
                    return


                if scout_result.get(
                    "full_analysis_required"
                ):

                    full_budget = inspect_cycle_budget(
                        "FULL_ESCALATED"
                    )

                    if not full_budget.get(
                        "allowed"
                    ):

                        print()
                        print(
                            "[COST BUDGET] Структурное событие "
                            "обнаружено, но дополнительный FULL "
                            "сейчас превысил бы дневной soft ceiling."
                        )
                        print(
                            "[COST BUDGET] "
                            f"spent=${full_budget.get('analysis_spent_usd')}; "
                            f"soft=${full_budget.get('soft_daily_usd')}."
                        )

                        mark_h1_evaluated(
                            h1_event_key,
                            {
                                **scout_result,
                                "budget_blocked": True,
                            },
                        )
                        return


                    full_required = True
                    analysis_cycle_type = (
                        CYCLE_FULL_ESCALATED
                    )

                    print()
                    print(
                        "[LOCAL EVENT -> FULL] "
                        "Старшая structural invalidation "
                        "требует перестройки карты."
                    )


                elif not (
                    scout_result.get(
                        "material_change"
                    )
                    or scout_result.get(
                        "possible_setup"
                    )
                ):

                    mark_h1_evaluated(
                        h1_event_key,
                        scout_result,
                    )

                    print()
                    print(
                        "[LOCAL NO EVENT] H1 не дала события, "
                        "способного изменить торговое решение."
                    )
                    print(
                        "[LOCAL NO EVENT] Claude API НЕ вызывается."
                    )

                    return


                else:

                    h1_budget = inspect_cycle_budget(
                        "H1_DECISION"
                    )

                    print()
                    print(
                        "[COST BUDGET] "
                        f"analysis spent=${h1_budget.get('analysis_spent_usd')}; "
                        f"target=${h1_budget.get('target_daily_usd')}; "
                        f"soft=${h1_budget.get('soft_daily_usd')}."
                    )


                    if not h1_budget.get(
                        "allowed"
                    ):

                        mark_h1_evaluated(
                            h1_event_key,
                            {
                                **scout_result,
                                "budget_blocked": True,
                            },
                        )

                        print(
                            "[COST BUDGET] H1 event найден, "
                            "но дневной target уже достигнут."
                        )
                        print(
                            "[COST BUDGET] Новый Claude H1 "
                            "не покупается."
                        )

                        return


                    print()
                    print(
                        "[LOCAL EVENT -> H1 DECISION] "
                        "Есть объективное событие; покупаем "
                        "только компактный TRADE decision."
                    )

                    _run_h1_decision_refresh(
                        snapshot=snapshot,
                        previous_reference=previous_reference,
                        scout_result=scout_result,
                        scout_archive=scout_archive,
                        execution_observation_only=execution_observation_only,
                        execution_block_reasons=execution_block_reasons,
                    )

                    return

        # ====================================================
        # 10. DEEP FULL PAYLOAD
        # ====================================================

        full_resume = _load_resumable_staged_full_archive(snapshot)
        if full_resume is not None:
            full_archive, full_record = full_resume
            claude_payload = full_record["payload"]
            archived_reference = full_record.get("previous_reference")
            previous_reference = (
                archived_reference
                if isinstance(archived_reference, dict)
                else None
            )
            archived_scout = full_record.get("scout_result")
            if isinstance(archived_scout, dict):
                scout_result = archived_scout
            analysis_cycle_type = (
                full_record.get("cycle_type") or analysis_cycle_type or "FULL"
            )
            print()
            print(
                "[API RESUME] STAGED FULL продолжает исходный frozen payload из "
                f"{full_archive}."
            )
        else:
            print()
            print(
                "[INFO] Формируем FULL RAW данные для Claude..."
            )

            claude_payload = build_claude_payload(
                snapshot,
                previous_reference=previous_reference,
            )

            print_payload_stats(
                claude_payload
            )

            save_debug_payload(
                claude_payload
            )

            print()
            print(
                "[OK] FULL RAW payload успешно подготовлен."
            )

            # Reference применяется к точному frozen payload и не меняется
            # между retry attempts, включая продолжение после рестарта.
            previous_reference = get_fresh_reference_for_payload(
                claude_payload
            )

            try:
                full_archive = save_analysis_archive(
                    snapshot=snapshot,
                    cycle_type=analysis_cycle_type or "FULL",
                    payload=claude_payload,
                    result=None,
                    scout_result=scout_result,
                    previous_reference=previous_reference,
                    api_attempt={
                        "status": "PAYLOAD_SAVED_BEFORE_API",
                        "retry_policy": "CONTROLLED_BOUNDED",
                    },
                    note=(
                        "STAGED FULL payload сохранён до платного API-вызова. "
                        "Свечи доступны вебу независимо от исхода Claude."
                    ),
                )
                safe_update_analysis_archive(
                    full_archive,
                    staged_analysis_version=STAGED_ANALYSIS_VERSION,
                )
            except Exception as error:
                print()
                print(
                    "[COST SAFETY] Не удалось сохранить FULL payload; "
                    "Claude API НЕ вызывается: "
                    f"{type(error).__name__}: {error}"
                )
                return

        # ====================================================
        # 11. DEEP FULL CLAUDE — TWO PERSISTED STAGES
        # ====================================================

        print()
        print("=" * 80)
        print(
            "ЗАПУСК ГЛУБОКОГО FULL-АНАЛИЗА CLAUDE — STAGED"
        )
        print("=" * 80)
        print(
            f"Cycle type: {analysis_cycle_type}"
        )

        # V8.5.3: market map is built from small durable micro-stages.
        # Every validated field is checkpointed; repair requests contain only
        # fields that are still missing.
        map_run = run_market_map_pipeline(
            claude_payload,
            previous_reference=previous_reference,
            snapshot=snapshot,
            cycle_type=analysis_cycle_type or "FULL",
            archive_path=full_archive,
        )

        if not map_run.get("ok"):
            error = map_run.get("error") or RuntimeError(
                "V8.5.3 MARKET_MAP micro-pipeline did not complete."
            )
            _register_api_retries_exhausted(
                snapshot=snapshot,
                api_stage="MARKET_MAP_MICRO",
                error=error,
            )
            print()
            print(
                "[BLOCKED] MARKET_MAP micro-pipeline не завершён. "
                "Уже полученные поля сохранены; FULL_DECISION, Risk Manager "
                "и Executor не запускаются."
            )
            return

        market_map = map_run["result"]

        # V8.5.3: the execution decision is also split into small stages.
        decision_run = run_trade_decision_pipeline(
            claude_payload,
            market_map=market_map,
            previous_reference=previous_reference,
            snapshot=snapshot,
            cycle_type=analysis_cycle_type or "FULL",
            archive_path=full_archive,
            decision_family="FULL_DECISION",
        )

        if not decision_run.get("ok"):
            error = decision_run.get("error") or RuntimeError(
                "V8.5.3 FULL_DECISION micro-pipeline did not complete."
            )
            _register_api_retries_exhausted(
                snapshot=snapshot,
                api_stage="FULL_DECISION_MICRO",
                error=error,
            )
            print()
            print(
                "[BLOCKED] FULL_DECISION micro-pipeline не завершён. "
                "Market map остаётся сохранённой; Risk Manager/Executor "
                "для этой H1 не запускаются."
            )
            return

        analysis = assemble_staged_analysis(
            payload=claude_payload,
            market_map=market_map,
            trade_decision=decision_run["result"],
            previous_reference=previous_reference,
        )

        full_usage = combine_stage_usage(
            map_run.get("usage"),
            decision_run.get("usage"),
        )
        full_usage["cost_audit"] = _build_known_api_cost_audit(full_archive)

        # Final debug output is exactly the legacy FULL contract.
        save_debug_response(analysis)
        print_analysis_summary(analysis)

        print()
        print("=" * 80)
        print(
            "[OK] АНАЛИЗ CLAUDE ЗАВЕРШЁН"
        )
        print("=" * 80)

        safe_update_analysis_archive(
            full_archive,
            result=analysis,
            scout_result=scout_result,
            previous_reference=previous_reference,
            api_usage=full_usage,
            note=(
                "FULL_MAP + FULL_DECISION validated; локально собран прежний "
                "FULL contract. Только он передаётся в Risk Manager/Executor."
            ),
            staged_analysis_version=STAGED_ANALYSIS_VERSION,
        )

        print(
            f"[ARCHIVE] FULL analysis сохранён: {full_archive}"
        )

        # Сохраняем последний полностью завершённый анализ только как
        # reference для следующей H1. Следующий Claude всё равно получает
        # полный raw market context и обязан анализировать его независимо.
        reference_record = save_reference_analysis(
            analysis=analysis,
            snapshot=snapshot,
        )

        print()
        print(
            "[REFERENCE] Последний успешный Claude-анализ сохранён "
            "как reference-only для следующей H1."
        )
        print(
            f"[REFERENCE] H1: {reference_record.get('h1_closed_bar_time_fp')}"
        )

        if execution_observation_only:
            reason_text = "; ".join(
                dict.fromkeys(
                    item
                    for item in execution_block_reasons
                    if item
                )
            ) or "Execution lifecycle временно запрещает новые ордера."
            risk_report = {
                "decision": "NO_TRADE",
                "approved": False,
                "reasons": [reason_text],
                "observation_only": True,
            }
            state_result = {
                "state_action": "OBSERVATION_ONLY_EXECUTION_BLOCKED",
                "active_plan": None,
                "plan_changed": False,
                "observation_only": True,
                "reason": reason_text,
            }
            analysis_registration = register_completed_analysis(
                snapshot=snapshot,
                analysis=analysis,
                risk_report=risk_report,
                state_result=state_result,
                symbol=SYMBOL,
                cycle_type=analysis_cycle_type or "FULL",
                scout_result=scout_result,
            )
            safe_update_analysis_archive(
                full_archive,
                risk_report=risk_report,
                trade_state_result=state_result,
                analysis_registration=analysis_registration,
                execution_report={
                    "decision": "OBSERVATION_ONLY",
                    "order_send_called": False,
                    "reason": reason_text,
                },
                note=(
                    "Анализ завершён и опубликован в execution quarantine; "
                    "Trade State и Executor не изменялись."
                ),
            )
            print_analysis_registration(analysis_registration)
            print()
            print("=" * 80)
            print("[OBSERVATION ONLY] АНАЛИЗ СОХРАНЁН, ОРДЕР НЕ ОТПРАВЛЕН")
            print("=" * 80)
            print(reason_text)
            print("=" * 80)
            return

        watch_state = refresh_entry_watch(
            analysis,
            source_h1=reference_record.get("h1_closed_bar_time_fp"),
        )
        print(
            "[ENTRY WATCH] "
            + (
                "Ожидаем закрытый M15/M5 trigger для короткого ENTRY_CHECK."
                if watch_state.get("status") == "watching"
                else "Условный M15/M5 trigger в FULL не задан."
            )
        )

        # ====================================================
        # 11. RISK MANAGER
        # ====================================================

        print()
        print(
            "[INFO] Передаём "
            "рекомендацию в Risk Manager..."
        )

        risk_report = (
            evaluate_trade(
                analysis=analysis,
                symbol=SYMBOL,
            )
        )

        print_risk_report(
            risk_report
        )

        # ====================================================
        # 12. TRADE STATE
        # ====================================================

        print()
        print(
            "[INFO] Обновляем Trade State..."
        )

        state_result = (
            register_trade_decision(
                analysis=analysis,
                risk_report=risk_report,
                snapshot=snapshot,
            )
        )

        print_trade_state_result(
            state_result
        )

        # ====================================================
        # 13. REGISTER ANALYZED H1
        # ====================================================

        print()
        print(
            "[INFO] Регистрируем "
            "завершённый H1-анализ..."
        )

        analysis_registration = (
            register_completed_analysis(
                snapshot=snapshot,
                analysis=analysis,
                risk_report=risk_report,
                state_result=state_result,
                symbol=SYMBOL,
                cycle_type=analysis_cycle_type or "FULL",
                scout_result=scout_result,
            )
        )

        print_analysis_registration(
            analysis_registration
        )

        # Полный read-only audit packet для Linux. Эта операция best-effort:
        # сбой локального веб-архива не влияет на торговую стратегию.
        safe_update_analysis_archive(
            full_archive,
            risk_report=risk_report,
            trade_state_result=state_result,
            analysis_registration=analysis_registration,
        )

        # ====================================================
        # ВАЖНО:
        #
        # Analysis State регистрируется ДО Executor.
        #
        # Если Executor по технической причине
        # завершится ошибкой, следующая попытка:
        #
        #     НЕ вызовет Claude снова,
        #     но сможет повторить Executor.
        #
        # ====================================================

        # ====================================================
        # 14. TRADE EXECUTOR
        # ====================================================

        print()
        print(
            "[INFO] Запускаем "
            "Trade Executor..."
        )

        execution_report = (
            execute_active_plan(
                snapshot=snapshot,
                symbol=SYMBOL,
            )
        )

        print_executor_report(
            execution_report[
                "validation_report"
            ]
        )

        print_live_execution_report(
            execution_report
        )

        safe_update_analysis_archive(
            full_archive,
            execution_report=execution_report,
        )

        _emit_trade_funnel(
            snapshot=snapshot,
            cycle_type=(analysis_cycle_type or "FULL"),
            analysis=analysis,
            risk_report=risk_report,
            state_result=state_result,
            execution_report=execution_report,
        )

        # ====================================================
        # 15. FINAL ANALYSIS RESULT
        # ====================================================

        risk_decision = (
            risk_report.get(
                "decision"
            )
        )

        state_action = (
            state_result.get(
                "state_action"
            )
        )

        recommendation = (
            analysis.get(
                "recommendation",
                {},
            )
        )

        print()
        print("=" * 80)
        print(
            "ФИНАЛЬНОЕ РЕШЕНИЕ АНАЛИЗА"
        )
        print("=" * 80)

        print(
            f"H1:              "
            f"{analysis_registration['h1_closed_bar_time']}"
        )

        print(
            f"Claude action:   "
            f"{recommendation.get('action')}"
        )

        print(
            f"Risk decision:   "
            f"{risk_decision}"
        )

        print(
            f"Trade State:     "
            f"{state_action}"
        )

        print(
            f"Plan ID:         "
            f"{analysis_registration['plan_id']}"
        )

        print()

        if (
            risk_decision
            == "APPROVED"
        ):

            print(
                "[APPROVED] "
                "Торговая идея одобрена."
            )

        elif (
            risk_decision
            == "NO_TRADE"
        ):

            print(
                "[NO TRADE] "
                "На этой H1 сделки нет."
            )

        elif (
            risk_decision
            == "REJECTED"
        ):

            print(
                "[REJECTED] "
                "Risk Manager заблокировал "
                "торговую идею."
            )

        print()
        print(
            "[H1 LOCKED] "
            "Эта H1 теперь считается "
            "обработанной."
        )

        print(
            "[H1 LOCKED] "
            "Повторный Claude-анализ "
            "этой H1 запрещён."
        )

        print("=" * 80)

        # ====================================================
        # 16. EXECUTION SUMMARY
        # ====================================================

        print_executor_final_summary(
            execution_report=(
                execution_report
            ),
            source=(
                "NEW_H1_ANALYSIS"
            ),
        )

    # ========================================================
    # FATAL ERROR
    # ========================================================

    except Exception as error:

        print()
        print("=" * 80)
        print(
            "[FATAL ERROR]"
        )
        print("=" * 80)

        print(
            f"{type(error).__name__}: "
            f"{error}"
        )

        print()
        print(
            "[SAFE MODE] "
            "При любой ошибке "
            "торговое исполнение запрещено."
        )

        print(
            "[SAFE MODE] "
            "Никакой ордер "
            "не отправлен."
        )

        print("=" * 80)

    # ========================================================
    # DISCONNECT
    # ========================================================

    finally:

        if connected and manage_connection:

            disconnect_mt5()


# ============================================================================
# V8.5.3 FULL RECOVERY HOTFIX  RESILIENT ARCHIVE FALLBACK
# ============================================================================

_COST_PREVIOUS_LOAD_RESUMABLE_STAGED_FULL_ARCHIVE = (
    _load_resumable_staged_full_archive
)


def _cost_same_h1(
    left,
    right,
) -> bool:

    if left is None or right is None:
        return False


    left_text = str(
        left
    ).strip()

    right_text = str(
        right
    ).strip()


    if left_text == right_text:
        return True


    try:

        left_dt = datetime.fromisoformat(
            left_text.replace(
                "Z",
                "+00:00",
            )
        )

        right_dt = datetime.fromisoformat(
            right_text.replace(
                "Z",
                "+00:00",
            )
        )


        if (
            left_dt.tzinfo is not None
            and right_dt.tzinfo is not None
        ):

            return (
                left_dt.astimezone(
                    timezone.utc
                )
                ==
                right_dt.astimezone(
                    timezone.utc
                )
            )


        return (
            left_dt.replace(
                tzinfo=None
            )
            ==
            right_dt.replace(
                tzinfo=None
            )
        )


    except Exception:

        return False


def _load_resumable_staged_full_archive(
    snapshot: dict,
):
    """
    First use the legacy API-journal recovery.

    If it has no V8.5.3 micro-pipeline entry, find the oldest
    incomplete FULL archive for this exact closed H1 and reuse
    that frozen payload.

    This is deliberately oldest-first: it maximizes reuse of
    already-paid resilient MARKET_MAP/TRADE checkpoints.
    """

    legacy = (
        _COST_PREVIOUS_LOAD_RESUMABLE_STAGED_FULL_ARCHIVE(
            snapshot
        )
    )


    if legacy is not None:
        return legacy


    current_h1 = (
        extract_latest_closed_h1_time(
            snapshot
        )
    )


    if current_h1 is None:
        return None


    from pathlib import Path as _CostPath


    archive_root = (
        _CostPath(__file__).resolve().parent
        / "analysis_archive"
    )


    if not archive_root.exists():
        return None


    candidates = []


    for archive_path in (
        archive_root.rglob(
            "*.json"
        )
    ):

        try:

            record = (
                load_analysis_archive(
                    archive_path
                )
            )

        except Exception:

            continue


        cycle_type = str(
            record.get(
                "cycle_type"
            )
            or ""
        ).upper()


        # Only actual FULL-family cycles.
        if not (
            cycle_type == "FULL"
            or cycle_type.startswith(
                "FULL_"
            )
        ):
            continue


        if not _cost_same_h1(
            record.get(
                "h1_closed_bar_time_fp"
            ),
            current_h1,
        ):
            continue


        if not isinstance(
            record.get(
                "payload"
            ),
            dict,
        ):
            continue


        # Completed FULL must never be treated as resumable.
        if isinstance(
            record.get(
                "result"
            ),
            dict,
        ):
            continue


        candidates.append(
            (
                str(
                    record.get(
                        "snapshot_time_fp"
                    )
                    or ""
                ),
                str(
                    archive_path
                ),
                archive_path,
                record,
            )
        )


    if not candidates:
        return None


    # Oldest frozen FULL first.
    candidates.sort(
        key=lambda item: (
            item[0],
            item[1],
        )
    )


    _, _, archive_path, record = (
        candidates[0]
    )


    print()
    print(
        "[V8.5.3 FULL RESUME] "
        "Найден незавершённый frozen FULL "
        "для той же закрытой H1."
    )

    print(
        "[V8.5.3 FULL RESUME] "
        f"Archive: {archive_path}"
    )

    print(
        "[V8.5.3 FULL RESUME] "
        "Новый MARKET_MAP покупать запрещено; "
        "используется исходный payload/checkpoint."
    )


    return (
        archive_path,
        record,
    )

# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()
