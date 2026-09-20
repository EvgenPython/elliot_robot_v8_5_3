"""Paid, decision-only confirmation after a deterministic M15/M5 trigger."""

from observability import observe, emit
import copy

from analysis_archive import (
    load_analysis_archive,
    save_analysis_archive,
    safe_update_analysis_archive,
    update_analysis_archive,
)
from chart_contract import sanitize_visualization
from claude_client import (
    claude_api_retry_context,
    get_error_failure_class,
    get_error_request_id,
    is_outcome_unknown_error,
    is_retryable_claude_error,
    validate_analysis_contract,
    validate_trade_levels,
)
from claude_payload import build_claude_payload
from claude_reference_state import get_fresh_reference_for_snapshot
from claude_resilient_pipeline import run_trade_decision_pipeline
from claude_request_guard import begin_api_attempt, get_api_cycle, mark_api_attempt
from claude_staged_client import (
    analyze_entry_check,
    get_last_stage_diagnostics,
    get_last_stage_usage,
)
from entry_watch import (
    ENTRY_CHECK_MAX_ATTEMPTS,
    load_entry_watch,
    mark_entry_check_result,
)
from live_executor import execute_active_plan
from market_data import get_market_snapshot
from prop_time import now_fp
from risk_manager import evaluate_trade
from trade_state import extract_latest_closed_h1_time, register_trade_decision
from web_market_snapshot import save_web_market_snapshot


VISUAL_ARRAYS = (
    "wave_points", "levels", "zones", "scenario_paths", "trendlines",
    "channels", "pattern_shapes", "market_events", "projected_waves",
    "wave_structures",
)


def _entry_check_guard_h1(snapshot: dict) -> str:
    """Return a restart-stable journal key for one watched setup.

    A retry may happen after the wall clock crosses into the next H1.  Using
    the latest snapshot H1 in that case would create a new API cycle and could
    pay for the same trigger twice.  The H1 that created the watch is durable
    and remains the key for every retry of that setup.
    """

    watch = load_entry_watch() or {}
    stable_source = watch.get("source_h1_closed_bar_time")
    return str(stable_source or extract_latest_closed_h1_time(snapshot))


def _recover_guarded_entry_response(guard: dict) -> dict | None:
    """Recover a response persisted just before a journal/process failure."""

    cycle = guard.get("cycle") or {}
    previous_archive = cycle.get("archive_path")
    if not previous_archive:
        return None
    try:
        record = load_analysis_archive(previous_archive)
    except Exception:
        return None
    decision = record.get("trade_decision_result")
    if not isinstance(decision, dict):
        return None
    return {
        "archive_path": str(previous_archive),
        "record": record,
        "decision": decision,
        "payload": record.get("payload"),
        "usage": record.get("trade_decision_usage"),
    }


def merge_visualization(base: dict, fresh: dict) -> dict:
    result = copy.deepcopy(base if isinstance(base, dict) else {})
    for name in VISUAL_ARRAYS:
        merged = []
        seen = set()
        for item in list(result.get(name) or []) + list(fresh.get(name) or []):
            marker = repr(item)
            if marker in seen:
                continue
            seen.add(marker)
            merged.append(copy.deepcopy(item))
        result[name] = merged
    old_comment = str(result.get("chart_comment", "")).strip()
    new_comment = str(fresh.get("chart_comment", "")).strip()
    result["chart_comment"] = " ".join(item for item in (old_comment, new_comment) if item)
    return result


def assemble_entry_refresh_analysis(
    *,
    reference_analysis: dict,
    decision: dict,
    payload: dict,
) -> dict:
    """Combine a fresh execution decision with the last validated market map.

    The decision-only call has no authority to rewrite D1/H4 or the parent
    Elliott count.  It refreshes the timestamp, H1/M15/M5 execution context,
    recommendation and execution drawings, then runs the complete local
    business validators again.
    """
    if not isinstance(reference_analysis, dict):
        raise ValueError("H1 decision refresh requires a validated reference analysis.")
    if not isinstance(decision, dict):
        raise ValueError("H1 decision refresh requires a decision object.")

    analysis = copy.deepcopy(reference_analysis)
    analysis["timestamp"] = str(payload.get("timestamp") or decision.get("timestamp"))
    analysis["instrument"] = str(decision.get("instrument") or analysis.get("instrument"))
    analysis["recommendation"] = copy.deepcopy(decision["recommendation"])
    reference_quality = analysis.get("data_quality") or {}
    decision_quality = decision["data_quality"]
    quality_issues = []
    for value in (reference_quality.get("issues"), decision_quality.get("issues")):
        text = str(value or "").strip()
        if text and text.lower() not in {"none", "no issues"} and text not in quality_issues:
            quality_issues.append(text)
    analysis["data_quality"] = {
        "sufficient": bool(reference_quality.get("sufficient"))
        and bool(decision_quality.get("sufficient")),
        "issues": "; ".join(quality_issues) if quality_issues else "none",
    }
    # A previous H1/M30/M15/M5 projection must not silently survive into the new
    # hour.  Keep higher-timeframe map objects and replace execution-timeframe
    # objects with the fresh decision response.
    base_visual = copy.deepcopy(analysis.get("visualization") or {})
    for name in VISUAL_ARRAYS:
        base_visual[name] = [
            item for item in list(base_visual.get(name) or [])
            if not isinstance(item, dict)
            or str(item.get("timeframe") or "").upper()
            not in {"M30", "M15", "M5"}
        ]
    analysis["visualization"] = merge_visualization(
        base_visual, decision.get("visualization") or {}
    )

    timeframe = analysis.get("timeframe_analysis")
    if isinstance(timeframe, dict):
        h1_context = str(decision.get("h1_execution_context") or "").strip()
        relationship = str(decision.get("multi_timeframe_relationship") or "").strip()
        if h1_context:
            timeframe["H1"] = "\n".join(
                part for part in (str(timeframe.get("H1") or "").strip(), h1_context)
                if part
            )
        if relationship:
            timeframe["relationship"] = "\n".join(
                part for part in (
                    str(timeframe.get("relationship") or "").strip(), relationship
                ) if part
            )
    micro = str(decision.get("microstructure_and_patterns") or "").strip()
    if micro:
        analysis["patterns"] = "\n".join(
            part for part in (str(analysis.get("patterns") or "").strip(), micro)
            if part
        )

    validate_trade_levels(analysis)
    validate_analysis_contract(analysis)
    sanitize_visualization(analysis, payload)
    return analysis


@observe("analysis")
def run_entry_check(symbol: str = "XAUUSD") -> dict:
    reference = get_fresh_reference_for_snapshot(
        {"instrument": symbol, "generated_at_fp": now_fp().isoformat()}
    )
    if not reference or not isinstance(reference.get("analysis"), dict):
        mark_entry_check_result(
            "blocked_no_fresh_full_reference",
            retryable=True,
            count_attempt=False,
        )
        return {"ok": False, "reason": "no_fresh_full_reference"}

    snapshot = get_market_snapshot(symbol)
    save_web_market_snapshot(snapshot)
    payload = build_claude_payload(snapshot, previous_reference=reference)
    archive = save_analysis_archive(
        snapshot=snapshot,
        cycle_type="ENTRY_CHECK",
        payload=payload,
        previous_reference=reference,
        note="ENTRY_CHECK payload saved before the single paid decision call.",
    )
    source_h1 = _entry_check_guard_h1(snapshot)
    guard = begin_api_attempt(
        h1_closed_bar_time=source_h1,
        api_stage="ENTRY_CHECK",
        cycle_type="ENTRY_CHECK",
        payload_timestamp=payload.get("timestamp"),
        archive_path=archive,
        max_attempts=ENTRY_CHECK_MAX_ATTEMPTS,
    )

    decision = None
    decision_payload = payload
    recovered_record = None

    if guard.get("allowed"):
        attempt = guard.get("attempt") or {}
        attempt_id = str(attempt.get("attempt_id") or "")
        durable_recovery = _recover_guarded_entry_response(guard)
        try:
            if durable_recovery is not None:
                recovered_record = durable_recovery["record"]
                decision = durable_recovery["decision"]
                decision_payload = durable_recovery.get("payload") or payload
                usage = durable_recovery.get("usage")
                archive = durable_recovery["archive_path"]
                diagnostics = {}
                failure_class = "RECOVERED_DURABLE_RESPONSE"
                billing_status = (
                    "USAGE_AVAILABLE" if isinstance(usage, dict)
                    else "RECOVERED_USAGE_UNKNOWN"
                )
                delivery_recovered = True
            else:
                # COST OPTIMIZATION V8.5.3  PHASE 2A ENTRY BUDGET
                # This branch is reached only when there is no already-paid
                # durable response to recover.
                from ai_cost_guard import inspect_cycle_budget

                budget = inspect_cycle_budget("ENTRY_CHECK")

                if not budget.get("allowed"):
                    mark_entry_check_result(
                        "blocked_daily_cost_budget",
                        retryable=False,
                        count_attempt=False,
                    )
                    safe_update_analysis_archive(
                        archive,
                        note=(
                            "ENTRY_CHECK skipped before Claude because "
                            "the daily analysis soft ceiling was reached."
                        ),
                    )
                    return {
                        "ok": False,
                        "reason": "DAILY_COST_BUDGET_REACHED",
                        "archive": str(archive),
                        "cost_budget": budget,
                    }

                attempts = list((guard.get("cycle") or {}).get("attempts") or [])
                previous_failure_class = None
                if len(attempts) >= 2:
                    previous_failure_class = attempts[-2].get("failure_class")
                watch_state = load_entry_watch() or {}
                projection = watch_state.get("projection") or {}
                entry_identity = "|".join(
                    str(value or "")
                    for value in (
                        source_h1,
                        projection.get("projection_id"),
                        watch_state.get("triggered_closed_bar_time"),
                        watch_state.get("source_analysis_timestamp"),
                    )
                )
                decision_run = run_trade_decision_pipeline(
                    payload,
                    market_map=reference["analysis"],
                    previous_reference=reference,
                    snapshot=snapshot,
                    cycle_type="ENTRY_CHECK",
                    archive_path=archive,
                    decision_family="ENTRY_CHECK",
                    pipeline_identity=entry_identity,
                )
                if not decision_run.get("ok"):
                    raise decision_run.get("error") or RuntimeError(
                        "ENTRY_CHECK micro-pipeline не завершён."
                    )
                decision = decision_run["result"]
                usage = decision_run.get("usage")
                diagnostics = {}

                # The paid response must be recoverable before the journal
                # marks it as the winner. This prevents both duplicate billing
                # and a silently consumed trigger after a process restart.
                update_analysis_archive(
                    archive,
                    trade_decision_result=decision,
                    trade_decision_usage=usage,
                    entry_check_api_attempt=attempt,
                    note="ENTRY_CHECK paid response saved before execution.",
                )
                failure_class = "VALIDATED_RESPONSE"
                billing_status = (
                    "USAGE_AVAILABLE" if isinstance(usage, dict) else "UNKNOWN"
                )
                delivery_recovered = False

            mark_api_attempt(
                h1_closed_bar_time=source_h1,
                api_stage="ENTRY_CHECK",
                attempt_id=attempt_id,
                status="VALIDATED",
                request_id=diagnostics.get("request_id"),
                usage=usage,
                failure_class=failure_class,
                billing_status=billing_status,
                delivery_recovered=delivery_recovered,
            )
        except Exception as error:
            diagnostics = get_last_stage_diagnostics("ENTRY_CHECK") or {}
            retryable = is_retryable_claude_error(error)
            attempt_result = mark_api_attempt(
                h1_closed_bar_time=source_h1,
                api_stage="ENTRY_CHECK",
                attempt_id=attempt_id,
                status="FAILED_RETRYABLE" if retryable else "FAILED_PERMANENT",
                request_id=diagnostics.get("request_id") or get_error_request_id(error),
                usage=diagnostics.get("usage"),
                error=error,
                retryable=retryable,
                outcome_unknown=is_outcome_unknown_error(error),
                failure_class=get_error_failure_class(error),
                billing_status=(
                    "UNKNOWN_MAY_BE_BILLED"
                    if is_outcome_unknown_error(error)
                    else "NO_COMPLETED_RESPONSE_REPORTED"
                ),
            )
            safe_update_analysis_archive(
                archive,
                entry_check_api_attempt=attempt_result,
                api_cycle=get_api_cycle(source_h1, "ENTRY_CHECK"),
                note=f"ENTRY_CHECK failed closed: {type(error).__name__}: {error}",
            )
            can_retry = attempt_result.get("cycle_status") == "WAITING_RETRY"
            mark_entry_check_result(
                f"failed_closed:{type(error).__name__}",
                retryable=can_retry,
            )
            return {
                "ok": False,
                "reason": f"{type(error).__name__}: {error}",
                "archive": str(archive),
            }
    else:
        cycle = guard.get("cycle") or get_api_cycle(source_h1, "ENTRY_CHECK") or {}
        if cycle.get("status") != "VALIDATED":
            mark_entry_check_result(
                f"api_guard_blocked:{guard.get('reason')}",
                retryable=False,
            )
            safe_update_analysis_archive(
                archive,
                api_cycle=cycle,
                note=f"ENTRY_CHECK blocked by durable API guard: {guard.get('reason')}",
            )
            return {
                "ok": False,
                "reason": str(guard.get("reason") or "entry_check_api_guard_blocked"),
                "archive": str(archive),
            }

        previous_archive = cycle.get("archive_path")
        if previous_archive:
            try:
                recovered_record = load_analysis_archive(previous_archive)
            except Exception:
                recovered_record = None
        if isinstance(recovered_record, dict):
            decision = recovered_record.get("trade_decision_result")
            decision_payload = recovered_record.get("payload") or payload
            archive = str(previous_archive)
            if (
                isinstance(recovered_record.get("trade_state_result"), dict)
                and isinstance(recovered_record.get("execution_report"), dict)
            ):
                mark_entry_check_result("recovered_completed_entry_check")
                return {
                    "ok": True,
                    "recovered": True,
                    "risk_report": recovered_record.get("risk_report"),
                    "state_result": recovered_record.get("trade_state_result"),
                    "execution_report": recovered_record.get("execution_report"),
                    "archive": str(previous_archive),
                }
        if not isinstance(decision, dict):
            mark_entry_check_result("validated_response_missing_from_archive")
            return {
                "ok": False,
                "reason": "validated_response_missing_from_archive",
                "archive": str(archive),
            }

    try:
        analysis = assemble_entry_refresh_analysis(
            reference_analysis=reference["analysis"],
            decision=decision,
            payload=decision_payload,
        )

        risk_report = evaluate_trade(analysis=analysis, symbol=symbol)
        state_result = register_trade_decision(
            analysis=analysis, risk_report=risk_report, snapshot=snapshot
        )
        execution_report = execute_active_plan(snapshot=snapshot, symbol=symbol)
        safe_update_analysis_archive(
            archive,
            result=analysis,
            trade_decision_result=decision,
            trade_decision_usage=get_last_stage_usage("ENTRY_CHECK"),
            api_cycle=get_api_cycle(source_h1, "ENTRY_CHECK"),
            risk_report=risk_report,
            trade_state_result=state_result,
            execution_report=execution_report,
            note="ENTRY_CHECK completed; the higher-timeframe FULL map was not rebuilt.",
        )
        mark_entry_check_result(str(risk_report.get("decision") or "completed"))
        return {
            "ok": True,
            "analysis": analysis,
            "risk_report": risk_report,
            "state_result": state_result,
            "execution_report": execution_report,
            "archive": str(archive),
        }
    except Exception as error:
        safe_update_analysis_archive(
            archive,
            note=f"ENTRY_CHECK failed closed: {type(error).__name__}: {error}",
        )
        # The paid response is already durable and marked VALIDATED. A retry
        # reuses it and therefore cannot create another Claude charge.
        mark_entry_check_result(
            f"post_response_failed:{type(error).__name__}",
            retryable=True,
        )
        return {"ok": False, "reason": f"{type(error).__name__}: {error}", "archive": str(archive)}
