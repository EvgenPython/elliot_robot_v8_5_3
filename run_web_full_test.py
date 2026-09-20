"""Paid analysis-only end-to-end test for the Linux web terminal.

The command deliberately runs outside the trading scheduler so a clean
installation can be verified while the market is closed.  It performs the
real Reliability V3 FULL_MAP + FULL_DECISION pipeline against a frozen MT5
snapshot, archives the validated result and prepares the candle snapshot for
``web_publisher.py``.

Safety boundary:

* no Risk Manager call;
* no Trade State registration;
* no Executor call and no ``mt5.order_send``;
* no update of the production Claude reference;
* dedicated request-journal stage names, so the test cannot consume or block
  a production FULL_MAP/FULL_DECISION slot for the same H1 candle.
"""

import argparse
import sys

from analysis_archive import (
    safe_update_analysis_archive,
    save_analysis_archive,
)
from claude_client import print_analysis_summary, save_debug_response
from claude_payload import (
    build_claude_payload,
    print_payload_stats,
    save_debug_payload,
)
from claude_reference_state import get_fresh_reference_for_payload
from claude_staged_client import (
    STAGED_ANALYSIS_VERSION,
    analyze_market_map,
    analyze_trade_decision,
    assemble_staged_analysis,
    build_previous_confirmed_anchor_reference,
    combine_stage_usage,
    get_last_stage_diagnostics,
    get_last_stage_usage,
    repair_market_map,
    repair_trade_decision,
    validate_market_map_result,
)
from main import _build_known_api_cost_audit, _run_api_with_retries
from market_data import get_market_snapshot
from mt5_client import connect_mt5, disconnect_mt5
from trade_state import extract_latest_closed_h1_time
from web_market_snapshot import save_web_market_snapshot


SYMBOL = "XAUUSD"
CYCLE_TYPE = "FULL_WEB_TEST"
MAP_STAGE = "WEB_TEST_V4_FULL_MAP"
MAP_REPAIR_STAGE = "WEB_TEST_V4_FULL_MAP_REPAIR"
DECISION_STAGE = "WEB_TEST_V4_FULL_DECISION"
DECISION_REPAIR_STAGE = "WEB_TEST_V4_FULL_DECISION_REPAIR"


def _collect_snapshot() -> dict:
    connected = False
    try:
        connected = connect_mt5()
        if not connected:
            raise RuntimeError("Не удалось подключиться к MT5.")
        snapshot = get_market_snapshot(symbol=SYMBOL)
    finally:
        if connected:
            disconnect_mt5()

    if not isinstance(snapshot, dict) or not snapshot:
        raise RuntimeError("MT5 вернул пустой Market Snapshot.")
    return snapshot


def _run_market_map(
    snapshot: dict,
    payload: dict,
    previous_reference: dict | None,
    archive_path,
) -> dict:
    previous_anchors = build_previous_confirmed_anchor_reference(
        previous_reference
    )
    run = _run_api_with_retries(
        snapshot=snapshot,
        api_stage=MAP_STAGE,
        cycle_type=CYCLE_TYPE,
        payload_timestamp=payload.get("timestamp"),
        archive_path=archive_path,
        api_call=lambda on_preflight, on_response: analyze_market_map(
            payload,
            previous_reference=previous_reference,
            on_preflight=on_preflight,
            on_response=on_response,
        ),
        result_archive_key="market_map_result",
        usage_archive_key="market_map_usage",
        result_validator=lambda result: validate_market_map_result(
            result,
            payload,
            previous_anchors,
        ),
        diagnostics_fallback=lambda: get_last_stage_diagnostics("FULL_MAP"),
        usage_fallback=lambda: get_last_stage_usage("FULL_MAP"),
    )

    if run.get("ok") or not run.get("repairable"):
        return run

    invalid_result = run.get("invalid_result")
    validation_error = run.get("validation_error")
    safe_update_analysis_archive(
        archive_path,
        market_map_invalid_result=invalid_result,
        market_map_validation_error=validation_error,
        note=(
            "WEB TEST FULL_MAP не прошёл локальную проверку; полный map "
            "повторно не покупается, выполняется один маленький REPAIR."
        ),
    )
    return _run_api_with_retries(
        snapshot=snapshot,
        api_stage=MAP_REPAIR_STAGE,
        cycle_type=CYCLE_TYPE,
        payload_timestamp=payload.get("timestamp"),
        archive_path=archive_path,
        api_call=lambda on_preflight, on_response: repair_market_map(
            payload,
            invalid_result=invalid_result,
            validation_error=validation_error,
            previous_reference=previous_reference,
            on_preflight=on_preflight,
            on_response=on_response,
        ),
        result_archive_key="market_map_result",
        usage_archive_key="market_map_repair_usage",
        result_validator=lambda result: validate_market_map_result(
            result,
            payload,
            previous_anchors,
        ),
        diagnostics_fallback=lambda: get_last_stage_diagnostics(
            "FULL_MAP_REPAIR"
        ),
        usage_fallback=lambda: get_last_stage_usage("FULL_MAP_REPAIR"),
    )


def _run_trade_decision(
    snapshot: dict,
    payload: dict,
    market_map: dict,
    previous_reference: dict | None,
    archive_path,
) -> tuple[dict, dict | None]:
    assembled_holder = {}

    def validate_and_assemble(result: dict):
        assembled_holder["analysis"] = assemble_staged_analysis(
            payload=payload,
            market_map=market_map,
            trade_decision=result,
            previous_reference=previous_reference,
        )
        return result

    run = _run_api_with_retries(
        snapshot=snapshot,
        api_stage=DECISION_STAGE,
        cycle_type=CYCLE_TYPE,
        payload_timestamp=payload.get("timestamp"),
        archive_path=archive_path,
        api_call=lambda on_preflight, on_response: analyze_trade_decision(
            payload,
            market_map=market_map,
            previous_reference=previous_reference,
            on_preflight=on_preflight,
            on_response=on_response,
        ),
        result_archive_key="trade_decision_result",
        usage_archive_key="trade_decision_usage",
        result_validator=validate_and_assemble,
        diagnostics_fallback=lambda: get_last_stage_diagnostics(
            "FULL_DECISION"
        ),
        usage_fallback=lambda: get_last_stage_usage("FULL_DECISION"),
    )

    if run.get("ok"):
        return run, assembled_holder.get("analysis")
    if not run.get("repairable"):
        return run, None

    invalid_result = run.get("invalid_result")
    validation_error = run.get("validation_error")
    safe_update_analysis_archive(
        archive_path,
        trade_decision_invalid_result=invalid_result,
        trade_decision_validation_error=validation_error,
        note=(
            "WEB TEST FULL_DECISION не прошёл локальную проверку; полный "
            "decision повторно не покупается, выполняется один REPAIR."
        ),
    )
    repaired = _run_api_with_retries(
        snapshot=snapshot,
        api_stage=DECISION_REPAIR_STAGE,
        cycle_type=CYCLE_TYPE,
        payload_timestamp=payload.get("timestamp"),
        archive_path=archive_path,
        api_call=lambda on_preflight, on_response: repair_trade_decision(
            payload,
            market_map=market_map,
            invalid_result=invalid_result,
            validation_error=validation_error,
            previous_reference=previous_reference,
            on_preflight=on_preflight,
            on_response=on_response,
        ),
        result_archive_key="trade_decision_result",
        usage_archive_key="trade_decision_repair_usage",
        result_validator=validate_and_assemble,
        diagnostics_fallback=lambda: get_last_stage_diagnostics(
            "FULL_DECISION_REPAIR"
        ),
        usage_fallback=lambda: get_last_stage_usage(
            "FULL_DECISION_REPAIR"
        ),
    )
    return repaired, assembled_holder.get("analysis")


def run_paid_web_test() -> int:
    print("=" * 80)
    print("RELIABILITY V3 — PAID WEB FULL TEST")
    print("=" * 80)
    print("[PAID TEST] Будут выполнены FULL_MAP и FULL_DECISION Claude.")
    print("[SAFE] Risk Manager, Trade State и Executor не вызываются.")
    print("[SAFE] mt5.order_send() не используется.")
    print("[SAFE] Production reference и analysis_state не изменяются.")
    print()

    snapshot = _collect_snapshot()
    h1_time = extract_latest_closed_h1_time(snapshot)
    print(f"[SNAPSHOT] Generated FP: {snapshot.get('generated_at_fp')}")
    print(f"[SNAPSHOT] Last tick FP: {snapshot.get('last_tick_time_fp')}")
    print(f"[SNAPSHOT] Latest closed H1: {h1_time}")

    if not save_web_market_snapshot(snapshot):
        print("[BLOCKED] Не удалось сохранить свечи; Claude не вызывается.")
        return 2

    payload = build_claude_payload(snapshot)
    print_payload_stats(payload)
    save_debug_payload(payload)
    previous_reference = get_fresh_reference_for_payload(payload)

    try:
        archive_path = save_analysis_archive(
            snapshot=snapshot,
            cycle_type=CYCLE_TYPE,
            payload=payload,
            previous_reference=previous_reference,
            api_attempt={
                "status": "PAYLOAD_SAVED_BEFORE_API",
                "retry_policy": "RELIABILITY_V3_ANALYSIS_ONLY",
            },
            note=(
                "Явно запущенный платный WEB FULL test. Торговый pipeline "
                "отключён; результат предназначен только для проверки веба."
            ),
        )
        safe_update_analysis_archive(
            archive_path,
            staged_analysis_version=STAGED_ANALYSIS_VERSION,
        )
    except Exception as error:
        print(
            "[COST SAFETY] Архив frozen payload создать не удалось; "
            f"Claude не вызывается: {type(error).__name__}: {error}"
        )
        return 2

    map_run = _run_market_map(
        snapshot,
        payload,
        previous_reference,
        archive_path,
    )
    if not map_run.get("ok"):
        print(
            "[WEB TEST FAILED] FULL_MAP не дал validated response: "
            f"{map_run.get('error')}"
        )
        return 3

    market_map = map_run["result"]
    decision_run, analysis = _run_trade_decision(
        snapshot,
        payload,
        market_map,
        previous_reference,
        archive_path,
    )
    if not decision_run.get("ok"):
        print(
            "[WEB TEST FAILED] FULL_DECISION не дал validated response: "
            f"{decision_run.get('error')}"
        )
        return 4

    if not isinstance(analysis, dict):
        analysis = assemble_staged_analysis(
            payload=payload,
            market_map=market_map,
            trade_decision=decision_run["result"],
            previous_reference=previous_reference,
        )

    usage = combine_stage_usage(
        map_run.get("usage"),
        decision_run.get("usage"),
    )
    usage["cost_audit"] = _build_known_api_cost_audit(archive_path)

    save_debug_response(analysis)
    print_analysis_summary(analysis)
    safe_update_analysis_archive(
        archive_path,
        result=analysis,
        previous_reference=previous_reference,
        api_usage=usage,
        note=(
            "FULL_WEB_TEST validated. Только archive/web telemetry; Risk "
            "Manager, Trade State, Executor и reference update не запускались."
        ),
        staged_analysis_version=STAGED_ANALYSIS_VERSION,
    )

    print()
    print("=" * 80)
    print("[OK] PAID WEB FULL TEST COMPLETED")
    print(f"[ARCHIVE] {archive_path}")
    print("[NEXT] Запустите web_publisher.py --once.")
    print("[SAFE] Торговые состояния не изменялись; ордера не отправлялись.")
    print("=" * 80)
    return 0


def parse_args(argv: list[str] | None = None):
    parser = argparse.ArgumentParser(
        description=(
            "Платный analysis-only FULL тест Claude для проверки Linux web."
        )
    )
    parser.add_argument(
        "--confirm-paid-analysis",
        action="store_true",
        help="Явно подтверждает два платных staged API-вызова Claude.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if not args.confirm_paid_analysis:
        print(
            "[BLOCKED] Это платный тест. Для явного запуска добавьте "
            "--confirm-paid-analysis."
        )
        return 2
    try:
        return run_paid_web_test()
    except KeyboardInterrupt:
        print("\n[STOPPED] Тест остановлен пользователем.")
        return 130
    except Exception as error:
        print(
            "[WEB TEST ERROR] "
            f"{type(error).__name__}: {error}"
        )
        return 5


if __name__ == "__main__":
    sys.exit(main())
