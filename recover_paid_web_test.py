"""Recover a billed saved FULL_MAP response and continue with decision only.

This utility is intentionally narrow.  It never calls FULL_MAP or
FULL_MAP_REPAIR.  It loads one existing frozen FULL_WEB_TEST archive, expands
and validates a locally saved Claude map/repair message, persists that map as
the stage winner, and only then may perform the still-missing FULL_DECISION.

Safety boundary is identical to run_web_full_test.py: no Risk Manager, Trade
State registration, Executor, production reference update, or mt5.order_send.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from analysis_archive import load_analysis_archive, safe_update_analysis_archive
from claude_client import print_analysis_summary, save_debug_response
from claude_staged_client import (
    STAGED_ANALYSIS_VERSION,
    _expand_market_map_wire_result,
    assemble_staged_analysis,
    build_previous_confirmed_anchor_reference,
    combine_stage_usage,
    validate_market_map_result,
)
from main import _build_known_api_cost_audit
from run_web_full_test import _run_trade_decision


EXPECTED_CYCLE_TYPE = "FULL_WEB_TEST"
EXPECTED_RESPONSE_STAGES = {"FULL_MAP", "FULL_MAP_REPAIR"}


def _load_json_object(path: Path, label: str) -> dict:
    with open(path, "r", encoding="utf-8") as file:
        value = json.load(file)
    if not isinstance(value, dict):
        raise ValueError(f"{label} должен быть JSON object: {path}")
    return value


def _extract_saved_wire_response(message: dict) -> dict:
    stage = str(message.get("stage") or "").upper()
    if stage not in EXPECTED_RESPONSE_STAGES:
        raise ValueError(
            "Saved response stage должен быть FULL_MAP/FULL_MAP_REPAIR, "
            f"получено {stage!r}."
        )
    if str(message.get("stop_reason") or "") != "end_turn":
        raise ValueError(
            "Saved response не завершён end_turn; локальное восстановление "
            "запрещено."
        )
    text_blocks = [
        block.get("text")
        for block in message.get("content", [])
        if isinstance(block, dict) and block.get("type") == "text"
    ]
    if len(text_blocks) != 1 or not isinstance(text_blocks[0], str):
        raise ValueError("Saved response должен содержать один text JSON block.")
    wire = json.loads(text_blocks[0])
    if not isinstance(wire, dict):
        raise ValueError("Saved response text должен декодироваться в object.")
    return wire


def _usage_dict(value) -> dict:
    if not isinstance(value, dict):
        return {}
    fields = (
        "input_tokens",
        "output_tokens",
        "cache_creation_input_tokens",
        "cache_read_input_tokens",
        "thinking_tokens",
    )
    return {field: int(value.get(field, 0) or 0) for field in fields}


def _sum_usage(*values) -> dict:
    normalized = [_usage_dict(value) for value in values]
    fields = set().union(*(item.keys() for item in normalized))
    return {
        field: sum(int(item.get(field, 0) or 0) for item in normalized)
        for field in sorted(fields)
    }


def _snapshot_identity(record: dict) -> dict:
    h1_time = record.get("h1_closed_bar_time_fp")
    if not h1_time:
        raise ValueError("Archive не содержит h1_closed_bar_time_fp.")
    return {
        "instrument": str(record.get("instrument") or "XAUUSD"),
        "generated_at_fp": record.get("snapshot_time_fp"),
        "timeframes": {
            "H1": {
                "closed_bars": [{"time_fp": str(h1_time)}],
            }
        },
    }


def recover_and_continue(
    archive_path: Path,
    map_response_path: Path,
    confirm_paid_decision: bool,
) -> int:
    print("=" * 80)
    print("RELIABILITY V3 — LOCAL MAP RECOVERY + PAID DECISION")
    print("=" * 80)
    print("[SAFE] FULL_MAP и FULL_MAP_REPAIR повторно НЕ вызываются.")
    print("[SAFE] Risk Manager, Trade State и Executor не вызываются.")
    print("[SAFE] mt5.order_send() не используется.")

    record = load_analysis_archive(archive_path)
    if str(record.get("cycle_type") or "").upper() != EXPECTED_CYCLE_TYPE:
        raise ValueError("Разрешён только архив cycle_type=FULL_WEB_TEST.")
    if record.get("result") is not None:
        print("[BLOCKED] Archive уже содержит итоговый validated FULL result.")
        return 2

    payload = record.get("payload")
    if not isinstance(payload, dict):
        raise ValueError("Archive не содержит frozen payload.")
    previous_reference = record.get("previous_reference")
    message = _load_json_object(map_response_path, "Saved Claude response")
    wire = _extract_saved_wire_response(message)
    market_map = _expand_market_map_wire_result(wire)
    previous_anchors = build_previous_confirmed_anchor_reference(
        previous_reference
    )
    validate_market_map_result(market_map, payload, previous_anchors)

    recovered_usage = _usage_dict(message.get("usage"))
    if not safe_update_analysis_archive(
        archive_path,
        market_map_result=market_map,
        market_map_repair_usage=recovered_usage,
        market_map_validation_error=None,
        staged_analysis_version=STAGED_ANALYSIS_VERSION,
        note=(
            "Оплаченный FULL_MAP_REPAIR локально восстановлен из полного "
            "end_turn JSON. Legacy data_quality issues объединены без "
            "изменения рыночной оценки; новый MAP API-запрос не выполнялся."
        ),
    ):
        print("[BLOCKED] Не удалось сохранить recovered map в archive.")
        return 2

    print("[RECOVERED] FULL_MAP_REPAIR прошёл все локальные validators.")
    print(
        "[RECOVERED] wave_points={}; levels={}; zones={}; paths={}.".format(
            len(market_map["visualization"]["wave_points"]),
            len(market_map["visualization"]["levels"]),
            len(market_map["visualization"]["zones"]),
            len(market_map["visualization"]["scenario_paths"]),
        )
    )

    if not confirm_paid_decision:
        print()
        print(
            "[BLOCKED] MAP восстановлен бесплатно. Для единственного ещё "
            "не выполненного платного FULL_DECISION добавьте "
            "--confirm-paid-decision."
        )
        return 2

    snapshot = _snapshot_identity(record)
    decision_run, analysis = _run_trade_decision(
        snapshot,
        payload,
        market_map,
        previous_reference,
        archive_path,
    )
    if not decision_run.get("ok"):
        print(
            "[RECOVERY TEST FAILED] FULL_DECISION не дал validated response: "
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

    map_total_usage = _sum_usage(
        record.get("market_map_usage"),
        recovered_usage,
    )
    usage = combine_stage_usage(map_total_usage, decision_run.get("usage"))
    usage["market_map_primary"] = _usage_dict(record.get("market_map_usage"))
    usage["market_map_repair"] = recovered_usage
    usage["cost_audit"] = _build_known_api_cost_audit(archive_path)

    save_debug_response(analysis)
    print_analysis_summary(analysis)
    if not safe_update_analysis_archive(
        archive_path,
        result=analysis,
        api_usage=usage,
        staged_analysis_version=STAGED_ANALYSIS_VERSION,
        note=(
            "FULL_WEB_TEST завершён после локального восстановления уже "
            "оплаченного MAP и отдельного validated FULL_DECISION. Торговый "
            "pipeline не запускался."
        ),
    ):
        print("[BLOCKED] Итоговый validated FULL не удалось сохранить.")
        return 2

    print()
    print("=" * 80)
    print("[OK] RECOVERED PAID WEB FULL TEST COMPLETED")
    print(f"[ARCHIVE] {archive_path}")
    print("[SAFE] MAP повторно не покупался; ордера не отправлялись.")
    print("=" * 80)
    return 0


def parse_args(argv: list[str] | None = None):
    parser = argparse.ArgumentParser(
        description=(
            "Локально восстанавливает сохранённый paid MAP и по явному "
            "подтверждению выполняет только отсутствующий FULL_DECISION."
        )
    )
    parser.add_argument("--archive", required=True, type=Path)
    parser.add_argument("--map-response", required=True, type=Path)
    parser.add_argument("--confirm-paid-decision", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        return recover_and_continue(
            args.archive,
            args.map_response,
            args.confirm_paid_decision,
        )
    except KeyboardInterrupt:
        print("\n[STOPPED] Recovery остановлен пользователем.")
        return 130
    except Exception as error:
        print(f"[RECOVERY ERROR] {type(error).__name__}: {error}")
        return 5


if __name__ == "__main__":
    sys.exit(main())
