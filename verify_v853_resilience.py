from __future__ import annotations

import ast
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parent


def require(condition: bool, message: str):
    if not condition:
        raise SystemExit(f"[FAIL] {message}")
    print(f"[OK] {message}")


def main() -> int:
    require((ROOT / "VERSION").read_text(encoding="utf-8").strip() == "8.5.3",
            "VERSION=8.5.3")
    for name in (
        "claude_partial_json.py",
        "claude_resilient_pipeline.py",
        "claude_stream_recovery.py",
        "claude_staged_client.py",
        "scout_client.py",
        "main.py",
        "entry_check_cycle.py",
        "position_monitor.py",
        "web_runtime_state.py",
        "web_publisher.py",
        "reset_ai_breaker.py",
    ):
        path = ROOT / name
        require(path.exists(), f"{name} exists")
        ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        print(f"[OK] {name} parses")

    pipeline = (ROOT / "claude_resilient_pipeline.py").read_text(encoding="utf-8")
    require("PERMANENT_BLOCKED" in pipeline, "global permanent breaker exists")
    require("partial_fields" in pipeline, "partial-field checkpoint consumption exists")
    require("MAX_INVALID_ATTEMPTS_PER_STAGE" in pipeline,
            "invalid-response repair loop is bounded")
    require("MAX_OUTCOME_UNKNOWN_ATTEMPTS_PER_STAGE" in pipeline,
            "outcome-unknown paid retry is bounded")
    require("clear_ai_circuit_breaker" in pipeline,
            "permanent breaker requires explicit operator reset")
    require("run_market_map_pipeline" in pipeline, "market micro-pipeline exists")
    require("run_trade_decision_pipeline" in pipeline, "decision micro-pipeline exists")
    require("run_position_review_pipeline" in pipeline, "position review micro-pipeline exists")
    require("register_permanent_ai_failure" in pipeline,
            "permanent errors can open the global breaker from any Claude stage")

    market_names = re.findall(r'\n\s*"(MM_[A-Z0-9_]+)"\s*,', pipeline)
    trade_names = re.findall(r'\n\s*"(TD_[A-Z0-9_]+)"\s*,', pipeline)
    require(len(set(market_names)) >= 12, "market analysis split into >=12 micro-stages")
    require(len(set(trade_names)) >= 7, "trade decision split into >=7 micro-stages")
    position_names = re.findall(r'\n\s*"(PR_[A-Z0-9_]+)"\s*,', pipeline)
    require(len(set(position_names)) >= 10,
            "position review split into >=10 micro-stages")
    require('family.startswith("TRADE_DECISION") and spec.name == "TD_CONTEXT"' in pipeline,
            "full market map is injected only into TD_CONTEXT")
    require('context["validated_market_map"] = copy.deepcopy(market_map or {})' in pipeline,
            "TD_CONTEXT receives validated market map")

    main_text = (ROOT / "main.py").read_text(encoding="utf-8")
    require("run_market_map_pipeline(" in main_text, "main routes FULL map to micro-pipeline")
    require('decision_family="H1_DECISION"' in main_text, "H1 decision uses micro-pipeline")
    require('decision_family="M30_DECISION"' in main_text, "M30 decision uses micro-pipeline")
    require('decision_family="FULL_DECISION"' in main_text, "FULL decision uses micro-pipeline")

    entry_text = (ROOT / "entry_check_cycle.py").read_text(encoding="utf-8")
    require('decision_family="ENTRY_CHECK"' in entry_text,
            "ENTRY_CHECK uses resilient trade micro-pipeline")
    position_text = (ROOT / "position_monitor.py").read_text(encoding="utf-8")
    require("run_position_review_pipeline(" in position_text,
            "open-position deep review uses resilient micro-pipeline")
    staged_text = (ROOT / "claude_staged_client.py").read_text(encoding="utf-8")
    require("ensure_ai_request_allowed" in staged_text and "register_permanent_ai_failure" in staged_text,
            "all structured Claude stages obey/open the global breaker")
    scout_text = (ROOT / "scout_client.py").read_text(encoding="utf-8")
    require("ensure_ai_request_allowed" in scout_text and "register_permanent_ai_failure" in scout_text,
            "Scout obeys/opens the global breaker")

    stream = (ROOT / "claude_stream_recovery.py").read_text(encoding="utf-8")
    require("recover_completed_top_level_fields" in stream,
            "SSE stream salvages completed top-level fields")
    require('progress["partial_fields"]' in stream,
            "partial fields are emitted during a live stream")

    runtime = (ROOT / "web_runtime_state.py").read_text(encoding="utf-8")
    require('"ai_analysis"' in runtime, "AI status exported to web runtime")
    publisher = (ROOT / "web_publisher.py").read_text(encoding="utf-8")
    require('"claude_resilient_pipeline"' in publisher,
            "resilient AI state is published to Linux web")

    print("\nV8.5.3 static resilience verification PASSED.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

