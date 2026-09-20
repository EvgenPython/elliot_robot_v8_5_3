"""One-command offline release gate for WaveFrame Robot V8.5.2.

The command never connects to MT5 or Anthropic.  It compiles the project and
runs the full suite, including the stateful MT5 lifecycle simulator.
"""

from __future__ import annotations

import compileall
import json
import re
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent
EXPECTED_VERSION = "8.5.2"
MINIMUM_TEST_COUNT = 138
REQUIRED_LIFECYCLE_TESTS = {
    "test_market_open_duplicate_guard_broker_close_and_statistics",
    "test_limit_pending_survives_restart_fills_and_closes",
    "test_stop_pending_is_cancelled_once_and_archived",
    "test_rejected_pending_does_not_leave_stuck_plan",
    "test_unknown_market_result_is_reconciled_after_restart_without_resend",
    "test_open_position_stop_is_moved_once_and_reconciled",
    "test_pending_ttl_is_exactly_three_h1_bars",
    "test_unsent_pending_can_retry_on_next_h1_within_ttl",
    "test_transient_entry_check_failure_keeps_trigger_for_retry",
    "test_entry_check_api_key_survives_crossing_into_next_h1",
    "test_paid_entry_response_is_recovered_before_any_second_api_call",
    "test_missed_midnight_is_reconstructed_from_booked_deals",
    "test_billed_refusal_is_retried_sequentially_with_recovery_context",
    "test_only_intermediate_half_hour_is_due_and_completion_is_durable",
    "test_hour_close_is_owned_by_h1_cycle",
    "test_demo_release_does_not_silently_arm_real_account",
}


def fail(message: str) -> int:
    print(f"[FAIL] {message}")
    return 1


def main() -> int:
    version = (ROOT / "VERSION").read_text(encoding="utf-8").strip()
    if version != EXPECTED_VERSION:
        return fail(f"VERSION={version!r}; expected {EXPECTED_VERSION!r}.")

    execution = json.loads(
        (ROOT / "config" / "execution.json").read_text(encoding="utf-8-sig")
    )
    if execution.get("trading_enabled") is not True:
        return fail("config/execution.json has trading_enabled != true.")
    modes = {str(item).upper() for item in execution.get("allowed_account_modes", [])}
    if modes != {"DEMO"}:
        return fail(
            "DEMO release must allow exactly DEMO; "
            f"got {sorted(modes)}."
        )

    source_text = "\n".join(
        path.read_text(encoding="utf-8", errors="replace")
        for path in ROOT.glob("*.py")
        if path.name != Path(__file__).name and not path.name.startswith("test_")
    )
    hidden_dry_run_pattern = re.compile(
        r"\bDRY_" r"RUN\s*=\s*True\b"
    )
    if hidden_dry_run_pattern.search(source_text):
        return fail("Hidden DRY_RUN=True assignment found in production sources.")

    print("[1/2] Compile all Python sources...")
    if not compileall.compile_dir(ROOT, quiet=1):
        return fail("Python compilation failed.")

    print("[2/2] Run complete offline regression suite...")
    completed = subprocess.run(
        [sys.executable, "-X", "utf8", str(ROOT / "run_tests.py")],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    combined_output = (completed.stdout or "") + (completed.stderr or "")
    print(combined_output, end="" if combined_output.endswith("\n") else "\n")
    if completed.returncode != 0:
        return fail(f"Regression suite exited with {completed.returncode}.")
    count_match = re.search(r"Ran\s+(\d+)\s+tests?", combined_output)
    if not count_match or int(count_match.group(1)) < MINIMUM_TEST_COUNT:
        actual = count_match.group(1) if count_match else "unknown"
        return fail(
            f"Only {actual} tests were discovered; expected at least "
            f"{MINIMUM_TEST_COUNT}."
        )
    missing = sorted(
        name for name in REQUIRED_LIFECYCLE_TESTS if name not in combined_output
    )
    if missing:
        return fail(
            "Required lifecycle scenarios were not executed: " + ", ".join(missing)
        )

    print()
    print("=" * 78)
    print("[PASS] WAVEFRAME ROBOT V8.5.2 RELEASE GATE")
    print("=" * 78)
    print("Verified offline with production code + stateful MT5 simulator:")
    print(f"- {count_match.group(1)} tests executed; required lifecycle names present;")
    print("- market order open and duplicate-send prevention;")
    print("- limit/stop pending placement, restart, fill, cancel and rejection;")
    print("- unknown order_send outcome recovery without a second send;")
    print("- broker-confirmed SL/TP closure and trading statistics;")
    print("- previous-wave stop move through TRADE_ACTION_SLTP;")
    print("- daily baseline recovery and entry-trigger retry continuity.")
    print("- DEMO-only release arming; REAL is rejected until explicitly enabled;")
    print("- refusal recovery is sequential and M30 decision is durable.")
    print()
    print("No real broker order was sent by this offline release gate.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
