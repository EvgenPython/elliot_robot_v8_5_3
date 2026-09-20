import json
import re
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch


BASE_DIR = Path(__file__).resolve().parent


def _install_runtime_stubs():
    mt5 = types.ModuleType("MetaTrader5")
    names = set()
    for source_path in BASE_DIR.glob("*.py"):
        names.update(
            re.findall(
                r"mt5\.([A-Z][A-Z0-9_]+)",
                source_path.read_text(encoding="utf-8"),
            )
        )
    for index, name in enumerate(sorted(names), 1):
        setattr(mt5, name, index)
    sys.modules.setdefault("MetaTrader5", mt5)

    if "anthropic" not in sys.modules:
        anthropic = types.ModuleType("anthropic")

        class Anthropic:
            pass

        class AnthropicError(Exception):
            pass

        anthropic.Anthropic = Anthropic
        for name in (
            "AuthenticationError",
            "PermissionDeniedError",
            "RateLimitError",
            "APITimeoutError",
            "APIConnectionError",
            "APIStatusError",
        ):
            setattr(anthropic, name, type(name, (AnthropicError,), {}))
        sys.modules["anthropic"] = anthropic


_install_runtime_stubs()

import claude_request_guard
import main
from claude_client import (
    ClaudeInvalidResponseError,
    ClaudeRefusalResponseError,
    ClaudeRequestOutcomeUnknownError,
    get_claude_api_retry_context,
    get_api_retry_policy,
)


class ControlledRetryIntegrationTests(unittest.TestCase):
    def test_default_policy_is_five_sequential_full_attempts_and_two_repairs(self):
        full = get_api_retry_policy("FULL_MAP", config={})
        repair = get_api_retry_policy("FULL_MAP_REPAIR", config={})
        self.assertEqual(full["max_attempts"], 5)
        self.assertEqual(full["retry_delays_seconds"], [30.0, 90.0, 180.0, 300.0])
        self.assertEqual(repair["max_attempts"], 2)
        self.assertEqual(repair["retry_delays_seconds"], [10.0])

    def test_invalid_response_returns_repair_context_without_full_retry(self):
        snapshot = {"instrument": "XAUUSD"}
        h1_time = "2026-08-17T09:00:00+03:00"
        calls = []
        invalid = {"instrument": "XAUUSD", "broken": True}

        with tempfile.TemporaryDirectory() as temporary_dir:
            temporary_dir = Path(temporary_dir)
            archive_path = temporary_dir / "archive.json"
            archive_path.write_text(
                json.dumps({"result": None, "api_usage": None}),
                encoding="utf-8",
            )

            def api_call(on_preflight, _on_response):
                calls.append(1)
                on_preflight(
                    {
                        "input_tokens": 100,
                        "payload_sha256": "same",
                        "usage": {"input_tokens": 100, "output_tokens": 20},
                    }
                )
                raise ClaudeInvalidResponseError(
                    "semantic validation failed",
                    invalid_result=invalid,
                    validation_error="anchor ids do not match",
                )

            with (
                patch.object(
                    claude_request_guard,
                    "STATE_PATH",
                    temporary_dir / "journal.json",
                ),
                patch.object(
                    main,
                    "extract_latest_closed_h1_time",
                    return_value=h1_time,
                ),
                patch.object(
                    main,
                    "get_api_retry_policy",
                    return_value={
                        "max_attempts": 5,
                        "retry_delays_seconds": [0, 0, 0, 0],
                    },
                ),
            ):
                run = main._run_api_with_retries(
                    snapshot=snapshot,
                    api_stage="FULL_MAP",
                    cycle_type="FULL_SCHEDULED",
                    payload_timestamp="2026-08-17T10:01:00+03:00",
                    archive_path=archive_path,
                    api_call=api_call,
                )

        self.assertEqual(calls, [1])
        self.assertFalse(run["ok"])
        self.assertTrue(run["repairable"])
        self.assertEqual(run["invalid_result"], invalid)
        self.assertEqual(run["validation_error"], "anchor ids do not match")

    def test_invalid_repair_is_exhausted_without_second_repair(self):
        snapshot = {"instrument": "XAUUSD"}
        h1_time = "2026-08-24T10:00:00+03:00"
        invalid = {"data_quality": ["true", "a", "b", "c"]}

        with tempfile.TemporaryDirectory() as temporary_dir:
            temporary_dir = Path(temporary_dir)
            archive_path = temporary_dir / "archive.json"
            archive_path.write_text(
                json.dumps({"result": None, "api_usage": None}),
                encoding="utf-8",
            )

            def api_call(on_preflight, _on_response):
                on_preflight({"input_tokens": 100, "payload_sha256": "same"})
                raise ClaudeInvalidResponseError(
                    "repair semantic validation failed",
                    invalid_result=invalid,
                    validation_error="data_quality length",
                )

            with (
                patch.object(
                    claude_request_guard,
                    "STATE_PATH",
                    temporary_dir / "journal.json",
                ),
                patch.object(
                    main,
                    "extract_latest_closed_h1_time",
                    return_value=h1_time,
                ),
                patch.object(
                    main,
                    "get_api_retry_policy",
                    return_value={
                        "max_attempts": 1,
                        "retry_delays_seconds": [],
                    },
                ),
            ):
                run = main._run_api_with_retries(
                    snapshot=snapshot,
                    api_stage="FULL_MAP_REPAIR",
                    cycle_type="FULL_WEB_TEST",
                    payload_timestamp="2026-08-24T11:03:55+03:00",
                    archive_path=archive_path,
                    api_call=api_call,
                )

        self.assertFalse(run["ok"])
        self.assertFalse(run["repairable"])
        self.assertTrue(run["exhausted"])
        self.assertEqual(run["invalid_result"], invalid)

    def test_one_lost_stream_then_one_validated_winner_and_recovery(self):
        snapshot = {"instrument": "XAUUSD"}
        h1_time = "2026-08-17T09:00:00+03:00"
        calls = []
        validated_result = {"instrument": "XAUUSD", "validated": True}

        with tempfile.TemporaryDirectory() as temporary_dir:
            temporary_dir = Path(temporary_dir)
            archive_path = temporary_dir / "archive.json"
            archive_path.write_text(
                json.dumps({"result": None, "api_usage": None}),
                encoding="utf-8",
            )

            def api_call(on_preflight, on_response):
                attempt_number = len(calls) + 1
                calls.append(attempt_number)
                on_preflight(
                    {
                        "model": "claude-sonnet-5",
                        "input_tokens": 50000,
                        "transport_payload_bytes": 95000,
                        "response_received": False,
                    }
                )
                if attempt_number < 2:
                    raise ClaudeRequestOutcomeUnknownError(
                        f"lost stream {attempt_number}"
                    )
                on_response(
                    {
                        "request_id": "req_winner",
                        "stop_reason": "end_turn",
                        "response_received": True,
                        "usage": {"input_tokens": 50000, "output_tokens": 8000},
                    }
                )
                return dict(validated_result)

            with (
                patch.object(
                    claude_request_guard,
                    "STATE_PATH",
                    temporary_dir / "journal.json",
                ),
                patch.object(
                    main,
                    "extract_latest_closed_h1_time",
                    return_value=h1_time,
                ),
                patch.object(
                    main,
                    "get_api_retry_policy",
                    return_value={
                        "max_attempts": 2,
                        "retry_delays_seconds": [0],
                        "outcome_unknown_min_delay_seconds": 0,
                    },
                ),
                patch.object(main.time, "sleep"),
            ):
                run = main._run_api_with_retries(
                    snapshot=snapshot,
                    api_stage="FULL",
                    cycle_type="FULL_SCHEDULED",
                    payload_timestamp="2026-08-17T10:01:00+03:00",
                    archive_path=archive_path,
                    api_call=api_call,
                )
                cycle = claude_request_guard.get_api_cycle(h1_time, "FULL")

                recovered = main._run_api_with_retries(
                    snapshot=snapshot,
                    api_stage="FULL",
                    cycle_type="FULL_SCHEDULED",
                    payload_timestamp="2026-08-17T10:01:00+03:00",
                    archive_path=archive_path,
                    api_call=lambda *_: self.fail("API must not run twice"),
                )

        self.assertTrue(run["ok"])
        self.assertEqual(calls, [1, 2])
        self.assertEqual(cycle["status"], "VALIDATED")
        self.assertEqual(len(cycle["attempts"]), 2)
        self.assertEqual(cycle["attempts"][0]["status"], "FAILED_RETRYABLE")
        self.assertEqual(cycle["attempts"][1]["status"], "VALIDATED")
        self.assertEqual(cycle["winner_attempt_id"], run["attempt"]["attempt_id"])
        self.assertTrue(recovered["ok"])
        self.assertTrue(recovered["recovered"])
        self.assertEqual(recovered["result"], validated_result)

    def test_billed_refusal_is_retried_sequentially_with_recovery_context(self):
        snapshot = {"instrument": "XAUUSD"}
        h1_time = "2026-09-17T09:00:00+03:00"
        contexts = []
        validated_result = {"instrument": "XAUUSD", "validated": True}

        with tempfile.TemporaryDirectory() as temporary_dir:
            temporary_dir = Path(temporary_dir)
            archive_path = temporary_dir / "archive.json"
            archive_path.write_text(
                json.dumps({"result": None, "api_usage": None}),
                encoding="utf-8",
            )

            def api_call(on_preflight, _on_response):
                contexts.append(get_claude_api_retry_context())
                on_preflight(
                    {
                        "input_tokens": 100,
                        "payload_sha256": "same-frozen-payload",
                        "usage": {"input_tokens": 100, "output_tokens": 5},
                    }
                )
                if len(contexts) == 1:
                    raise ClaudeRefusalResponseError("provider refusal")
                return dict(validated_result)

            with (
                patch.object(
                    claude_request_guard,
                    "STATE_PATH",
                    temporary_dir / "journal.json",
                ),
                patch.object(
                    main,
                    "extract_latest_closed_h1_time",
                    return_value=h1_time,
                ),
                patch.object(
                    main,
                    "get_api_retry_policy",
                    return_value={
                        "max_attempts": 3,
                        "retry_delays_seconds": [0, 0],
                        "outcome_unknown_min_delay_seconds": 0,
                    },
                ),
                patch.object(main.time, "sleep"),
            ):
                run = main._run_api_with_retries(
                    snapshot=snapshot,
                    api_stage="FULL_DECISION",
                    cycle_type="FULL_SCHEDULED",
                    payload_timestamp="2026-09-17T10:00:05+03:00",
                    archive_path=archive_path,
                    api_call=api_call,
                )

        self.assertTrue(run["ok"])
        self.assertEqual(len(contexts), 2)
        self.assertIsNone(contexts[0].get("previous_failure_class"))
        self.assertEqual(
            contexts[1].get("previous_failure_class"),
            "REFUSAL_RESPONSE",
        )

    def test_two_unknown_outcomes_open_circuit_without_third_paid_call(self):
        snapshot = {"instrument": "XAUUSD"}
        h1_time = "2026-08-17T09:00:00+03:00"
        calls = []

        with tempfile.TemporaryDirectory() as temporary_dir:
            temporary_dir = Path(temporary_dir)
            archive_path = temporary_dir / "archive.json"
            archive_path.write_text(
                json.dumps({"result": None, "api_usage": None}),
                encoding="utf-8",
            )

            def api_call(on_preflight, _on_response):
                calls.append(len(calls) + 1)
                on_preflight(
                    {
                        "model": "claude-sonnet-5",
                        "input_tokens": 50000,
                        "payload_sha256": "same-frozen-payload",
                    }
                )
                raise ClaudeRequestOutcomeUnknownError("lost paid stream")

            with (
                patch.object(
                    claude_request_guard,
                    "STATE_PATH",
                    temporary_dir / "journal.json",
                ),
                patch.object(
                    main,
                    "extract_latest_closed_h1_time",
                    return_value=h1_time,
                ),
                # Even a dangerous old/local override cannot buy attempt 3.
                patch.object(
                    main,
                    "get_api_retry_policy",
                    return_value={
                        "max_attempts": 5,
                        "retry_delays_seconds": [0, 0, 0, 0],
                        "outcome_unknown_min_delay_seconds": 0,
                    },
                ),
                patch.object(main.time, "sleep"),
            ):
                run = main._run_api_with_retries(
                    snapshot=snapshot,
                    api_stage="FULL_MAP",
                    cycle_type="FULL_SCHEDULED",
                    payload_timestamp="2026-08-17T10:01:00+03:00",
                    archive_path=archive_path,
                    api_call=api_call,
                )
                cycle = claude_request_guard.get_api_cycle(
                    h1_time, "FULL_MAP"
                )

        self.assertFalse(run["ok"])
        self.assertEqual(calls, [1, 2])
        self.assertEqual(cycle["status"], "EXHAUSTED")
        self.assertEqual(cycle["block_reason"], "OUTCOME_UNKNOWN_LIMIT")
        self.assertTrue(cycle["alert_required"])
        self.assertEqual(
            cycle["attempts"][-1]["failure_class"],
            "OUTCOME_UNKNOWN_LIMIT",
        )


if __name__ == "__main__":
    unittest.main()
