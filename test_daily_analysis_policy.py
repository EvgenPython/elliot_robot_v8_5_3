import ast
import inspect
import json
import re
import sys
import tempfile
import textwrap
import types
import unittest
from pathlib import Path
from unittest.mock import patch


if "MetaTrader5" not in sys.modules:
    try:
        import MetaTrader5  # noqa: F401
    except ModuleNotFoundError:
        mt5_stub = types.ModuleType("MetaTrader5")
        names = set()
        for source_path in Path(__file__).resolve().parent.glob("*.py"):
            names.update(
                re.findall(
                    r"mt5\.([A-Z][A-Z0-9_]+)",
                    source_path.read_text(encoding="utf-8"),
                )
            )
        for index, name in enumerate(sorted(names), 1):
            setattr(mt5_stub, name, index)
        mt5_stub.last_error = lambda: (0, "test")
        sys.modules["MetaTrader5"] = mt5_stub

if "anthropic" not in sys.modules:
    try:
        import anthropic  # noqa: F401
    except ModuleNotFoundError:
        anthropic_stub = types.ModuleType("anthropic")

        class Anthropic:
            pass

        class AnthropicError(Exception):
            pass

        anthropic_stub.Anthropic = Anthropic
        for name in (
            "AuthenticationError",
            "PermissionDeniedError",
            "RateLimitError",
            "APITimeoutError",
            "APIConnectionError",
            "APIStatusError",
        ):
            setattr(anthropic_stub, name, type(name, (AnthropicError,), {}))
        sys.modules["anthropic"] = anthropic_stub

import analysis_schedule as schedule
import claude_reference_state as reference_state
import main as robot_main
import runner
import scout_client
import scout_payload
import test_professional_trader_analysis as paid_analysis_test
import web_runtime_state


def _snapshot(h1_open_time_fp: str) -> dict:
    return {
        "instrument": "XAUUSD",
        "generated_at_fp": h1_open_time_fp,
        "timeframes": {
            "H1": {
                "closed_bars": [
                    {"time": h1_open_time_fp},
                ],
            },
        },
    }


class DailyBaselineScheduleTests(unittest.TestCase):
    def test_exactly_one_mandatory_full_hour_exists(self):
        self.assertEqual(schedule.DAILY_BASELINE_FULL_CLOSE_HOUR, 8)
        self.assertEqual(schedule.MANDATORY_FULL_CLOSE_HOURS, (8,))

    def test_h1_closed_at_0800_fp_is_daily_full(self):
        result = schedule.inspect_analysis_schedule(
            _snapshot("2026-08-24T07:00:00+03:00")
        )
        self.assertTrue(result["mandatory_full"])
        self.assertTrue(result["daily_baseline_full"])
        self.assertEqual(result["cycle_mode"], schedule.CYCLE_FULL_SCHEDULED)

    def test_other_working_h1_closes_are_scout_cycles(self):
        for h1_open_hour in (8, 11, 15, 19, 21):
            with self.subTest(h1_open_hour=h1_open_hour):
                result = schedule.inspect_analysis_schedule(
                    _snapshot(
                        f"2026-08-24T{h1_open_hour:02d}:00:00+03:00"
                    )
                )
                self.assertFalse(result["mandatory_full"])
                self.assertEqual(result["cycle_mode"], schedule.CYCLE_SCOUT)

    def test_weekend_never_has_scheduled_full(self):
        result = schedule.inspect_analysis_schedule(
            _snapshot("2026-08-22T07:00:00+03:00")
        )
        self.assertFalse(result["mandatory_full"])


class DailyReferencePolicyTests(unittest.TestCase):
    @staticmethod
    def _reference() -> dict:
        return {
            "instrument": "XAUUSD",
            "saved_at_fp": "2026-08-24T08:10:00+03:00",
            "market_snapshot_time_fp": "2026-08-24T08:00:05+03:00",
            "h1_closed_bar_time_fp": "2026-08-24T07:00:00+03:00",
            "analysis": {"recommendation": {"action": "stay_out"}},
        }

    def test_daily_reference_remains_available_late_same_fp_day(self):
        reference = self._reference()
        payload = {
            "instrument": "XAUUSD",
            "timestamp": "2026-08-24T22:59:00+03:00",
        }
        with patch.object(
            reference_state, "load_reference_state", return_value=reference
        ):
            self.assertIs(
                reference_state.get_fresh_reference_for_payload(payload),
                reference,
            )

    def test_previous_fp_day_reference_is_rejected(self):
        reference = self._reference()
        payload = {
            "instrument": "XAUUSD",
            "timestamp": "2026-08-25T08:00:05+03:00",
        }
        with patch.object(
            reference_state, "load_reference_state", return_value=reference
        ):
            self.assertIsNone(
                reference_state.get_fresh_reference_for_payload(payload)
            )

    def test_reference_age_policy_covers_complete_working_window(self):
        self.assertGreaterEqual(reference_state.MAX_REFERENCE_AGE_HOURS, 16)

    def test_scout_h1_tape_covers_complete_day_after_baseline(self):
        self.assertGreaterEqual(scout_payload.SCOUT_CLOSED_BAR_LIMITS["H1"], 16)
        self.assertLess(
            scout_payload.SCOUT_CLOSED_BAR_LIMITS["M5"],
            24 * 12,
        )

    def test_scout_policy_uses_single_schedule_source(self):
        self.assertEqual(
            scout_payload.DAILY_BASELINE_FULL_CLOSE_HOUR,
            schedule.DAILY_BASELINE_FULL_CLOSE_HOUR,
        )


class CheapScoutPolicyTests(unittest.TestCase):
    def test_scout_uses_separate_haiku_model_by_default(self):
        self.assertEqual(
            scout_client.get_scout_model({"model": "claude-sonnet-5"}),
            scout_client.DEFAULT_SCOUT_MODEL,
        )
        self.assertIn("haiku", scout_client.DEFAULT_SCOUT_MODEL)

    def test_haiku_scout_omits_unsupported_effort_parameter(self):
        model = scout_client.get_scout_model({})
        effort = scout_client.get_scout_effort({}, model)
        self.assertIsNone(effort)
        self.assertNotIn(
            "effort",
            scout_client._scout_output_config(effort),
        )

    def test_scout_model_can_be_overridden_without_changing_full_model(self):
        config = {
            "model": "claude-sonnet-5",
            "scout_model": "claude-sonnet-4-6",
            "scout_effort": "low",
        }
        self.assertEqual(
            scout_client.get_scout_model(config),
            "claude-sonnet-4-6",
        )
        self.assertEqual(
            scout_client.get_scout_effort(
                config,
                scout_client.get_scout_model(config),
            ),
            "low",
        )

    def test_only_new_semantic_event_forces_full(self):
        base = {
            "material_change": False,
            "possible_setup": False,
            "full_analysis_required": False,
            "confidence": "high",
            "trigger_kind": "unchanged",
        }
        payload = {"timestamp": "2026-08-24T10:00:00+03:00"}

        unchanged = scout_client._normalize_result(dict(base), payload)
        self.assertFalse(unchanged["full_analysis_required"])

        for trigger_kind in (
            "same_move",
            "expected_continuation",
            "approaching_level",
            "expected_level_break",
        ):
            with self.subTest(trigger_kind=trigger_kind):
                candidate = dict(
                    base,
                    trigger_kind=trigger_kind,
                    confidence="medium",
                    material_change=True,
                )
                normalized = scout_client._normalize_result(candidate, payload)
                self.assertFalse(normalized["full_analysis_required"])

        for trigger_kind in (
            "structure_change",
            "new_wave",
            "pullback",
            "retest",
            "consolidation",
            "reversal",
            "character_change",
            "scenario_invalidation",
            "reference_conflict",
            "possible_setup",
        ):
            with self.subTest(trigger_kind=trigger_kind):
                candidate = dict(base, trigger_kind=trigger_kind)
                normalized = scout_client._normalize_result(candidate, payload)
                self.assertTrue(normalized["full_analysis_required"])

        setup_overrides_same_move = dict(
            base,
            trigger_kind="same_move",
            possible_setup=True,
        )
        normalized = scout_client._normalize_result(
            setup_overrides_same_move,
            payload,
        )
        self.assertTrue(normalized["full_analysis_required"])

    def test_shock_move_continuation_does_not_repeat_full(self):
        payload = {"timestamp": "2026-08-28T20:00:00+03:00"}
        same_bearish_impulse = {
            "material_change": True,
            "possible_setup": False,
            "full_analysis_required": True,
            "confidence": "medium",
            "trigger_kind": "same_move",
            "observed_changes": [
                "Bearish impulse made another low in the same wave",
                "Next support on the existing path was broken",
            ],
            "reason": (
                "The prior FULL already mapped the bearish shock move; "
                "there is no pullback, retest, base, reversal, or setup."
            ),
        }

        normalized = scout_client._normalize_result(
            same_bearish_impulse,
            payload,
        )
        self.assertFalse(normalized["full_analysis_required"])

        for trigger_kind in ("pullback", "retest", "reversal"):
            with self.subTest(trigger_kind=trigger_kind):
                event = dict(
                    same_bearish_impulse,
                    material_change=True,
                    trigger_kind=trigger_kind,
                )
                normalized = scout_client._normalize_result(event, payload)
                self.assertTrue(normalized["full_analysis_required"])

    def test_deterministic_entry_invalidation_cannot_be_called_continuation(self):
        payload = {
            "timestamp": "2026-09-01T09:00:00+03:00",
            "deterministic_reference_facts": {
                "entry_projection": {
                    "status": "invalidated",
                    "projection": {"timeframe": "M5"},
                    "terminal_event": {
                        "kind": "entry_projection_invalidated",
                        "level": 4423.95,
                        "bar": {"close": 4418.2},
                    },
                }
            },
        }
        model_result = {
            "material_change": False,
            "possible_setup": False,
            "full_analysis_required": False,
            "confidence": "high",
            "trigger_kind": "expected_continuation",
            "observed_changes": [],
            "reason": "EN: No break.\nRU: Пробоя нет.",
        }
        normalized = scout_client._normalize_result(model_result, payload)
        self.assertEqual(
            normalized["trigger_kind"], "entry_projection_invalidated"
        )
        self.assertFalse(normalized["full_analysis_required"])
        self.assertIn("4418.2", normalized["reason"])
        self.assertIn("4423.95", normalized["reason"])


class PositionHoldPolicyTests(unittest.TestCase):
    def test_main_position_gate_precedes_market_snapshot_and_returns(self):
        tree = ast.parse(textwrap.dedent(inspect.getsource(robot_main.main)))
        calls = {}
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            function = node.func
            if isinstance(function, ast.Name):
                calls.setdefault(function.id, []).append(node.lineno)

        self.assertLess(
            min(calls["inspect_position_gate"]),
            min(calls["get_market_snapshot"]),
        )

        blocking_if = None
        for node in ast.walk(tree):
            if not isinstance(node, ast.If):
                continue
            test_text = ast.unparse(node.test)
            if "position_gate" in test_text and "block_claude" in test_text:
                blocking_if = node
                break

        self.assertIsNotNone(blocking_if)
        self.assertTrue(
            any(isinstance(item, ast.Return) for item in blocking_if.body)
        )

    def test_runner_position_hold_skips_flat_h1_branch(self):
        tree = ast.parse(textwrap.dedent(inspect.getsource(runner.run_forever)))
        managed_hold = None
        for node in ast.walk(tree):
            if not isinstance(node, ast.If):
                continue
            if ast.unparse(node.test).strip() == "managed_positions":
                managed_hold = node
                break

        self.assertIsNotNone(managed_hold)
        self.assertTrue(
            any(isinstance(item, ast.Continue) for item in managed_hold.body)
        )


class RunnerAnalysisClockTests(unittest.TestCase):
    def test_pending_uses_bounded_multi_h1_lifetime_and_cancel_is_retried(self):
        plan = {"order_type": "limit", "source_h1_closed_bar_time": "2026-09-11T09:00:00+03:00"}
        gate = {"analysis_window_allowed": True, "latest_closed_h1_time": plan["source_h1_closed_bar_time"]}
        self.assertIsNone(runner._pending_cancel_reason(plan, gate))
        gate["latest_closed_h1_time"] = "2026-09-11T10:00:00+03:00"
        self.assertIsNone(runner._pending_cancel_reason(plan, gate))
        gate["latest_closed_h1_time"] = "2026-09-11T12:00:00+03:00"
        self.assertIn("Срок pending истёк", runner._pending_cancel_reason(plan, gate))
        gate["latest_closed_h1_time"] = plan["source_h1_closed_bar_time"]
        plan.update(execution_status="cancel_requested", cancel_reason="previous cancellation")
        self.assertEqual(runner._pending_cancel_reason(plan, gate), "previous cancellation")

    def test_runner_still_runs_observation_when_reconciliation_fails(self):
        gate = {"allowed": True, "analysis_window_allowed": True, "latest_closed_h1_time": "2026-09-11T10:00:00+03:00"}
        with (
            patch.object(runner, "connect_mt5", return_value=True),
            patch.object(runner, "disconnect_mt5"),
            patch.object(runner, "_refresh_daily_state", return_value={"fp_day": "2026-09-11"}),
            patch.object(runner, "_print_runner_header"),
            patch.object(runner, "_print_daily_rollover"),
            patch.object(runner, "write_runner_status"),
            patch.object(runner, "get_managed_positions", return_value=[]),
            patch.object(runner, "get_active_plan", return_value={"order_type": "limit"}),
            patch.object(runner, "reconcile_active_pending_execution", return_value={"blocked": True, "decision": "RECONCILIATION_ERROR"}),
            patch.object(runner, "inspect_market_runtime_gate", return_value=gate),
            patch.object(runner, "load_analysis_state", return_value={}),
            patch.object(runner, "_run_full_cycle") as run_cycle,
            patch.object(runner, "execute_pending_cancel") as cancel,
            patch.object(runner.time, "sleep", side_effect=KeyboardInterrupt),
        ):
            with self.assertRaises(KeyboardInterrupt):
                runner.run_forever()
        run_cycle.assert_called_once_with("FRESH_NEW_H1_EXECUTION_QUARANTINE", analysis_only=True)
        cancel.assert_not_called()

    def test_pending_quarantine_processes_each_closed_h1_once(self):
        gate = {
            "allowed": True,
            "latest_closed_h1_time": "2026-09-11T10:00:00+03:00",
        }
        with patch.object(
            runner,
            "load_analysis_state",
            return_value={"last_analyzed_h1": "2026-09-11T09:00:00+03:00"},
        ):
            due, latest = runner._fresh_h1_analysis_due(
                gate,
                last_attempt_h1=None,
                last_attempt_at=None,
            )
        self.assertTrue(due)
        self.assertEqual(latest, "2026-09-11T10:00:00+03:00")

        with patch.object(
            runner,
            "load_analysis_state",
            return_value={"last_analyzed_h1": latest},
        ):
            due, _ = runner._fresh_h1_analysis_due(
                gate,
                last_attempt_h1=latest,
                last_attempt_at=None,
            )
        self.assertFalse(due)

    def test_failed_same_h1_waits_before_retry(self):
        gate = {
            "allowed": True,
            "latest_closed_h1_time": "2026-09-11T10:00:00+03:00",
        }
        with (
            patch.object(
                runner,
                "load_analysis_state",
                return_value={"last_analyzed_h1": "2026-09-11T09:00:00+03:00"},
            ),
            patch.object(runner, "_seconds_since", return_value=10),
        ):
            due, _ = runner._fresh_h1_analysis_due(
                gate,
                last_attempt_h1="2026-09-11T10:00:00+03:00",
                last_attempt_at=None,
            )
        self.assertFalse(due)


class PaidDiagnosticSafetyTests(unittest.TestCase):
    def test_legacy_professional_test_requires_explicit_paid_confirmation(self):
        self.assertEqual(paid_analysis_test.main([]), 2)


class RuntimeTelemetryPolicyTests(unittest.TestCase):
    def test_open_profit_includes_swap_and_unknown_is_not_zero(self):
        positions = [types.SimpleNamespace(profit=134.04, swap=-11.92)]
        with patch.object(web_runtime_state.mt5, "positions_get", return_value=positions, create=True):
            actual = web_runtime_state._open_positions_telemetry()
        self.assertTrue(actual["available"])
        self.assertAlmostEqual(actual["profit_plus_swap"], 122.12)
        with patch.object(web_runtime_state.mt5, "positions_get", return_value=None, create=True):
            self.assertEqual(web_runtime_state._open_positions_telemetry(), {"available": False})

    def test_runner_status_identifies_active_daily_full_policy(self):
        with tempfile.TemporaryDirectory() as directory:
            status_path = Path(directory) / "runner_status.json"
            with (
                patch.object(web_runtime_state, "RUNNER_STATUS_PATH", status_path),
                patch.object(web_runtime_state, "TRADE_STATE_PATH", Path(directory) / "trade.json"),
                patch.object(web_runtime_state, "ANALYSIS_STATE_PATH", Path(directory) / "analysis.json"),
                patch.object(web_runtime_state, "POSITION_MONITOR_STATE_PATH", Path(directory) / "monitor.json"),
                patch.object(web_runtime_state, "POSITION_PROTECTION_STATE_PATH", Path(directory) / "protection.json"),
            ):
                self.assertTrue(
                    web_runtime_state.write_runner_status(
                        connected=False,
                        daily_state=None,
                    )
                )

            payload = json.loads(status_path.read_text(encoding="utf-8"))
            policy = payload["analysis_policy"]
            self.assertEqual(
                policy["version"],
                schedule.ANALYSIS_POLICY_VERSION,
            )
            self.assertEqual(policy["scheduled_fulls_per_fp_day"], 1)
            self.assertEqual(
                policy["daily_baseline_full_close_hour_fp"],
                8,
            )
            self.assertEqual(
                policy["other_closed_h1_mode"], "SCOUT_THEN_H1_DECISION"
            )
            self.assertTrue(policy["decision_refresh_each_closed_h1"])
            self.assertEqual(
                policy["managed_position_mode"],
                "M15_SCOUT_H1_DEEP_ELLIOTT_PROTECTION",
            )
            self.assertTrue(policy["position_monitor_automatic_trade_changes"])
            self.assertTrue(policy["never_widen_stop"])
            self.assertTrue(policy["resume_after_confirmed_close"])


if __name__ == "__main__":
    unittest.main()
