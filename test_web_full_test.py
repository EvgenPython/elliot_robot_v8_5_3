import ast
import re
import sys
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

import run_web_full_test as web_test


class PaidWebFullTestSafetyTests(unittest.TestCase):
    def test_paid_test_uses_fresh_v4_journal_stages(self):
        self.assertEqual(web_test.MAP_STAGE, "WEB_TEST_V4_FULL_MAP")
        self.assertEqual(
            web_test.DECISION_STAGE,
            "WEB_TEST_V4_FULL_DECISION",
        )

    def test_explicit_paid_confirmation_is_required(self):
        with patch.object(web_test, "run_paid_web_test") as run:
            self.assertEqual(web_test.main([]), 2)
            run.assert_not_called()

    def test_successful_analysis_only_flow_archives_result(self):
        snapshot = {
            "instrument": "XAUUSD",
            "generated_at_fp": "2026-08-22T17:00:00+03:00",
            "last_tick_time_fp": "2026-08-21T23:59:59+03:00",
        }
        payload = {
            "instrument": "XAUUSD",
            "timestamp": "2026-08-22T17:00:00+03:00",
        }
        market_map = {"instrument": "XAUUSD", "map": "validated"}
        decision = {"instrument": "XAUUSD", "decision": "validated"}
        analysis = {
            "instrument": "XAUUSD",
            "recommendation": {"action": "stay_out"},
        }

        with (
            patch.object(web_test, "_collect_snapshot", return_value=snapshot),
            patch.object(
                web_test,
                "extract_latest_closed_h1_time",
                return_value="2026-08-21T22:00:00+03:00",
            ),
            patch.object(
                web_test, "save_web_market_snapshot", return_value=True
            ),
            patch.object(web_test, "build_claude_payload", return_value=payload),
            patch.object(web_test, "print_payload_stats"),
            patch.object(web_test, "save_debug_payload"),
            patch.object(
                web_test, "get_fresh_reference_for_payload", return_value=None
            ),
            patch.object(
                web_test,
                "save_analysis_archive",
                return_value=Path("archive.json"),
            ),
            patch.object(web_test, "safe_update_analysis_archive") as update,
            patch.object(
                web_test,
                "_run_market_map",
                return_value={
                    "ok": True,
                    "result": market_map,
                    "usage": {"input_tokens": 10},
                },
            ),
            patch.object(
                web_test,
                "_run_trade_decision",
                return_value=(
                    {
                        "ok": True,
                        "result": decision,
                        "usage": {"output_tokens": 5},
                    },
                    analysis,
                ),
            ),
            patch.object(
                web_test,
                "combine_stage_usage",
                return_value={"totals": {}},
            ),
            patch.object(
                web_test,
                "_build_known_api_cost_audit",
                return_value={"known_usage_attempts": 2},
            ),
            patch.object(web_test, "save_debug_response"),
            patch.object(web_test, "print_analysis_summary"),
        ):
            self.assertEqual(web_test.run_paid_web_test(), 0)

        final_updates = [
            call.kwargs
            for call in update.call_args_list
            if call.kwargs.get("result") is analysis
        ]
        self.assertEqual(len(final_updates), 1)
        self.assertIn("api_usage", final_updates[0])

    def test_script_contains_no_trading_pipeline_calls(self):
        source = (BASE_DIR / "run_web_full_test.py").read_text(
            encoding="utf-8"
        )
        tree = ast.parse(source)
        called_names = set()
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if isinstance(node.func, ast.Name):
                called_names.add(node.func.id)
            elif isinstance(node.func, ast.Attribute):
                called_names.add(node.func.attr)

        forbidden = {
            "order_send",
            "evaluate_trade",
            "register_trade_decision",
            "execute_active_plan",
            "register_completed_analysis",
            "save_reference_analysis",
        }
        self.assertFalse(called_names & forbidden)


if __name__ == "__main__":
    unittest.main()
