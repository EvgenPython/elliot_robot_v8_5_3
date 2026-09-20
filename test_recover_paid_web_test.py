import ast
import json
import re
import sys
import types
import unittest
from pathlib import Path


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

import recover_paid_web_test as recovery


class PaidMapRecoveryTests(unittest.TestCase):
    def test_saved_end_turn_repair_json_is_accepted(self):
        wire = {
            "timestamp": "2026-08-24T11:03:55+03:00",
            "instrument": "XAUUSD",
        }
        message = {
            "stage": "FULL_MAP_REPAIR",
            "stop_reason": "end_turn",
            "content": [{"type": "text", "text": json.dumps(wire)}],
        }
        self.assertEqual(recovery._extract_saved_wire_response(message), wire)

    def test_incomplete_saved_response_is_rejected_before_api(self):
        message = {
            "stage": "FULL_MAP_REPAIR",
            "stop_reason": "max_tokens",
            "content": [{"type": "text", "text": "{}"}],
        }
        with self.assertRaisesRegex(ValueError, "не завершён end_turn"):
            recovery._extract_saved_wire_response(message)

    def test_usage_totals_include_primary_map_and_repair(self):
        total = recovery._sum_usage(
            {"input_tokens": 47_428, "output_tokens": 12_137},
            {"input_tokens": 6_556, "output_tokens": 4_264},
        )
        self.assertEqual(total["input_tokens"], 53_984)
        self.assertEqual(total["output_tokens"], 16_401)

    def test_recovery_script_cannot_call_map_or_trading_pipeline(self):
        source = (BASE_DIR / "recover_paid_web_test.py").read_text(
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
            "analyze_market_map",
            "repair_market_map",
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
