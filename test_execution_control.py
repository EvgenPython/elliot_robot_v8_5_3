import json
import sys
import tempfile
import types
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

if "MetaTrader5" not in sys.modules:
    mt5_stub = types.ModuleType("MetaTrader5")
    mt5_stub.ACCOUNT_TRADE_MODE_DEMO = 0
    mt5_stub.ACCOUNT_TRADE_MODE_CONTEST = 1
    mt5_stub.ACCOUNT_TRADE_MODE_REAL = 2
    mt5_stub.account_info = lambda: None
    mt5_stub.terminal_info = lambda: None
    sys.modules["MetaTrader5"] = mt5_stub

import execution_control


class ExecutionControlTests(unittest.TestCase):
    def _inspect(self, config, *, trade_mode=0, server="AnyBroker-MT5", allowed=True):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "execution.json"
            path.write_text(json.dumps(config), encoding="utf-8")
            account = SimpleNamespace(
                trade_mode=trade_mode,
                login=123456,
                server=server,
                trade_allowed=allowed,
            )
            terminal = SimpleNamespace(connected=True, trade_allowed=allowed)
            with (
                patch.object(execution_control, "EXECUTION_CONFIG_PATH", path),
                patch.object(
                    execution_control.mt5,
                    "account_info",
                    return_value=account,
                    create=True,
                ),
                patch.object(
                    execution_control.mt5,
                    "terminal_info",
                    return_value=terminal,
                    create=True,
                ),
            ):
                return execution_control.inspect_execution_safety_gate()

    def test_disabled_config_is_explicitly_trading_disabled(self):
        report = self._inspect({
            "trading_enabled": False,
            "allowed_account_modes": ["DEMO", "CONTEST", "REAL"],
        })
        self.assertEqual(report["mode"], "TRADING_DISABLED")
        self.assertFalse(report["order_send_allowed"])

    def test_live_is_not_bound_to_broker_name(self):
        report = self._inspect({
            "trading_enabled": True,
            "allowed_account_modes": ["DEMO", "CONTEST", "REAL"],
        }, trade_mode=0, server="Completely-Different-Broker")
        self.assertEqual(report["mode"], "LIVE")
        self.assertTrue(report["order_send_allowed"])

    def test_real_account_can_be_explicitly_allowed(self):
        report = self._inspect({
            "trading_enabled": True,
            "allowed_account_modes": ["DEMO", "CONTEST", "REAL"],
        }, trade_mode=2)
        self.assertTrue(report["order_send_allowed"])

    def test_demo_release_does_not_silently_arm_real_account(self):
        report = self._inspect({
            "trading_enabled": True,
            "allowed_account_modes": ["DEMO"],
        }, trade_mode=2)
        self.assertFalse(report["order_send_allowed"])
        self.assertTrue(any("не разрешён" in item for item in report["errors"]))

    def test_terminal_trade_disabled_blocks_live(self):
        report = self._inspect({
            "trading_enabled": True,
            "allowed_account_modes": ["DEMO", "CONTEST", "REAL"],
        }, allowed=False)
        self.assertFalse(report["order_send_allowed"])
        self.assertTrue(report["errors"])


if __name__ == "__main__":
    unittest.main()
