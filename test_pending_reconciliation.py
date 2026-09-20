import unittest
from collections import namedtuple
import re
import sys
import types
from pathlib import Path
from types import SimpleNamespace
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

import pending_executor


class PendingReconciliationRegressionTests(unittest.TestCase):
    def setUp(self):
        self.plan = {
            "plan_id": "5930f79e34034481",
            "symbol": "XAUUSD",
            "action": "enter_long",
            "order_type": "limit",
            "volume": 0.04,
            "pending_ticket": 289503148,
            "send_request": {"volume": 0.04},
        }

    @patch("pending_executor.complete_cancel_actions_for_plan")
    @patch("pending_executor.finalize_active_pending_plan")
    @patch("pending_executor.find_entry_deals_for_plan")
    @patch("pending_executor.get_history_order_for_plan")
    @patch("pending_executor.find_active_pending_orders")
    @patch("pending_executor.find_owned_positions")
    def test_cancelled_history_order_is_resolved_before_deal_lookup(
        self,
        find_positions,
        find_orders,
        history_order,
        find_deals,
        finalize,
        complete_cancel,
    ):
        """Regression for the 2026-09-11 V8.4 reconciliation deadlock."""
        find_positions.return_value = {
            "ok": True,
            "error": None,
            "valid": [],
            "invalid": [],
        }
        find_orders.return_value = {
            "ok": True,
            "error": None,
            "valid": [],
            "invalid": [],
        }
        history_order.return_value = {
            "ok": True,
            "error": None,
            "matches": [
                {
                    "raw": SimpleNamespace(
                        state=int(
                            getattr(
                                pending_executor.mt5,
                                "ORDER_STATE_CANCELED",
                                2,
                            )
                        )
                    ),
                    "dict": {
                        "ticket": 289503148,
                        "state": 2,
                    },
                }
            ],
        }

        report = pending_executor.reconcile_pending_plan(self.plan)

        self.assertTrue(report["resolved"])
        self.assertFalse(report["blocked"])
        self.assertEqual(report["decision"], "PENDING_CANCELLED")
        find_deals.assert_not_called()
        finalize.assert_called_once()
        complete_cancel.assert_called_once()

    @patch("pending_executor.get_symbol_tolerances", return_value=(0.01, 0.001))
    @patch(
        "pending_executor.mt5.last_error",
        return_value=(-2, "Terminal: Invalid params"),
        create=True,
    )
    @patch("pending_executor.mt5.history_deals_get", create=True)
    def test_deal_lookup_falls_back_to_range_and_filters_by_order_ticket(
        self,
        history_deals_get,
        _last_error,
        _tolerances,
    ):
        Deal = namedtuple(
            "Deal",
            "ticket order position_id entry type symbol magic volume price comment",
        )
        common = {
            "position_id": 9001,
            "entry": int(getattr(pending_executor.mt5, "DEAL_ENTRY_IN", 0)),
            "type": int(pending_executor.mt5.DEAL_TYPE_BUY),
            "symbol": "XAUUSD",
            "magic": int(pending_executor.MAGIC_NUMBER),
            "volume": 0.04,
            "price": 4322.0,
            "comment": "",
        }
        wrong = Deal(ticket=1, order=111, **common)
        matching = Deal(ticket=2, order=289503148, **common)
        history_deals_get.side_effect = [None, (wrong, matching)]

        report = pending_executor.find_entry_deals_for_plan(self.plan)

        self.assertTrue(report["ok"])
        self.assertEqual(len(report["matches"]), 1)
        self.assertEqual(report["matches"][0]["dict"]["order"], 289503148)
        self.assertIn("date-range fallback", report["lookup_note"])
        self.assertEqual(history_deals_get.call_count, 2)


if __name__ == "__main__":
    unittest.main()
