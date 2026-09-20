import copy
import re
import sys
import tempfile
import types
import unittest
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

import position_protection as protection


def _snapshot():
    bars = [
        {
            "time_fp": "2026-09-10T09:00:00+03:00",
            "open": 101.0,
            "high": 104.0,
            "low": 100.0,
            "close": 103.0,
        },
        {
            "time_fp": "2026-09-10T09:15:00+03:00",
            "open": 103.0,
            "high": 110.0,
            "low": 102.0,
            "close": 109.0,
        },
        {
            "time_fp": "2026-09-10T09:30:00+03:00",
            "open": 109.0,
            "high": 111.0,
            "low": 105.0,
            "close": 108.0,
        },
    ]
    return {
        "instrument": "XAUUSD",
        "symbol_info": {
            "digits": 1,
            "point": 0.1,
            "trade_tick_size": 0.1,
            "trade_stops_level": 10,
            "trade_freeze_level": 0,
        },
        "tick": {
            "bid": 112.0,
            "ask": 112.2,
            "spread_price": 0.2,
        },
        "timeframes": {
            "M15": {"closed_bars": bars},
            "H1": {"closed_bars": copy.deepcopy(bars)},
        },
    }


def _management(action="tighten_stop_and_recalculate_target"):
    return {
        "action": action,
        "structure_confirmed": True,
        "position_direction": "bullish",
        "current_m15_wave": "3",
        "stop_reference_wave": "1",
        "stop_wave_start_time": "2026-09-10T09:00:00+03:00",
        "stop_wave_end_time": "2026-09-10T09:15:00+03:00",
        "stop_wave_status": "completed",
        "stop_anchor_time": "2026-09-10T09:00:00+03:00",
        "stop_anchor_price": 100.0,
        "stop_anchor_kind": "low",
        "fib_method": "wave3_extension",
        "fib_timeframe": "M15",
        "fib_ratio": 1.618,
        "fib_leg_start_time": "2026-09-10T09:00:00+03:00",
        "fib_leg_start_price": 100.0,
        "fib_leg_start_kind": "low",
        "fib_leg_end_time": "2026-09-10T09:15:00+03:00",
        "fib_leg_end_price": 110.0,
        "fib_leg_end_kind": "high",
        "fib_projection_time": "2026-09-10T09:30:00+03:00",
        "fib_projection_price": 105.0,
        "fib_projection_kind": "low",
        "management_reason": "EN: Confirmed.\nRU: Подтверждено.",
    }


def _review(action="tighten_stop_and_recalculate_target"):
    return {
        "position_ticket": "77",
        "review_trigger": "m15_event",
        "confidence": "high",
        "position_status": "healthy",
        "advisory_action": "hold",
        "data_quality": {"sufficient": True, "issues": "none"},
        "position_management": _management(action),
    }


def _position(stop_loss=95.0, take_profit=120.0):
    return SimpleNamespace(
        ticket=77,
        symbol="XAUUSD",
        type=protection.mt5.POSITION_TYPE_BUY,
        magic=protection.MAGIC_NUMBER,
        sl=stop_loss,
        tp=take_profit,
        price_open=102.0,
    )


def _config():
    return {
        "automatic_trade_changes": True,
        "minimum_confidence": "high",
    }


class PositionProtectionTests(unittest.TestCase):
    def test_wave3_uses_wave1_low_and_fibonacci_target(self):
        prices = protection._build_prices(
            review=_review(),
            snapshot=_snapshot(),
            position=_position(),
            config=_config(),
        )
        self.assertTrue(prices["stop_changed"])
        self.assertGreater(prices["new_sl"], prices["current_sl"])
        self.assertEqual(prices["new_sl"], 99.9)
        self.assertEqual(
            prices["evidence"]["stop"]["buffer_policy"],
            "one_price_step_behind_previous_wave",
        )
        self.assertTrue(prices["target_changed"])
        self.assertAlmostEqual(prices["new_tp"], 121.1, places=1)

    def test_existing_tighter_stop_is_never_widened(self):
        prices = protection._build_prices(
            review=_review("tighten_stop"),
            snapshot=_snapshot(),
            position=_position(stop_loss=101.0),
            config=_config(),
        )
        self.assertFalse(prices["stop_changed"])
        self.assertEqual(prices["new_sl"], 101.0)

    def test_wave5_requires_wave3_reference(self):
        review = _review("tighten_stop")
        review["position_management"]["current_m15_wave"] = "5"
        with self.assertRaises(protection.PositionProtectionError):
            protection._build_prices(
                review=review,
                snapshot=_snapshot(),
                position=_position(),
                config=_config(),
            )
        review["position_management"]["stop_reference_wave"] = "3"
        prices = protection._build_prices(
            review=review,
            snapshot=_snapshot(),
            position=_position(),
            config=_config(),
        )
        self.assertTrue(prices["stop_changed"])

    def test_bearish_wave5_moves_stop_above_wave3_high(self):
        snapshot = _snapshot()
        snapshot["tick"].update({"bid": 98.0, "ask": 98.2})
        review = _review("tighten_stop")
        management = review["position_management"]
        management.update(
            {
                "position_direction": "bearish",
                "current_m15_wave": "5",
                "stop_reference_wave": "3",
                "stop_anchor_time": "2026-09-10T09:15:00+03:00",
                "stop_anchor_price": 110.0,
                "stop_anchor_kind": "high",
            }
        )
        position = _position(stop_loss=130.0)
        position.type = protection.mt5.POSITION_TYPE_SELL
        position.price_open = 120.0
        prices = protection._build_prices(
            review=review,
            snapshot=snapshot,
            position=position,
            config=_config(),
        )
        self.assertTrue(prices["stop_changed"])
        self.assertLess(prices["new_sl"], 130.0)
        self.assertEqual(prices["new_sl"], 110.1)

    def test_invented_anchor_is_rejected(self):
        review = _review("tighten_stop")
        review["position_management"]["stop_anchor_price"] = 99.5
        with self.assertRaises(protection.PositionProtectionError):
            protection._build_prices(
                review=review,
                snapshot=_snapshot(),
                position=_position(),
                config=_config(),
            )

    def test_live_request_is_only_sltp_and_is_reconciled(self):
        position = _position()
        context = {
            "live_positions": [
                {
                    "position_ticket": 77,
                    "stop_loss": 95.0,
                    "take_profit": 120.0,
                    "errors": [],
                }
            ]
        }
        sent = []

        def order_send(request):
            sent.append(copy.deepcopy(request))
            position.sl = request["sl"]
            position.tp = request["tp"]
            return SimpleNamespace(
                retcode=getattr(protection.mt5, "TRADE_RETCODE_DONE", 10009)
            )

        with tempfile.TemporaryDirectory() as directory:
            with (
                patch.object(
                    protection,
                    "STATE_PATH",
                    Path(directory) / "position_protection.json",
                ),
                patch.object(protection, "_current_position", return_value=position),
                patch.object(protection, "_refresh_execution_snapshot", side_effect=lambda snapshot: snapshot),
                patch.object(
                    protection,
                    "inspect_execution_safety_gate",
                    return_value={"order_send_allowed": True},
                ),
                patch.object(
                    protection,
                    "update_managed_position_protection",
                    return_value={"updated": True},
                ) as update_state,
                patch.object(
                    protection.mt5,
                    "order_send",
                    side_effect=order_send,
                    create=True,
                ),
            ):
                result = protection.execute_position_protection(
                    review=_review(),
                    snapshot=_snapshot(),
                    position_context=context,
                    config=_config(),
                    event_key="77|m15|2026-09-10T09:30:00+03:00",
                )

        self.assertTrue(result["ok"])
        self.assertEqual(result["status"], "protection_modified")
        self.assertEqual(len(sent), 1)
        self.assertEqual(sent[0]["action"], protection.mt5.TRADE_ACTION_SLTP)
        self.assertEqual(sent[0]["position"], 77)
        update_state.assert_called_once()


if __name__ == "__main__":
    unittest.main()
