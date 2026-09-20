import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pandas as pd

import decision_clock


FP = timezone(timedelta(hours=3))


class M30DecisionClockTests(unittest.TestCase):
    def _bars(self, opened: datetime):
        return pd.DataFrame([{"time_fp": opened}])

    def test_only_intermediate_half_hour_is_due_and_completion_is_durable(self):
        current = datetime(2026, 9, 17, 10, 30, 5, tzinfo=FP)
        closed_open = datetime(2026, 9, 17, 10, 0, 0, tzinfo=FP)
        gate = {"analysis_window_allowed": True, "tick_fresh": True}

        with tempfile.TemporaryDirectory() as temporary:
            state_path = Path(temporary) / "m30.json"
            with (
                patch.object(decision_clock, "STATE_PATH", state_path),
                patch.object(decision_clock, "now_fp", return_value=current),
                patch.object(
                    decision_clock,
                    "get_closed_bars",
                    return_value=self._bars(closed_open),
                ),
            ):
                due = decision_clock.inspect_m30_decision_due(
                    symbol="XAUUSD", market_gate=gate
                )
                self.assertTrue(due["due"])
                decision_clock.mark_m30_decision_attempt(
                    due["m30_open_time_fp"], status="COMPLETED"
                )
                repeated = decision_clock.inspect_m30_decision_due(
                    symbol="XAUUSD", market_gate=gate
                )

        self.assertFalse(repeated["due"])
        self.assertTrue(repeated["already_completed"])

    def test_hour_close_is_owned_by_h1_cycle(self):
        current = datetime(2026, 9, 17, 11, 0, 5, tzinfo=FP)
        closed_open = datetime(2026, 9, 17, 10, 30, 0, tzinfo=FP)
        gate = {"analysis_window_allowed": True, "tick_fresh": True}

        with tempfile.TemporaryDirectory() as temporary, patch.object(
            decision_clock, "STATE_PATH", Path(temporary) / "m30.json"
        ), patch.object(
            decision_clock, "now_fp", return_value=current
        ), patch.object(
            decision_clock,
            "get_closed_bars",
            return_value=self._bars(closed_open),
        ):
            result = decision_clock.inspect_m30_decision_due(
                symbol="XAUUSD", market_gate=gate
            )

        self.assertFalse(result["due"])
        self.assertFalse(result["intermediate_half_hour"])


if __name__ == "__main__":
    unittest.main()
