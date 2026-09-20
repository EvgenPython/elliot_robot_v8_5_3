import tempfile
import unittest
import json
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

import entry_watch
import entry_check_cycle


class EntryWatchTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.state_dir = Path(self.temporary.name)
        self.path_patch = patch.object(
            entry_watch, "ENTRY_WATCH_PATH", self.state_dir / "entry_watch.json"
        )
        self.dir_patch = patch.object(entry_watch, "STATE_DIR", self.state_dir)
        self.path_patch.start()
        self.dir_patch.start()

    def tearDown(self):
        self.path_patch.stop()
        self.dir_patch.stop()
        self.temporary.cleanup()

    def test_refresh_prefers_m5_primary_projection(self):
        analysis = {
            "timestamp": "2026-08-30T08:00:00+03:00",
            "visualization": {
                "projected_waves": [
                    {
                        "projection_id": "m15",
                        "scenario": "primary",
                        "timeframe": "M15",
                        "direction": "down",
                        "confirmation_level": 4500.0,
                    },
                    {
                        "projection_id": "m5",
                        "scenario": "primary",
                        "timeframe": "M5",
                        "direction": "down",
                        "confirmation_level": 4490.0,
                    },
                ]
            },
        }
        state = entry_watch.refresh_entry_watch(analysis, "2026-08-30T07:00:00+03:00")
        self.assertEqual(state["status"], "watching")
        self.assertEqual(state["projection"]["projection_id"], "m5")

    def test_closed_bar_confirmation_triggers_once(self):
        entry_watch._atomic_write(
            {
                "status": "watching",
                "projection": {
                    "projection_id": "p1",
                    "timeframe": "M5",
                    "direction": "down",
                    "confirmation_level": 4490.0,
                    "invalidation_level": 4520.0,
                },
                "last_checked_closed_bar_time": None,
            }
        )
        bar = {
            "time": "2026-08-30T08:05:00+03:00",
            "open": 4501.0,
            "high": 4502.0,
            "low": 4487.0,
            "close": 4489.0,
        }
        with patch.object(entry_watch, "_latest_closed_bar", return_value=bar):
            result = entry_watch.inspect_entry_trigger()
        self.assertTrue(result["triggered"])
        self.assertEqual(entry_watch.load_entry_watch()["status"], "triggered")

    def test_invalidation_wins_over_confirmation(self):
        entry_watch._atomic_write(
            {
                "status": "watching",
                "projection": {
                    "projection_id": "p2",
                    "timeframe": "M15",
                    "direction": "up",
                    "confirmation_level": 4520.0,
                    "invalidation_level": 4490.0,
                },
                "last_checked_closed_bar_time": None,
            }
        )
        bar = {
            "time": "2026-08-30T08:15:00+03:00",
            "open": 4500.0,
            "high": 4502.0,
            "low": 4480.0,
            "close": 4485.0,
        }
        with patch.object(entry_watch, "_latest_closed_bar", return_value=bar):
            result = entry_watch.inspect_entry_trigger()
        self.assertFalse(result["triggered"])
        self.assertTrue(result["invalidated"])
        saved = entry_watch.load_entry_watch()
        self.assertEqual(saved["status"], "invalidated")
        self.assertEqual(saved["last_checked_bar"]["close"], 4485.0)
        self.assertEqual(
            saved["terminal_event"]["kind"], "entry_projection_invalidated"
        )

    def test_transient_entry_check_failure_keeps_trigger_for_retry(self):
        entry_watch._atomic_write(
            {
                "status": "triggered",
                "projection": {
                    "projection_id": "p3",
                    "timeframe": "M5",
                    "direction": "up",
                    "confirmation_level": 4500.0,
                },
                "triggered_closed_bar_time": "2026-08-30T08:05:00+03:00",
                "entry_check_attempts": 0,
            }
        )
        retry = entry_watch.mark_entry_check_result(
            "temporary_api_failure",
            retryable=True,
            retry_after_seconds=300,
            max_attempts=2,
        )
        self.assertEqual(retry["status"], "retry_pending")
        self.assertEqual(retry["entry_check_attempts"], 1)

        with patch.object(
            entry_watch,
            "now_fp",
            return_value=entry_watch.now_fp() + timedelta(seconds=301),
        ), patch.object(
            entry_watch,
            "_latest_closed_bar",
            return_value={
                "time": "2026-08-30T08:05:00+03:00",
                "open": 4495.0,
                "high": 4505.0,
                "low": 4490.0,
                "close": 4501.0,
            },
        ):
            due = entry_watch.inspect_entry_trigger()
        self.assertTrue(due["triggered"])

    def test_entry_check_retry_is_bounded(self):
        entry_watch._atomic_write(
            {
                "status": "triggered",
                "projection": {"projection_id": "p4"},
                "entry_check_attempts": 1,
            }
        )
        final = entry_watch.mark_entry_check_result(
            "second_failure",
            retryable=True,
            max_attempts=2,
        )
        self.assertEqual(final["status"], "failed")
        self.assertIsNone(final["next_retry_at_fp"])

    def test_entry_check_api_key_survives_crossing_into_next_h1(self):
        snapshot = {
            "timeframes": {
                "H1": {
                    "closed_bars": [
                        {"time_fp": "2026-09-17T11:00:00+03:00"}
                    ]
                }
            }
        }
        with patch.object(
            entry_check_cycle,
            "load_entry_watch",
            return_value={
                "source_h1_closed_bar_time": "2026-09-17T10:00:00+03:00"
            },
        ):
            key = entry_check_cycle._entry_check_guard_h1(snapshot)
        self.assertEqual(key, "2026-09-17T10:00:00+03:00")

    def test_paid_entry_response_is_recovered_before_any_second_api_call(self):
        archive = self.state_dir / "entry_archive.json"
        archive.write_text(
            json.dumps(
                {
                    "payload": {"timestamp": "2026-09-17T10:05:00+03:00"},
                    "trade_decision_result": {"recommendation": {"action": "enter_long"}},
                    "trade_decision_usage": {"input_tokens": 100, "output_tokens": 20},
                }
            ),
            encoding="utf-8",
        )
        recovered = entry_check_cycle._recover_guarded_entry_response(
            {"cycle": {"archive_path": str(archive)}}
        )
        self.assertIsNotNone(recovered)
        self.assertEqual(
            recovered["decision"]["recommendation"]["action"],
            "enter_long",
        )
        self.assertEqual(recovered["archive_path"], str(archive))


if __name__ == "__main__":
    unittest.main()
