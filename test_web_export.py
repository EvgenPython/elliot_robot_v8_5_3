import ast
import importlib.util
import json
import os
import sys
import tempfile
import types
import unittest
from datetime import timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import web_publisher
import web_market_snapshot
import claude_request_guard
from chart_contract import sanitize_visualization
from single_instance import SingleInstanceError, SingleInstanceLock


BASE_DIR = Path(__file__).resolve().parent


def _load_schema_from_source() -> dict:
    return _load_claude_client_without_sdk().CLAUDE_RESPONSE_SCHEMA


def _assert_closed_required_objects(testcase, schema):
    if isinstance(schema, dict):
        if schema.get("type") == "object":
            properties = schema.get("properties", {})
            testcase.assertFalse(schema.get("additionalProperties", True))
            testcase.assertEqual(set(properties), set(schema.get("required", [])))
        for value in schema.values():
            _assert_closed_required_objects(testcase, value)
    elif isinstance(schema, list):
        for value in schema:
            _assert_closed_required_objects(testcase, value)


def _load_claude_client_without_sdk():
    anthropic_stub = types.ModuleType("anthropic")

    class DummyAnthropic:
        pass

    class DummyAnthropicError(Exception):
        pass

    anthropic_stub.Anthropic = DummyAnthropic
    for name in (
        "AuthenticationError",
        "PermissionDeniedError",
        "RateLimitError",
        "APITimeoutError",
        "APIConnectionError",
        "APIStatusError",
    ):
        setattr(anthropic_stub, name, type(name, (DummyAnthropicError,), {}))

    spec = importlib.util.spec_from_file_location(
        "claude_client_transport_test",
        BASE_DIR / "claude_client.py",
    )
    module = importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules, {"anthropic": anthropic_stub}):
        spec.loader.exec_module(module)
    return module


class StructuredOutputSchemaTests(unittest.TestCase):
    def test_visualization_schema_is_closed_and_required(self):
        schema = _load_schema_from_source()
        visualization = schema["properties"]["visualization"]

        self.assertIn("visualization", schema["required"])
        self.assertEqual(
            set(visualization["properties"]),
            {
                "wave_points",
                "levels",
                "zones",
                "scenario_paths",
                "trendlines",
                "channels",
                "pattern_shapes",
                "market_events",
                "projected_waves",
                "wave_structures",
                "chart_comment",
            },
        )
        _assert_closed_required_objects(self, schema)


class ClaudeTransportTests(unittest.TestCase):
    def test_columnar_transport_preserves_every_bar_and_reduces_json(self):
        module = _load_claude_client_without_sdk()
        bars = [
            {
                "time": f"2026-08-17T{index % 24:02d}:00:00+03:00",
                "open": 4300.0 + index,
                "high": 4301.0 + index,
                "low": 4299.0 + index,
                "close": 4300.5 + index,
                "tick_volume": 1000 + index,
                "spread_points": 32,
                "real_volume": 0,
            }
            for index in range(100)
        ]
        payload = {
            "instrument": "XAUUSD",
            "timestamp": "2026-08-17T12:00:00+03:00",
            "timezone": "FundingPips Platform Time UTC+3",
            "cache_partition": {"base_cutoff_fp": "2026-08-17T00:00:00+03:00"},
            "cacheable_history": {
                "closed_market_history_before_day_start": {
                    "H1": {"closed_bars_count": len(bars), "closed_bars": bars}
                }
            },
            "live_market": {"raw_timeframes_since_day_start": {}},
        }

        transport = module.build_transport_payload(payload)
        table = transport["cacheable_history"][
            "closed_market_history_before_day_start"
        ]["H1"]["closed_bars"]
        decoded = [dict(zip(table["columns"], row)) for row in table["rows"]]

        original_size = len(module._compact_json(payload).encode("utf-8"))
        transport_size = len(module._compact_json(transport).encode("utf-8"))

        self.assertEqual(decoded, bars)
        self.assertLess(transport_size, original_size * 0.7)

    def test_midstream_overloaded_error_is_retryable_even_after_http_200(self):
        module = _load_claude_client_without_sdk()

        class MidstreamError(Exception):
            status_code = 200
            request_id = "req_midstream"

        translated = module._translate_api_status_error(
            MidstreamError("{'type': 'overloaded_error'}"),
            "stream",
        )

        self.assertTrue(translated.retryable)
        self.assertEqual(translated.request_id, "req_midstream")


class ChartContractTests(unittest.TestCase):
    def test_trade_levels_are_copied_without_changing_recommendation(self):
        payload = {
            "cacheable_history": {
                "symbol_specification": {"point": 0.01},
                "closed_market_history_before_day_start": {
                    "H1": {
                        "closed_bars": [
                            {
                                "time": "2026-08-16T10:00:00+03:00",
                                "open": 3350.0,
                                "high": 3360.0,
                                "low": 3340.0,
                                "close": 3355.0,
                            }
                        ]
                    }
                },
            }
        }
        recommendation = {
            "action": "enter_long",
            "entry_price": 3355.0,
            "stop_loss": 3340.0,
            "take_profit": 3385.0,
        }
        analysis = {
            "recommendation": dict(recommendation),
            "visualization": {
                "wave_points": [
                    {
                        "scenario": "primary",
                        "degree": "minor",
                        "timeframe": "H1",
                        "sequence": 1,
                        "label": "1",
                        "time": "2026-08-16T10:00:00+03:00",
                        "price": 3360.0,
                        "status": "confirmed",
                    },
                    {
                        "scenario": "primary",
                        "degree": "minor",
                        "timeframe": "H1",
                        "sequence": 2,
                        "label": "2",
                        "time": "invented-time",
                        "price": 9999.0,
                        "status": "confirmed",
                    },
                ],
                "levels": [],
                "zones": [],
                "scenario_paths": [],
                "chart_comment": "",
            },
        }

        warnings = sanitize_visualization(analysis, payload)

        self.assertEqual(analysis["recommendation"], recommendation)
        self.assertEqual(len(analysis["visualization"]["wave_points"]), 1)
        levels = {
            item["kind"]: item["price"]
            for item in analysis["visualization"]["levels"]
        }
        self.assertEqual(
            levels,
            {"entry": 3355.0, "stop_loss": 3340.0, "take_profit": 3385.0},
        )
        self.assertTrue(warnings)


class PublisherTests(unittest.TestCase):
    def test_second_publisher_lock_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary_dir:
            lock_path = Path(temporary_dir) / "publisher.lock"
            with SingleInstanceLock(lock_path):
                with self.assertRaises(SingleInstanceError):
                    with SingleInstanceLock(lock_path):
                        pass

    def test_fallback_event_id_is_stable(self):
        config = {"engine_id": "xauusd-windows-01"}
        first = web_publisher.build_analysis_envelope(
            config,
            {"revision": 1},
            "analysis_archive/2026-08-16/a.json",
            "abc",
        )
        second = web_publisher.build_analysis_envelope(
            config,
            {"revision": 1},
            "analysis_archive/2026-08-16/a.json",
            "abc",
        )
        self.assertEqual(first["event_id"], second["event_id"])

    def test_enabled_config_requires_https_and_token(self):
        base = {
            "enabled": True,
            "base_url": "https://dashboard.example.com",
            "engine_id": "xauusd-windows-01",
        }

        with tempfile.TemporaryDirectory() as temporary_dir:
            path = Path(temporary_dir) / "web_export.json"
            path.write_text(json.dumps(base), encoding="utf-8")

            with patch.dict(os.environ, {"ROBOT_WEB_API_TOKEN": "test-token"}):
                config = web_publisher.load_config(path)
            self.assertNotIn("test-token", json.dumps(base))
            self.assertEqual(config["resolved_api_token"], "test-token")

            base["base_url"] = "http://dashboard.example.com"
            path.write_text(json.dumps(base), encoding="utf-8")
            with patch.dict(os.environ, {"ROBOT_WEB_API_TOKEN": "test-token"}):
                with self.assertRaises(web_publisher.PublisherError):
                    web_publisher.load_config(path)

    def test_market_envelope_has_stable_identity_and_exact_payload(self):
        config = {"engine_id": "xauusd-windows-01"}
        payload = {
            "instrument": "XAUUSD",
            "timestamp": "2026-08-17T12:00:00+03:00",
            "cacheable_history": {},
            "live_market": {},
        }
        first = web_publisher.build_market_envelope(config, payload, "a" * 64)
        second = web_publisher.build_market_envelope(config, payload, "a" * 64)

        self.assertEqual(first["snapshot_id"], second["snapshot_id"])
        self.assertIs(first["payload"], payload)
        self.assertEqual(
            first["source_snapshot_at_fp"],
            payload["timestamp"],
        )


class WebMarketSnapshotTests(unittest.TestCase):
    def test_snapshot_reuses_collected_data_and_contains_failures(self):
        payload = {
            "instrument": "XAUUSD",
            "timestamp": "2026-08-17T12:00:00+03:00",
            "cacheable_history": {},
            "live_market": {},
        }
        with tempfile.TemporaryDirectory() as temporary_dir:
            path = Path(temporary_dir) / "state" / "web_market_snapshot.json"
            with patch.object(
                web_market_snapshot,
                "build_claude_payload",
                return_value=payload,
            ) as builder:
                saved = web_market_snapshot.save_web_market_snapshot(
                    {"already_collected": True},
                    path=path,
                )

            self.assertTrue(saved)
            builder.assert_called_once_with({"already_collected": True})
            self.assertEqual(
                json.loads(path.read_text(encoding="utf-8")),
                payload,
            )

        with patch.object(
            web_market_snapshot,
            "build_claude_payload",
            side_effect=RuntimeError("telemetry failed"),
        ):
            self.assertFalse(
                web_market_snapshot.save_web_market_snapshot(
                    {"already_collected": True}
                )
            )


class ArchiveTests(unittest.TestCase):
    def test_archive_has_stable_ids_and_revisioned_updates(self):
        prop_time_stub = types.ModuleType("prop_time")
        prop_time_stub.FUNDINGPIPS_TZ = timezone(timedelta(hours=3))

        trade_state_stub = types.ModuleType("trade_state")
        trade_state_stub.extract_latest_closed_h1_time = (
            lambda snapshot: snapshot.get("test_h1_time")
        )

        module_path = BASE_DIR / "analysis_archive.py"
        spec = importlib.util.spec_from_file_location(
            "analysis_archive_test_instance",
            module_path,
        )
        module = importlib.util.module_from_spec(spec)

        with patch.dict(
            sys.modules,
            {
                "prop_time": prop_time_stub,
                "trade_state": trade_state_stub,
            },
        ):
            spec.loader.exec_module(module)

        snapshot = {
            "instrument": "XAUUSD",
            "generated_at_fp": "2026-08-16T12:00:00+03:00",
            "test_h1_time": "2026-08-16T11:00:00+03:00",
        }

        with tempfile.TemporaryDirectory() as temporary_dir:
            module.ARCHIVE_DIR = Path(temporary_dir)
            path = module.save_analysis_archive(
                snapshot=snapshot,
                cycle_type="FULL_SCHEDULED",
                payload={"raw": True},
                result={"recommendation": {"action": "stay_out"}},
            )
            first = json.loads(path.read_text(encoding="utf-8"))

            module.safe_update_analysis_archive(
                path,
                risk_report={"decision": "NO_TRADE"},
                execution_report={"decision": "NO_ACTIVE_PLAN"},
            )
            second = json.loads(path.read_text(encoding="utf-8"))

        self.assertEqual(first["event_id"], second["event_id"])
        self.assertEqual(first["cycle_id"], second["cycle_id"])
        self.assertEqual(first["revision"] + 1, second["revision"])
        self.assertEqual(second["risk_report"]["decision"], "NO_TRADE")
        self.assertEqual(second["execution_report"]["decision"], "NO_ACTIVE_PLAN")

    def test_recovery_archive_keeps_payload_without_claude_result(self):
        prop_time_stub = types.ModuleType("prop_time")
        prop_time_stub.FUNDINGPIPS_TZ = timezone(timedelta(hours=3))

        trade_state_stub = types.ModuleType("trade_state")
        trade_state_stub.extract_latest_closed_h1_time = lambda snapshot: None

        module_path = BASE_DIR / "analysis_archive.py"
        spec = importlib.util.spec_from_file_location(
            "analysis_archive_recovery_test_instance",
            module_path,
        )
        module = importlib.util.module_from_spec(spec)

        with patch.dict(
            sys.modules,
            {
                "prop_time": prop_time_stub,
                "trade_state": trade_state_stub,
            },
        ):
            spec.loader.exec_module(module)

        payload = {
            "instrument": "XAUUSD",
            "timestamp": "2026-08-17T10:15:14+03:00",
            "live_market": {
                "raw_timeframes_since_day_start": {
                    "H1": {
                        "closed_bars_since_day_start": [
                            {
                                "time": "2026-08-17T09:00:00+03:00",
                                "open": 1,
                                "high": 2,
                                "low": 0.5,
                                "close": 1.5,
                            }
                        ]
                    }
                }
            },
        }

        with tempfile.TemporaryDirectory() as temporary_dir:
            module.ARCHIVE_DIR = Path(temporary_dir)
            path = module.save_payload_recovery_archive(payload)
            record = json.loads(path.read_text(encoding="utf-8"))

        self.assertEqual(record["cycle_type"], "FULL_RECOVERY")
        self.assertEqual(record["payload"], payload)
        self.assertIsNone(record["result"])
        self.assertEqual(
            record["api_attempt"]["retry_policy"],
            "NOT_APPLICABLE_RECOVERY_ONLY",
        )


class ClaudeRequestGuardTests(unittest.TestCase):
    def test_retryable_failure_allows_next_attempt_then_exhausts(self):
        with tempfile.TemporaryDirectory() as temporary_dir:
            with patch.object(
                claude_request_guard,
                "STATE_PATH",
                Path(temporary_dir) / "guard.json",
            ):
                first = claude_request_guard.begin_api_attempt(
                    "2026-08-17T09:00:00+03:00",
                    "FULL",
                    "FULL_SCHEDULED",
                    max_attempts=2,
                )
                failed = claude_request_guard.mark_api_attempt(
                    "2026-08-17T09:00:00+03:00",
                    "FULL",
                    "FAILED_RETRYABLE",
                    attempt_id=first["attempt"]["attempt_id"],
                    error=RuntimeError("stream lost"),
                    outcome_unknown=True,
                )
                second = claude_request_guard.begin_api_attempt(
                    "2026-08-17T09:00:00+03:00",
                    "FULL",
                    "FULL_SCHEDULED",
                    max_attempts=2,
                )
                exhausted_attempt = claude_request_guard.mark_api_attempt(
                    "2026-08-17T09:00:00+03:00",
                    "FULL",
                    "FAILED_RETRYABLE",
                    attempt_id=second["attempt"]["attempt_id"],
                    error=RuntimeError("stream lost again"),
                    outcome_unknown=True,
                )
                blocked = claude_request_guard.begin_api_attempt(
                    "2026-08-17T09:00:00+03:00",
                    "FULL",
                    "FULL_SCHEDULED",
                    max_attempts=2,
                )

        self.assertTrue(first["allowed"])
        self.assertEqual(failed["cycle_status"], "WAITING_RETRY")
        self.assertTrue(second["allowed"])
        self.assertEqual(second["attempt"]["attempt_number"], 2)
        self.assertEqual(exhausted_attempt["cycle_status"], "EXHAUSTED")
        self.assertFalse(blocked["allowed"])
        self.assertEqual(blocked["reason"], "MAX_ATTEMPTS_EXHAUSTED")

    def test_validated_winner_blocks_duplicate_response(self):
        with tempfile.TemporaryDirectory() as temporary_dir:
            with patch.object(
                claude_request_guard,
                "STATE_PATH",
                Path(temporary_dir) / "guard.json",
            ):
                first = claude_request_guard.begin_api_attempt(
                    "2026-08-17T10:00:00+03:00",
                    "FULL",
                    "FULL_SCHEDULED",
                    max_attempts=3,
                )
                completed = claude_request_guard.mark_api_attempt(
                    "2026-08-17T10:00:00+03:00",
                    "FULL",
                    "VALIDATED",
                    attempt_id=first["attempt"]["attempt_id"],
                    request_id="req_123",
                )
                duplicate = claude_request_guard.begin_api_attempt(
                    "2026-08-17T10:00:00+03:00",
                    "FULL",
                    "FULL_SCHEDULED",
                    max_attempts=3,
                )

        self.assertEqual(completed["cycle_status"], "VALIDATED")
        self.assertFalse(duplicate["allowed"])
        self.assertEqual(
            duplicate["reason"],
            "VALIDATED_RESPONSE_ALREADY_EXISTS",
        )

    def test_retry_rejects_changed_payload_hash_before_dispatch(self):
        with tempfile.TemporaryDirectory() as temporary_dir:
            with patch.object(
                claude_request_guard,
                "STATE_PATH",
                Path(temporary_dir) / "guard.json",
            ):
                first = claude_request_guard.begin_api_attempt(
                    "2026-08-17T11:00:00+03:00",
                    "FULL",
                    "FULL_SCHEDULED",
                    max_attempts=3,
                )
                claude_request_guard.update_api_attempt(
                    "2026-08-17T11:00:00+03:00",
                    "FULL",
                    first["attempt"]["attempt_id"],
                    input_tokens=50000,
                    payload_sha256="a" * 64,
                )
                claude_request_guard.mark_api_attempt(
                    "2026-08-17T11:00:00+03:00",
                    "FULL",
                    "FAILED_RETRYABLE",
                    attempt_id=first["attempt"]["attempt_id"],
                    error=RuntimeError("stream lost"),
                )
                second = claude_request_guard.begin_api_attempt(
                    "2026-08-17T11:00:00+03:00",
                    "FULL",
                    "FULL_SCHEDULED",
                    max_attempts=3,
                )

                with self.assertRaisesRegex(RuntimeError, "payload_sha256"):
                    claude_request_guard.update_api_attempt(
                        "2026-08-17T11:00:00+03:00",
                        "FULL",
                        second["attempt"]["attempt_id"],
                        input_tokens=50000,
                        payload_sha256="b" * 64,
                    )


if __name__ == "__main__":
    unittest.main()
