"""Regression tests: no paid API requests or actual MT5 orders."""
import copy
import json
import os
import tempfile
import unittest
import zipfile
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import test_staged_analysis as fixtures
import test_position_protection as pf
import claude_staged_client as staged
import position_monitor as monitor
import position_protection as protection
import runner
import live_executor as executor
import pending_executor as pending
import observability as audit
import entry_check_cycle
import main as robot_main
import analysis_schedule
from api_costs import estimate_cost
from export_diagnostics import export as export_diagnostics
from review_report import build_report
from response_contract import contract_errors, to_wire, plan_repair, merge_repair


class NamedResponseTests(unittest.TestCase):
    def data(self):
        payload = fixtures._payload()
        good = fixtures._market_map(payload, {"mode": "initialize", "preserved_anchor_ids": [],
                    "invalidated_anchor_ids": [], "reason": "Initial map."})
        return payload, good

    def test_named_map_and_decision_roundtrip(self):
        payload, good = self.data()
        wire = to_wire(good, staged.MARKET_MAP_WIRE_SCHEMA)
        self.assertEqual(contract_errors(wire, staged.MARKET_MAP_WIRE_SCHEMA), [])
        self.assertEqual(staged._expand_market_map_wire_result(wire), good)
        decision = fixtures._decision(payload)
        self.assertEqual(staged._expand_trade_decision_wire_result(
            to_wire(decision, staged.TRADE_DECISION_WIRE_SCHEMA)), decision)

    def test_september_missing_cells_only_repair_broken_section(self):
        payload, good = self.data()
        for field in ("price_structure", "wave_count", "higher_timeframe_context"):
            with self.subTest(field=field):
                broken = fixtures._wire_market_map(good)
                broken[field].pop()
                original, schema = plan_repair(broken, staged.MARKET_MAP_WIRE_SCHEMA, field + ": missing cell")
                self.assertEqual(schema["required"], [field])
                patch_value = {field: to_wire(good[field], staged.MARKET_MAP_WIRE_SCHEMA["properties"][field])}
                with patch.object(staged, "_request_structured_stage", return_value=patch_value) as request:
                    result = staged.repair_market_map(payload, broken, field + ": missing cell")
                self.assertEqual(result, good)
                self.assertEqual(request.call_args.kwargs["schema"]["required"], [field])

    def test_h1_refresh_has_its_own_entry_prompt_and_named_contract(self):
        payload = fixtures._payload()
        decision = fixtures._decision(payload)
        market_map = fixtures._market_map(payload, {
            "mode": "initialize", "preserved_anchor_ids": [],
            "invalidated_anchor_ids": [], "reason": "Initial map."
        })
        with patch.object(
            staged, "_request_structured_stage",
            return_value=to_wire(decision, staged.TRADE_DECISION_WIRE_SCHEMA),
        ) as request:
            actual = staged.analyze_h1_decision_refresh(payload, market_map)
        self.assertEqual(actual, decision)
        self.assertEqual(request.call_args.kwargs["stage"], "H1_DECISION")
        self.assertIn("отсутствие старого conditional trigger не запрещает", request.call_args.kwargs["system_prompt"])

    def test_repair_cannot_change_healthy_sections_or_omit_required_field(self):
        _, good = self.data()
        wire = to_wire(good, staged.MARKET_MAP_WIRE_SCHEMA)
        wire["wave_count"].pop("summary")
        original, schema = plan_repair(wire, staged.MARKET_MAP_WIRE_SCHEMA, "wave_count")
        with self.assertRaises(ValueError):
            merge_repair(original, {"wave_count": wire["wave_count"]}, schema, staged.MARKET_MAP_WIRE_SCHEMA)
        with self.assertRaises(ValueError):
            merge_repair(original, {"wave_count": good["wave_count"], "patterns": "changed"}, schema, staged.MARKET_MAP_WIRE_SCHEMA)


class MonitorContinuityTests(unittest.TestCase):
    def state(self):
        value = monitor._empty_state()
        value.update(position_key="77", last_processed_h1="2026-09-10T09:00:00+03:00",
                     last_processed_m15="2026-09-10T09:30:00+03:00")
        return value

    def test_m15_scout_and_new_h1_deep_review_are_due(self):
        for h1, action in [("2026-09-10T09:00:00+03:00", "m15_scout"),
                           ("2026-09-10T10:00:00+03:00", "deep_review")]:
            with patch.object(monitor, "load_position_monitor_state", return_value=self.state()), \
                 patch.object(monitor, "_latest_closed_bar_time", side_effect=lambda symbol, tf: h1 if tf == "H1" else "2026-09-10T09:45:00+03:00"):
                result = monitor.inspect_position_monitor_due("XAUUSD", [{"position_ticket": 77}])
            self.assertTrue(result["due"])
            self.assertEqual(result["action"], action)

    def test_failed_escalation_does_not_consume_m15(self):
        with tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
            stack.enter_context(patch.object(monitor, "STATE_PATH", Path(directory) / "monitor.json"))
            monitor._save_state(self.state())
            for name, value in {
                "inspect_position_gate": {"live_managed": [{"position_ticket": 77}]},
                "get_market_snapshot": {}, "_position_context": {}, "load_reference_state": {},
                "_build_position_scout_payload": {}, "build_claude_payload": {},
                "save_analysis_archive": Path(directory) / "archive.json", "safe_update_analysis_archive": True,
            }.items():
                stack.enter_context(patch.object(monitor, name, return_value=value))
            stack.enter_context(patch.object(monitor, "_run_api_with_retries", side_effect=[
                {"ok": True, "result": {"full_analysis_required": True}},
                {"ok": False, "error": "temporary review failure"}]))
            due = {"due": True, "action": "m15_scout", "trigger": "m15_close", "position_key": "77",
                   "event_time": "2026-09-10T09:45:00+03:00", "m15_time": "2026-09-10T09:45:00+03:00",
                   "h1_time": "2026-09-10T09:00:00+03:00", "attempt_key": "77|m15|0945", "config": monitor.DEFAULT_CONFIG}
            result = monitor.run_position_monitor("XAUUSD", due)
            self.assertFalse(result["ok"])
            self.assertEqual(monitor.load_position_monitor_state()["last_processed_m15"], self.state()["last_processed_m15"])
            self.assertFalse(monitor.load_position_monitor_state()["last_attempt_ok"])

    def test_technical_audit_error_does_not_skip_position_analysis(self):
        with ExitStack() as stack:
            for name, value in {"connect_mt5": True, "_refresh_daily_state": {"fp_day": "2026-09-10"},
                                "_print_runner_header": None, "_print_daily_rollover": None, "write_runner_status": None,
                                "get_managed_positions": [{"position_ticket": 77}],
                                "inspect_market_runtime_gate": {"tick_fresh": True},
                                "inspect_position_monitor_due": {"due": True}}.items():
                stack.enter_context(patch.object(runner, name, return_value=value))
            stack.enter_context(patch.object(runner, "_run_full_cycle", side_effect=RuntimeError("audit unavailable")))
            run_monitor = stack.enter_context(patch.object(runner, "run_position_monitor"))
            stack.enter_context(patch.object(runner.time, "sleep", side_effect=KeyboardInterrupt))
            with self.assertRaises(KeyboardInterrupt):
                runner.run_forever()
            run_monitor.assert_called_once()


class HourlyDecisionRefreshTests(unittest.TestCase):
    def test_refresh_replaces_stale_m15_projection_but_keeps_h4_map(self):
        payload = fixtures._payload()
        market_map = fixtures._market_map(payload, {
            "mode": "initialize", "preserved_anchor_ids": [],
            "invalidated_anchor_ids": [], "reason": "Initial map."
        })
        reference_decision = fixtures._decision(payload)
        reference = staged.assemble_staged_analysis(
            payload=payload, market_map=market_map,
            trade_decision=reference_decision, previous_reference=None,
        )
        def projection(identifier, timeframe):
            bar = payload["live_market"]["raw_timeframes_since_day_start"][timeframe]["closed_bars_since_day_start"][-1]
            return {
                "projection_id": identifier, "structure_id": "s1",
                "parent_structure_id": "", "parent_wave_id": "",
                "scenario": "primary", "degree": "minor",
                "timeframe": timeframe, "label": "next", "wave_type": "impulse",
                "direction": "up", "anchor_time": bar["time"],
                "anchor_price": bar["close"], "target_price_low": bar["close"] + 2,
                "target_price_high": bar["close"] + 4, "confirmation_level": bar["close"] + 1,
                "invalidation_level": bar["close"] - 1, "status": "developing",
                "basis": "EN: Test.\nRU: Тест.",
            }
        reference["visualization"]["projected_waves"] = [
            projection("old-m15", "M15"), projection("keep-h4", "H4")
        ]
        fresh = fixtures._decision(payload)
        fresh["visualization"]["projected_waves"] = [
            projection("fresh-m15", "M15")
        ]
        actual = entry_check_cycle.assemble_entry_refresh_analysis(
            reference_analysis=reference, decision=fresh, payload=payload
        )
        identifiers = {
            item.get("projection_id")
            for item in actual["visualization"]["projected_waves"]
        }
        self.assertEqual(identifiers, {"keep-h4", "fresh-m15"})

    def test_non_full_h1_still_reaches_decision_risk_and_executor(self):
        payload = fixtures._payload()
        decision = fixtures._decision(payload)
        analysis = {"recommendation": decision["recommendation"]}
        previous = {"analysis": {"instrument": "XAUUSD"}}
        snapshot = {"generated_at_fp": payload["timestamp"]}
        with tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
            archive = Path(directory) / "refresh.json"
            stack.enter_context(patch.object(robot_main, "_load_h1_decision_archive", return_value=None))
            stack.enter_context(patch.object(robot_main, "build_claude_payload", return_value=payload))
            stack.enter_context(patch.object(robot_main, "save_analysis_archive", return_value=archive))
            stack.enter_context(patch.object(robot_main, "run_trade_decision_pipeline", return_value={"ok": True, "result": decision, "usage": {}}))
            stack.enter_context(patch.object(robot_main, "_run_api_with_retries", return_value={
                "ok": True, "result": decision, "usage": {}
            }))
            stack.enter_context(patch.object(robot_main, "assemble_entry_refresh_analysis", return_value=analysis))
            stack.enter_context(patch.object(robot_main, "save_debug_response"))
            stack.enter_context(patch.object(robot_main, "print_analysis_summary"))
            stack.enter_context(patch.object(robot_main, "safe_update_analysis_archive"))
            stack.enter_context(patch.object(robot_main, "refresh_entry_watch", return_value={"status": "inactive"}))
            stack.enter_context(patch.object(robot_main, "evaluate_trade", return_value={
                "decision": "NO_TRADE", "approved": False, "reasons": ["stay_out"]
            }))
            stack.enter_context(patch.object(robot_main, "print_risk_report"))
            stack.enter_context(patch.object(robot_main, "register_trade_decision", return_value={
                "state_action": "NO_TRADE", "active_plan": None
            }))
            stack.enter_context(patch.object(robot_main, "print_trade_state_result"))
            register = stack.enter_context(patch.object(robot_main, "register_completed_analysis", return_value={
                "h1_closed_bar_time": "2026-09-16T09:00:00+03:00"
            }))
            stack.enter_context(patch.object(robot_main, "print_analysis_registration"))
            execute = stack.enter_context(patch.object(robot_main, "execute_active_plan", return_value={
                "decision": "NO_ACTIVE_PLAN", "order_send_called": False
            }))
            stack.enter_context(patch.object(robot_main, "print_live_execution_report"))
            stack.enter_context(patch.object(robot_main, "extract_latest_closed_h1_time", return_value="2026-09-16T09:00:00+03:00"))
            stack.enter_context(patch.object(robot_main, "emit"))
            robot_main._run_h1_decision_refresh(
                snapshot=snapshot, previous_reference=previous,
                scout_result={"full_analysis_required": False},
                scout_archive=Path(directory) / "scout.json",
                execution_observation_only=False,
                execution_block_reasons=[],
            )
        execute.assert_called_once()
        self.assertEqual(
            register.call_args.kwargs["cycle_type"],
            analysis_schedule.CYCLE_H1_DECISION_REFRESH,
        )


class ProtectionEvidenceTests(unittest.TestCase):
    def test_weakened_thesis_can_still_tighten_verified_stop(self):
        review = pf._review("tighten_stop")
        review["position_status"] = "weakened"
        prices = protection._build_prices(review=review, snapshot=pf._snapshot(), position=pf._position(), config=pf._config())
        self.assertTrue(prices["stop_changed"])

    def test_incomplete_wave_or_wrong_range_extreme_is_rejected(self):
        for update in ({"stop_wave_status": "unclear"},
                       {"stop_anchor_time": "2026-09-10T09:15:00+03:00", "stop_anchor_price": 102.0},
                       {"stop_wave_end_time": "2026-09-10T12:00:00+03:00"},
                       {"stop_anchor_price": float("nan")}):
            review = pf._review("tighten_stop")
            review["position_management"].update(update)
            with self.assertRaises(protection.PositionProtectionError):
                protection._build_prices(review=review, snapshot=pf._snapshot(), position=pf._position(), config=pf._config())

    def test_stale_quote_prevents_modification(self):
        with patch.object(protection, "get_current_tick", return_value={"time_fp": "2000-01-01T00:00:00+03:00"}):
            with self.assertRaises(protection.PositionProtectionError):
                protection._refresh_execution_snapshot(pf._snapshot())


class ExecutionRouteTests(unittest.TestCase):
    def test_approved_limit_and_stop_orders_reach_mt5_once(self):
        for order_type in ("limit", "stop"):
            with self.subTest(order_type=order_type), ExitStack() as stack:
                plan = {"plan_id": "pending-test", "order_type": order_type, "execution_status": pending.EXECUTION_NOT_SENT}
                request = {"action": pending.mt5.TRADE_ACTION_PENDING, "volume": .04}
                empty = {"ok": True, "valid": [], "invalid": []}
                for name, value in {
                    "get_active_plan": plan, "pending_plan_belongs_to_current_h1": {
                        "valid": True,
                        "same_h1": True,
                        "source_h1": "2026-09-16T10:00:00+03:00",
                        "current_h1": "2026-09-16T10:00:00+03:00",
                        "age_h1_bars": 0,
                        "max_h1_bars": pending.PENDING_MAX_H1_BARS,
                    },
                    "validate_active_plan": {"ready_for_send": True, "request": request},
                    "inspect_execution_safety_gate": {"mode": "LIVE", "order_send_allowed": True},
                    "find_active_pending_orders": empty, "find_owned_positions": empty,
                    "mark_active_plan_send_intent": {"saved": True}, "record_active_plan_order_send_result": None,
                    "build_order_send_result": {"retcode": 10009},
                    "reconcile_pending_plan": {"decision": "PENDING_ACTIVE", "pending_ticket": 78},
                }.items():
                    stack.enter_context(patch.object(pending, name, return_value=value))
                send = stack.enter_context(patch.object(pending.mt5, "order_send", return_value=SimpleNamespace(retcode=10009), create=True))
                result = pending.execute_pending_plan({})
                self.assertEqual(result["decision"], "PENDING_ORDER_PLACED")
                send.assert_called_once_with(request)

    def test_approved_market_order_reaches_mt5_once(self):
        plan = {"plan_id": "test-plan", "order_type": "market", "execution_status": executor.EXECUTION_NOT_SENT}
        request = {"action": executor.mt5.TRADE_ACTION_DEAL, "symbol": "XAUUSD", "volume": .06}
        with ExitStack() as stack:
            for name, value in {
                "get_active_plan": plan,
                "validate_active_plan": {"decision": "ORDER_CHECK_PASSED", "ready_for_send": True, "request": request},
                "inspect_execution_safety_gate": {"mode": "LIVE", "order_send_allowed": True},
                "reconcile_plan_execution": {"resolved": False, "blocked": False},
                "mark_active_plan_send_intent": {"saved": True},
                "record_active_plan_order_send_result": None,
                "reconcile_after_order_send": {"resolved": True, "position_ticket": 77},
                "build_order_send_result": {"retcode": 10009},
            }.items():
                stack.enter_context(patch.object(executor, name, return_value=value))
            send = stack.enter_context(patch.object(executor.mt5, "order_send", return_value=SimpleNamespace(retcode=10009), create=True))
            result = executor.execute_active_plan({})
        self.assertEqual(result["decision"], "ORDER_SEND_SUCCESS")
        send.assert_called_once_with(request)

    def test_existing_execution_does_not_send_duplicate(self):
        with patch.object(executor, "get_active_plan", return_value={"plan_id": "one", "order_type": "market"}), \
             patch.object(executor, "validate_active_plan", return_value={"decision": "ORDER_CHECK_PASSED", "ready_for_send": True}), \
             patch.object(executor, "inspect_execution_safety_gate", return_value={"mode": "LIVE", "order_send_allowed": True}), \
             patch.object(executor, "reconcile_plan_execution", return_value={"resolved": True, "position_ticket": 77}), \
             patch.object(executor.mt5, "order_send", create=True) as send:
            result = executor.execute_active_plan({})
        self.assertEqual(result["decision"], "RECOVERED_EXISTING_EXECUTION")
        send.assert_not_called()


class AuditTests(unittest.TestCase):
    def test_credentials_redacted_and_cyrillic_roundtrips(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {"ROBOT_DIAGNOSTICS_DIR": directory}):
            path = audit.emit("test", "event", data={"api_key": "private", "text": "Волна", "password": "private"})
            value = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(value["data"]["text"], "Волна")
            self.assertNotIn("private", path.read_text())

    def test_diagnostic_disk_failure_does_not_interrupt_business_function(self):
        @audit.observe("test")
        def business():
            return {"ok": True}
        with patch.object(audit, "_root", side_effect=OSError("disk full")):
            self.assertEqual(business(), {"ok": True})

    def test_cost_does_not_double_count_thinking_and_unknown_is_not_zero(self):
        self.assertAlmostEqual(estimate_cost("claude-sonnet-5", {"input_tokens": 20000,
            "output_tokens": 5000, "thinking_tokens": 4000})["usd"], .09)
        self.assertIsNone(estimate_cost("unknown", {"input_tokens": 1, "output_tokens": 1})["usd"])

    def test_diagnostic_zip_redacts_secrets_and_reports_trade_funnel(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "config").mkdir()
            (root / "logs").mkdir()
            archive_dir = root / "analysis_archive" / "2026-09-16"
            archive_dir.mkdir(parents=True)
            # Assemble a realistic-looking fake at runtime so repository
            # secret scanners do not mistake the fixture for a credential.
            secret = "sk" + "-ant-private-test-value"
            (root / "config" / "anthropic.json").write_text(
                json.dumps({"api_key": secret, "model": "claude-sonnet-5"}),
                encoding="utf-8",
            )
            (root / "logs" / "runner.log").write_text(
                "ключ=" + secret + "\n", encoding="utf-8"
            )
            (archive_dir / "cycle.json").write_text(json.dumps({
                "event_id": "one", "revision": 1,
                "snapshot_time_fp": "2026-09-16T10:00:00+03:00",
                "h1_closed_bar_time_fp": "2026-09-16T09:00:00+03:00",
                "cycle_type": "H1_DECISION_REFRESH",
                "result": {"recommendation": {
                    "action": "stay_out", "setup_type": "no_trade", "confidence": "medium"
                }},
                "risk_report": {"decision": "NO_TRADE", "reasons": ["Claude stay_out"]},
                "execution_report": {"decision": "NO_ACTIVE_PLAN", "order_send_called": False},
            }), encoding="utf-8")
            report = build_report(root, "2026-09-16", "2026-09-16")
            self.assertEqual(report["trade_funnel"]["claude_actions"], {"stay_out": 1})
            output = root / "bundle.zip"
            export_diagnostics(root, "2026-09-16", "2026-09-16", output)
            with zipfile.ZipFile(output) as bundle:
                combined = "\n".join(
                    bundle.read(name).decode("utf-8", errors="replace")
                    for name in bundle.namelist()
                )
            self.assertNotIn(secret, combined)
            self.assertIn("H1_DECISION_REFRESH", combined)


if __name__ == "__main__":
    unittest.main()
